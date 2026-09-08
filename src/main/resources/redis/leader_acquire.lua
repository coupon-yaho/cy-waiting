-- 리더 획득·연장. **획득과 확인이 갈리면 두 리더가 생긴다.**
--
-- KEYS[1]  scheduler:leader
-- KEYS[2]  {scheduler:leader}:gen   임기를 세는 값
-- ARGV[1]  ownerId. 이 노드를 가리키는 값
-- ARGV[2]  리스(밀리초). 양의 정수
--
-- 반환  {acquired, owner, ttlMillis, fence}
--   acquired   1 이면 내가 리더다 (새로 잡았거나 연장했다)
--   owner      지금 락을 쥔 노드
--   ttlMillis  남은 리스
--   fence      내 펜스 번호. 못 잡았으면 0
--
-- **소유자 ID 를 값에 담는다.** 안 담으면 남의 락을 지울 수 있고, 그러면
-- 리더가 둘이 되어 배분 총합이 전역 크레딧을 넘는다.
--
-- 재진입은 연장이다. 매 틱 새로 잡으려 하면 리더십이 흔들리고, 그때마다
-- 평활화 상태가 초기화된다.
--
-- **펜스 번호를 같이 담는다.** 되돌릴 수 없는 쓰기는 이 번호를 들고
-- 나가고, 줄 옆의 울타리가 그것으로 옛 리더를 가려낸다. 리더 키는 줄과 다른
-- 슬롯이라 그쪽에서 이 키를 못 읽기 때문이다.
--
-- **세는 값을 쓴다.** 시계로 매기면 시계가 뒤로 간 노드가 정당하게 승계해도 번호가
-- 작아져, 울타리가 새 리더를 막거나 옛 리더를 통과시킨다 — 어느 쪽으로 비교를 걸어도
-- 한쪽이 깨진다. 세는 값은 그 순서가 시계와 무관하다.
--
-- 키를 리더 키와 **같은 슬롯**에 묶는 것은 해시 태그가 한다. 태그 없는
-- `scheduler:leader` 는 키 전체로 슬롯을 정하므로 태그 안의 글자가 같으면 같은
-- 슬롯이다. 리더 키 이름을 안 바꾸므로 롤아웃 중에 리더가 둘이 되지 않는다.

local lease = tonumber(ARGV[2])
if lease == nil or lease < 1 or lease ~= math.floor(lease) then
    return redis.error_reply('리스는 양의 정수여야 한다: ' .. tostring(ARGV[2]))
end
if ARGV[1] == nil or ARGV[1] == '' then
    return redis.error_reply('ownerId 는 필수다')
end

-- 값의 형식은 `<펜스 번호>|<ownerId>` 다. **옛 형식(번호 없음)도 읽는다** —
-- 롤아웃 구간에 옛 노드가 남긴 값을 못 읽으면 그 락을 남의 것으로 보고
-- 리더가 둘이 된다.
local function ownerOf(value)
    local sep = string.find(value, '|', 1, true)
    if sep == nil then
        return value, 0
    end
    return string.sub(value, sep + 1), tonumber(string.sub(value, 1, sep - 1)) or 0
end

-- **씨앗은 시계다.** 1 부터 세면 남아 있는 옛 울타리 표(마이크로초 벽시계)를 못 넘어,
-- 새 리더가 자기 임기로 들어가려 할 때마다 그 표에 막힌다. 한 번 만들면 수명을 안 준다 —
-- 사라지면 씨앗부터 다시 세고, 그 사이 시계가 뒤로 가 있으면 임기가 되감긴다.
local function nextGeneration()
    if redis.call('EXISTS', KEYS[2]) == 0 then
        local t = redis.call('TIME')
        local seed = tonumber(t[1]) * 1000000 + tonumber(t[2])
        -- 자리 수를 박아 쓴다. 그냥 이어 붙이면 큰 수가 지수 표기로 나간다.
        redis.call('SET', KEYS[2], string.format('%.0f', seed))
    end
    return redis.call('INCR', KEYS[2])
end

local current = redis.call('GET', KEYS[1])

if not current then
    -- 아무도 안 잡았다. NX 로 잡아 **경합에서 하나만 이기게** 한다.
    -- **자리 수를 박아 쓴다.** 그냥 이어 붙이면 큰 수가 지수 표기로 나가
    -- ('1.7e+15') 정밀도를 잃고, 되읽은 펜스 번호가 쓴 것과 달라진다.
    local fence = nextGeneration()
    local mark = string.format('%.0f', fence)
    if redis.call('SET', KEYS[1], mark .. '|' .. ARGV[1], 'NX', 'PX', lease) then
        return {1, ARGV[1], lease, fence}
    end
    -- 그 사이 다른 노드가 잡았다. 다시 읽어 사실대로 알린다.
    current = redis.call('GET', KEYS[1])
    if not current then
        return {0, '', redis.call('PTTL', KEYS[1]), 0}
    end
    return {0, (ownerOf(current)), redis.call('PTTL', KEYS[1]), 0}
end

local owner, fence = ownerOf(current)
if owner == ARGV[1] then
    -- 내 락이다. 연장한다 — 새로 잡으려 하면 그 틈에 남이 가져간다. **펜스 번호는 그대로
    -- 둔다.** 매 틱 새로 매기면 자기 자신을 옛 리더로 만든다. 다만 번호가 0 이면 매긴다 —
    -- 0 은 울타리가 전부 거절하는 값이라 연장만으로는 스스로 못 빠져나온다.
    if fence <= 0 then
        fence = nextGeneration()
        redis.call('SET', KEYS[1], string.format('%.0f', fence) .. '|' .. ARGV[1],
                'PX', lease)
        return {1, ARGV[1], lease, fence}
    end
    redis.call('PEXPIRE', KEYS[1], lease)
    return {1, ARGV[1], lease, fence}
end

return {0, owner, redis.call('PTTL', KEYS[1]), 0}

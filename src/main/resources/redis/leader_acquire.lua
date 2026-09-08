-- 리더 획득·연장. **획득과 확인이 갈리면 두 리더가 생긴다.**
--
-- KEYS[1]  scheduler:leader
-- KEYS[2]  {scheduler:leader}:gen   임기를 세는 값. 태그가 슬롯을 묶는다
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
-- **키 개수를 본다.** 하나로 부르면 세는 값을 nil 로 만져 스크립트가 통째로 터지고,
-- 그 오류는 갱신 경로가 삼켜 전 노드가 리더 없이 돈다.
if KEYS[2] == nil then
    return redis.error_reply('KEYS 는 리더 키와 세는 값 둘이어야 한다')
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

-- **번호는 세는 값과 시계 중 큰 쪽에서 하나 오른다.**
--
-- 세는 값만 쓰면 되감김을 못 버틴다. 스크립트의 효과는 복제와 AOF 에 한 트랜잭션으로
-- 나가므로 복제본이 `INCR` 을 못 받았다면 락도 못 받았다 — 되감김과 재선거가 항상
-- 같이 온다. 그때 다음 리더가 **방금 나간 번호를 그대로 다시 발급**하고, 다른 슬롯에
-- 남아 있는 울타리 표는 그 번호를 같은 임기로 보고 통과시킨다.
--
-- 시계만 쓰면 뒤로 간 시계를 못 버틴다. 승계한 노드의 번호가 옛 리더보다 작아진다.
--
-- 그래서 **둘 중 큰 쪽**을 바닥으로 삼는다. 시계가 뒤로 가면 세는 값이 이기고, 세는
-- 값이 되감기면 시계가 이긴다. 한쪽만으로는 어느 경우에도 못 버틴다.
--
-- 시계 바닥에 **배포 창을 더한다.** 안 더하면 배포 중에 아직 안 바뀐 노드가 리더를
-- 한 번 쥘 때 그쪽 번호가 더 커서, 새 노드가 표 수명 내내 거절된다.
--
-- 수명을 안 준다. 사라지면 시계 바닥부터 다시 세는데, 그 바닥이 남아 있는 옛 표를
-- 넘으므로 잃는 것이 없다.
local ROLLOUT_MARGIN = 86400000000  -- 24시간(마이크로초). 배포가 이보다 길면 못 막는다

local function nextGeneration()
    local t = redis.call('TIME')
    local floor = tonumber(t[1]) * 1000000 + tonumber(t[2]) + ROLLOUT_MARGIN
    -- 자리 수를 박아 쓴다. 그냥 이어 붙이면 큰 수가 지수 표기로 나간다.
    local mark = string.format('%.0f', floor)
    -- **성하지 않은 값도 여기서 걷어낸다.** 타입이 어긋나거나 정수가 아니면 INCR 이
    -- 스크립트째 터지고 그러면 리더가 영영 안 뽑힌다. SET 은 타입을 덮는다.
    local stored = redis.pcall('GET', KEYS[2])
    local seen = type(stored) == 'string' and tonumber(stored) or nil
    if seen == nil or seen ~= seen or seen ~= math.floor(seen) or seen < floor then
        redis.call('SET', KEYS[2], mark)
    end
    -- **레디스가 거절하는 모양을 `tonumber` 는 받는다.** 지수 표기와 int64 밖의 값이
    -- 그렇다. 여기서 안 걷으면 그 키 하나가 리더를 영영 막는다.
    local bumped = redis.pcall('INCR', KEYS[2])
    if type(bumped) == 'table' then
        -- **낮춰 덮기 전에 그 값이 나갈 수 있었는지부터 본다.** 0 을 더해 보면
        -- 레디스가 읽는 정수인지가 갈린다 — 읽히는데 못 오르는 것은 상한뿐이고,
        -- 그 번호는 직전 임기로 이미 나갔다. 낮춰 덮으면 그 번호를 든 유령이
        -- 울타리를 통과한다. 리더를 안 뽑는 쪽이 안전한 방향이라 nil 로 알린다.
        if type(redis.pcall('INCRBY', KEYS[2], 0)) ~= 'table' then
            return nil
        end
        redis.call('SET', KEYS[2], mark)
        bumped = redis.call('INCR', KEYS[2])
    end
    return bumped
end

local CEILING = '세는 값이 상한이라 임기를 못 매긴다'

local current = redis.call('GET', KEYS[1])

if not current then
    -- 아무도 안 잡았다. NX 로 잡아 **경합에서 하나만 이기게** 한다. 진 쪽도 번호를
    -- 태우므로 **연속은 보장하지 않는다** — 지키는 것은 순서뿐이다.
    local fence = nextGeneration()
    if fence == nil then
        return redis.error_reply(CEILING)
    end
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
        if fence == nil then
            return redis.error_reply(CEILING)
        end
        redis.call('SET', KEYS[1], string.format('%.0f', fence) .. '|' .. ARGV[1],
                'PX', lease)
        return {1, ARGV[1], lease, fence}
    end
    redis.call('PEXPIRE', KEYS[1], lease)
    return {1, ARGV[1], lease, fence}
end

return {0, owner, redis.call('PTTL', KEYS[1]), 0}

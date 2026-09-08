-- 배분 적용. **임계를 산출하고 나서 쓴다.**
--
-- KEYS[1]  queue:{cid}          ZSET
-- KEYS[2]  admitted:{cid}       입장 임계. 개수가 아니라 score 값이다
-- KEYS[3]  applyfence:{cid}     마지막으로 들인 리더의 임기
-- ARGV[1]  이번 회차에 들일 인원. 0 이상의 정수
-- ARGV[2]  이 회차의 임기(펜스 번호). 0 이면 리더가 아니다
-- ARGV[3]  울타리 표의 수명(ms)
--
-- 반환  {임계, 들인 인원}. 울타리가 막았으면 {-1, -1, 막은 임기}
--   임계      새 입장 임계. 안 바뀌었으면 이전 값
--   들인 인원  임계 위로 새로 들어온 사람 수
--
--   **거절을 칸 수로 가른다.** `{-1, 0}` 은 임계가 없고 들일 사람도 없는 정상
--   회차와 같은 값이라, 그것으로 가르면 새 쿠폰과 빈 큐가 거절로 오독된다.
--
-- **수를 문자열로 만들 때 tostring 을 쓰지 않는다.** Lua 5.1 은 %.14g 로
-- 찍는데 마이크로초 score 는 16자리라 과학 표기로 접히며 최대 100μs 가
-- 반올림된다. 올림 쪽으로 접히면 그 사이 도착자가 줄을 안 서고 통과하고,
-- 내림 쪽으로 접히면 이미 통과한 사람이 다시 대기가 된다. %.0f 를 쓴다.
--
-- **임계는 뒤로 안 간다.** 되돌리면 이미 통과한 사람이 다시 대기가 되고,
-- 그건 순번 역행이다. 두 번 적용돼도 값이 같거나 커지므로 리더가 겹쳐도
-- 순번은 안전하다 — 개수 기반이면 두 번 적용이 두 배 입장이 됐다.
--
-- **그것으로 초과 발급은 안 막힌다.** 단조는 순번 역행에 대한 보장이고, 두
-- 리더가 각각 제 몫을 밀면 임계가 예산을 넘어 오른다. 그래서 울타리를 둔다 —
-- 옛 임기의 적용은 임계를 안 올린다.

-- 배정밀도가 정확한 정수 범위. 넘으면 세는 것 자체가 의미를 잃는다.
local MAX_ADMIT = 9007199254740992

local fence = tonumber(ARGV[2])
if fence == nil or fence ~= fence or fence ~= math.floor(fence) then
    return redis.error_reply('펜스 번호는 정수여야 한다: ' .. tostring(ARGV[2]))
end
local fenceTtl = tonumber(ARGV[3])
if fenceTtl == nil or fenceTtl ~= fenceTtl or fenceTtl < 1
        or fenceTtl ~= math.floor(fenceTtl) then
    return redis.error_reply('울타리 수명은 1 이상의 정수여야 한다: ' .. tostring(ARGV[3]))
end

-- **0 은 리더가 아니라는 뜻이다.** 강등된 노드가 그 값을 들고 나오므로, 안 막으면
-- 리더가 아닌 노드가 사람을 들인다.
if fence <= 0 then
    return {'-1', -1, 0}
end

-- **옛 임기는 안 들인다.** 승계 뒤 깨어난 유령의 회차는 이미 제 몫을 계산한
-- 뒤이고, 그것을 그대로 밀면 같은 초에 두 리더의 몫이 다 나간다.
-- 같은 번호의 재시도는 막지 않는다 — 막으면 실패한 회차가 영영 안 된다.
local seenFence = tonumber(redis.call('GET', KEYS[3]))
if seenFence ~= nil and seenFence == seenFence and fence < seenFence then
    return {'-1', -1}
end
-- **수명을 준다.** 이 표는 쿠폰별이라 그 쿠폰이 한산하면 갱신이 안 온다 — 스냅샷
-- 울타리처럼 짧게 두면 그 사이 문이 통째로 사라진다. 그래서 쿠폰별 표와 같은
-- 수명을 쓴다. 길어서 생기는 영구 차단은 승계 때 문을 다시 잠그는 것이 푼다.
redis.call('SET', KEYS[3], string.format('%.0f', fence), 'PX', fenceTtl)

local admit = tonumber(ARGV[1])
-- 무한대는 math.floor 를 통과한다. 상한을 안 두면 그 뒤 LIMIT 에서 엉뚱한
-- 메시지로 터져 원인을 못 찾는다.
if admit == nil or admit ~= admit or admit < 0 or admit ~= math.floor(admit)
        or admit > MAX_ADMIT then
    return redis.error_reply('들일 인원은 0 이상 ' .. MAX_ADMIT
            .. ' 이하의 정수여야 한다: ' .. tostring(ARGV[1]))
end

-- **없는 것과 깨진 것을 가른다.** 둘 다 -1 로 접으면 큐 맨 앞부터 다시 세어
-- 임계가 뒤로 가고, 그 회차에서 이미 통과한 사람이 대기로 되돌아간다.
-- 없는 것은 새 쿠폰이라 -1 이 맞지만, 깨진 것은 소리를 내야 한다.
local raw = redis.call('GET', KEYS[2])
local current = -1
if raw then
    current = tonumber(raw)
    -- 무한대는 비교를 통과하면서 임계를 영원히 못 올리게 만든다. 조용히 멎는다.
    if current == nil or current ~= current or current == math.huge
            or current == -math.huge then
        return redis.error_reply('임계가 수가 아니다 — 낮추지 않는다: ' .. tostring(raw))
    end
end

if admit == 0 then
    -- 크레딧이 없다. **임계를 낮추지 않는다** — 낮추면 통과한 사람이 되돌아온다.
    return {string.format('%.0f', current), 0}
end

-- **이미 임계 아래인 사람은 세지 않는다.** 앞에서부터 세면 통과한 사람 자리에
-- 크레딧을 낭비하고, 그만큼 실제로 들어오는 사람이 준다.
local from = current >= 0 and '(' .. string.format('%.0f', current) or '-inf'
local picked = redis.call('ZRANGEBYSCORE', KEYS[1], from, '+inf', 'WITHSCORES',
        'LIMIT', admit - 1, 1)

local threshold
if #picked > 0 then
    threshold = tonumber(picked[2])
else
    -- 크레딧이 남은 큐보다 크다. **`+inf` 를 쓰지 않는다** — 그러면 이후
    -- 도착자까지 임계 아래로 들어와 줄을 서지 않고 통과한다.
    local last = redis.call('ZRANGE', KEYS[1], -1, -1, 'WITHSCORES')
    if #last == 0 then
        -- 큐가 비었다. 들일 사람이 없으니 임계도 그대로다.
        return {string.format('%.0f', current), 0}
    end
    threshold = tonumber(last[2])
end

if threshold <= current then
    return {string.format('%.0f', current), 0}
end

local exact = string.format('%.0f', threshold)
local entering = redis.call('ZCOUNT', KEYS[1], from, exact)
redis.call('SET', KEYS[2], exact)
return {exact, entering}

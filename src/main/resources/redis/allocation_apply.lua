-- 배분 적용. **임계를 산출하고 나서 쓴다.**
--
-- KEYS[1]  queue:{cid}          ZSET
-- KEYS[2]  admitted:{cid}       입장 임계. 개수가 아니라 score 값이다 (D-8)
-- ARGV[1]  이번 회차에 들일 인원. 0 이상의 정수
--
-- 반환  {임계, 들인 인원}
--   임계      새 입장 임계. 안 바뀌었으면 이전 값
--   들인 인원  임계 위로 새로 들어온 사람 수
--
-- **수를 문자열로 만들 때 tostring 을 쓰지 않는다.** Lua 5.1 은 %.14g 로
-- 찍는데 마이크로초 score 는 16자리라 과학 표기로 접히며 최대 100μs 가
-- 반올림된다. 올림 쪽으로 접히면 그 사이 도착자가 줄을 안 서고 통과하고,
-- 내림 쪽으로 접히면 이미 통과한 사람이 다시 대기가 된다. %.0f 를 쓴다.
--
-- **임계는 뒤로 안 간다.** 되돌리면 이미 통과한 사람이 다시 대기가 되고,
-- 그건 순번 역행이다. 두 번 적용돼도 값이 같거나 커지므로 리더가 겹쳐도
-- 안전하다 — 개수 기반이면 두 번 적용이 두 배 입장이 됐다 (A-7).

-- 배정밀도가 정확한 정수 범위. 넘으면 세는 것 자체가 의미를 잃는다.
local MAX_ADMIT = 9007199254740992

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
    -- 도착자까지 임계 아래로 들어와 줄을 서지 않고 통과한다 (D-10).
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

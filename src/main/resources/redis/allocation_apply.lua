-- 배분 적용. **임계를 산출하고 나서 쓴다.**
--
-- KEYS[1]  queue:{cid}          ZSET
-- KEYS[2]  admitted:{cid}       입장 임계. 개수가 아니라 score 값이다 (D-8)
-- ARGV[1]  이번 판에 들일 인원. 0 이상의 정수
--
-- 반환  {임계, 들인 인원}
--   임계      새 입장 임계. 안 바뀌었으면 이전 값
--   들인 인원  임계 위로 새로 들어온 사람 수
--
-- **임계는 뒤로 안 간다.** 되돌리면 이미 통과한 사람이 다시 대기가 되고,
-- 그건 순번 역행이다. 두 번 적용돼도 값이 같거나 커지므로 리더가 겹쳐도
-- 안전하다 — 개수 기반이면 두 번 적용이 두 배 입장이 됐다 (A-7).

local admit = tonumber(ARGV[1])
if admit == nil or admit ~= admit or admit < 0 or admit ~= math.floor(admit) then
    return redis.error_reply('들일 인원은 0 이상의 정수여야 한다: ' .. tostring(ARGV[1]))
end

local current = tonumber(redis.call('GET', KEYS[2]) or '-1')

if admit == 0 then
    -- 크레딧이 없다. **임계를 낮추지 않는다** — 낮추면 통과한 사람이 되돌아온다.
    return {tostring(current), 0}
end

-- **이미 임계 아래인 사람은 세지 않는다.** 앞에서부터 세면 통과한 사람 자리에
-- 크레딧을 낭비하고, 그만큼 실제로 들어오는 사람이 준다.
local from = current >= 0 and '(' .. current or '-inf'
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
        return {tostring(current), 0}
    end
    threshold = tonumber(last[2])
end

if threshold <= current then
    return {tostring(current), 0}
end

local entering = redis.call('ZCOUNT', KEYS[1], from, threshold)
redis.call('SET', KEYS[2], tostring(threshold))
return {tostring(threshold), entering}

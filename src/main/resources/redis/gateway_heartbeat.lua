-- 게이트웨이 하트비트. **시각은 레디스가 찍는다.**
--
-- KEYS[1]  gw:instances        HASH. field = instanceId, value = 서버 시각(초)
-- ARGV[1]  instanceId
-- ARGV[2]  죽은 항목 임계(초). 이보다 오래된 field 는 지운다
--
-- 반환  {살아있는 수, 서버 시각(초)}
--
-- **노드가 제 벽시계를 찍으면 안 된다.** 시계가 앞선 노드는 영영 신선하고
-- 뒤진 노드는 즉시 만료된다. 한 시계로 재야 비교가 성립한다.
--
-- **쓰기와 정리를 한 스크립트에 둔다.** 나누면 그 사이에 다른 노드가 세어
-- 방금 지운 항목을 살아 있는 것으로 본다.

-- **상한과 무한을 함께 막는다.** tonumber 는 1e400 을 inf 로 주는데
-- math.floor(inf) == inf 라 정수 검사를 그냥 통과한다. 그러면 아무것도 영영
-- 안 지워져 해시가 배포 이력만큼 자라고, 매 틱 그걸 다 읽는다.
local MAX_REAP = 86400
local reapAfter = tonumber(ARGV[2])
if reapAfter == nil or reapAfter ~= reapAfter or reapAfter < 1
        or reapAfter > MAX_REAP or reapAfter ~= math.floor(reapAfter) then
    return redis.error_reply('임계는 1..' .. MAX_REAP .. ' 의 정수여야 한다: ' .. tostring(ARGV[2]))
end
if ARGV[1] == nil or ARGV[1] == '' then
    return redis.error_reply('instanceId 는 필수다')
end

local now = tonumber(redis.call('TIME')[1])

-- **내 하트비트를 먼저 쓴다.** 정리를 먼저 하면 그 사이 터졌을 때 나까지
-- 빠진 채로 남고, 그러면 남은 노드가 큰 몫을 쓴다.
redis.call('HSET', KEYS[1], ARGV[1], now)

-- 정리는 읽으면서 한다. 배포 이력만큼 해시가 자라면 매 틱 그걸 다 읽는다.
local alive = 0
local dead = {}
local entries = redis.call('HGETALL', KEYS[1])
for i = 1, #entries, 2 do
    local seen = tonumber(entries[i + 1])
    -- **미래 시각은 죽은 것으로 본다.** 복제본이 승격하면서 시계가 뒤로 가면
    -- 기존 항목이 전부 미래가 되고, now - seen 이 음수라 영영 안 지워진다.
    -- 죽은 노드가 계속 세어져 분모가 부풀고 스스로 회복되지 않는다.
    if seen == nil or seen > now or now - seen > reapAfter then
        dead[#dead + 1] = entries[i]
    else
        alive = alive + 1
    end
end

-- **unpack 한계를 넘기지 않는다.** 한 번에 다 못 지우면 다음 틱이 마저 지운다 —
-- 남은 것은 어차피 죽은 항목이라 세는 값에 영향이 없다.
if #dead > 0 then
    local limit = 4000
    for i = 1, #dead, limit do
        local last = math.min(i + limit - 1, #dead)
        redis.call('HDEL', KEYS[1], unpack(dead, i, last))
    end
end

return {alive, now}

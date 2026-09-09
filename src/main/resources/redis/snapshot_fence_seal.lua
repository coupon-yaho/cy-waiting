-- 승계 직후 발행의 문을 잠근다.
--
-- KEYS[1]  {gw:snapshot}:fence
-- ARGV[1]  펜스 번호. 리더가 리스를 새로 잡을 때 받은 값
-- ARGV[2]  표의 수명(밀리초)
--
-- 반환  1 이면 잠갔다, 0 이면 안 잠갔다
--
-- **쿠폰 쪽 잠금과 같은 슬롯이 아니다.** 이 키는 스냅샷 태그라 한 스크립트로 못 묶는다 —
-- 부르는 쪽이 둘을 나란히 돌려 승계가 멎는 시간을 안 늘린다.
--
-- **올리기만 한다.** 덮어쓰면 승계 중에 멎었던 옛 리더의 잠금이 늦게 착륙하면서 표를
-- 낮추고, 그 뒤 그 리더의 발행이 통과한다. 전 노드가 유령의 재료를 읽는 상태라
-- 이 스크립트가 막으려던 것보다 나쁘다. 수명이 짧아 낮춰서 풀 이유도 없다.

local fence = tonumber(ARGV[1])
if fence == nil or fence ~= fence or fence ~= math.floor(fence) then
    return redis.error_reply('펜스 번호는 정수여야 한다: ' .. tostring(ARGV[1]))
end
local ttl = tonumber(ARGV[2])
if ttl == nil or ttl ~= ttl or ttl < 1 or ttl ~= math.floor(ttl) then
    return redis.error_reply('울타리 수명은 1 이상의 정수여야 한다: ' .. tostring(ARGV[2]))
end
-- **0 은 리더가 아니라는 뜻이다.** 강등된 노드가 그 값을 들고 나오므로, 안 막으면
-- 표가 아직 없는 자리에 0 이 서서 그 뒤의 모든 발행이 통과한다.
if fence <= 0 then
    return 0
end

-- **무한대는 안 비켜 준다.** 덮어쓰는 잠금과 달리 이쪽은 큰 값을 못 고치므로,
-- 밖에서 들어온 `1e400` 하나가 잠금도 발행도 영영 막는다.
local seen = tonumber(redis.call('GET', KEYS[1]))
if seen ~= nil and seen == seen and seen ~= math.huge and fence < seen then
    return 0
end
-- 자리 수를 박아 쓴다. 그냥 이어 붙이면 큰 수가 지수 표기로 나간다.
redis.call('SET', KEYS[1], string.format('%.0f', fence), 'PX', ttl)
return 1

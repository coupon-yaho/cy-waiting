-- 입장 적용의 문을 <b>새 임기로 잠근다.</b>
--
-- KEYS[1]  applyfence:{cid}   그 쿠폰에 마지막으로 사람을 들인 리더의 임기
-- ARGV[1]  잠글 임기. 0 이면 리더가 아니다
-- ARGV[2]  울타리 표의 수명(ms)
--
-- 반환  1 이면 잠갔다, 0 이면 안 했다
--
-- **적용만으로는 문이 안 잠긴다.** 그쪽은 그 쿠폰에 크레딧이 갈 때만 도므로,
-- 새 리더가 그 쿠폰을 한 번 만지기 전까지 표에는 옛 임기가 남는다. 그 창에
-- 유령이 먼저 도착하면 자기 번호와 같아서 통과한다 — 같은 초에 두 리더의 몫이
-- 다 나가고 그것이 초과 발급이다.
--
-- **덮어쓴다.** 큰 값만 쓰면 시계가 뒤로 간 리더가 승계해도 옛 표를 못 넘어
-- 그 쿠폰의 줄이 수명 내내 안 빠진다. 락을 쥔 것이 권위이지 번호의 크기가
-- 권위가 아니다 — 락은 이 스크립트를 부르기 전에 이미 이겼다.

local fence = tonumber(ARGV[1])
if fence == nil or fence ~= fence or fence ~= math.floor(fence) then
    return redis.error_reply('펜스 번호는 정수여야 한다: ' .. tostring(ARGV[1]))
end
if fence <= 0 then
    return 0
end

local ttl = tonumber(ARGV[2])
if ttl == nil or ttl ~= ttl or ttl < 1 or ttl ~= math.floor(ttl) then
    return redis.error_reply('울타리 수명은 1 이상의 정수여야 한다: ' .. tostring(ARGV[2]))
end

redis.call('SET', KEYS[1], string.format('%.0f', fence), 'PX', ttl)
return 1

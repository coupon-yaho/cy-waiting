-- 그 쿠폰의 문들을 **새 임기로 잠근다.**
--
-- KEYS[1]  applyfence:{cid}   마지막으로 사람을 들인 리더의 임기. 덮어쓴다
-- KEYS[2]  dropfence:{cid}    마지막으로 줄을 지운 리더의 임기. **올리기만 한다**
-- ARGV[1]  잠글 임기. 0 이면 리더가 아니다
-- ARGV[2]  울타리 표의 수명(ms)
--
-- 반환  1 이면 잠갔다, 0 이면 안 했다
--
-- **한 번에 잠근다.** 표마다 왕복하면 승계 뒤 배분이 안 도는 시간이 그만큼
-- 곱해진다 — 쿠폰이 만 개면 초 단위다. 두 키는 태그가 같아 슬롯이 안 갈린다.
--
-- **적용만으로는 문이 안 잠긴다.** 그쪽은 그 쿠폰에 크레딧이 갈 때만 도므로,
-- 새 리더가 그 쿠폰을 한 번 만지기 전까지 표에는 옛 임기가 남는다. 그 창에
-- 유령이 먼저 도착하면 자기 번호와 같아서 통과한다 — 같은 초에 두 리더의 몫이
-- 다 나가고 그것이 초과 발급이다.
--
-- **입장은 덮어쓴다.** 큰 값만 쓰면 시계가 뒤로 간 리더가 승계해도 옛 표를 못 넘어
-- 그 쿠폰의 줄이 수명 내내 안 빠진다. 락을 쥔 것이 권위이지 번호의 크기가
-- 권위가 아니다 — 락은 이 스크립트를 부르기 전에 이미 이겼다.
--
-- **삭제는 올리기만 한다.** 그쪽 쓰기는 되돌릴 수 없다. 덮어쓰면 승계 중에 멈췄던
-- 옛 리더가 깨어나 새 리더의 표를 낮추고, 그 뒤 자기 유령 삭제가 자기 번호와 같아서
-- 통과한다 — 이 울타리가 막으려던 바로 그 삭제다. 못 올려서 막히는 쪽은 죽은 줄이
-- 표의 수명 동안 남는 것이고, 지운 줄은 되살릴 방법이 없다.

local fence = tonumber(ARGV[1])
if fence == nil or fence ~= fence or fence ~= math.floor(fence) then
    return redis.error_reply('펜스 번호는 정수여야 한다: ' .. tostring(ARGV[1]))
end
local ttl = tonumber(ARGV[2])
if ttl == nil or ttl ~= ttl or ttl < 1 or ttl ~= math.floor(ttl) then
    return redis.error_reply('울타리 수명은 1 이상의 정수여야 한다: ' .. tostring(ARGV[2]))
end

-- **인자를 다 본 뒤에 분기한다.** 먼저 되돌아가면 수명이 쓰레기여도 조용히 지나가고,
-- 그 오타는 리더가 된 노드에서만 드러난다.
if fence <= 0 then
    return 0
end

local mark = string.format('%.0f', fence)
redis.call('SET', KEYS[1], mark, 'PX', ttl)

-- 더 큰 표가 이미 있으면 그쪽이 새 리더다. 낮추지 않는다.
local seen = tonumber(redis.call('GET', KEYS[2]))
if seen == nil or seen ~= seen or fence >= seen then
    redis.call('SET', KEYS[2], mark, 'PX', ttl)
end
return 1

-- 스냅샷 발행. **통째로 갈아 끼우되 키가 비는 순간이 없어야 한다.**
--
-- KEYS[1]  gw:snapshot
-- ARGV[1]  첫 필드 이름
-- ARGV[2]  첫 필드 값
-- ARGV[n]  이후 이름과 값이 번갈아 온다. 개수는 짝수여야 한다
--
-- 반환  {실린 필드 수, 지운 필드 수}
--
-- 지우고 쓰는 것을 나눠 치면 그 사이에 끊길 때 **키가 없는 채로 남는다.**
-- 그러면 전 노드가 판정 재료를 잃고, 낡음으로 넘어가 그때부터 줄 없는 쿠폰이
-- 통째로 통과한다. 리더가 스스로 공유 상태를 부수는 셈이다.
--
-- 남기지 않는 것도 함께 지킨다 — 끝난 쿠폰이 남으면 각 노드가 없는 쿠폰을
-- 영영 판정한다. 그래서 **먼저 덮어쓰고 남은 것을 지운다.**
--
-- 이 순서는 메모리가 찼을 때도 옳다. 쓰기는 거부되지만 삭제는 통과하므로,
-- 반대로 하면 지우기만 하고 못 쓰는 상태가 된다.

if #ARGV == 0 or #ARGV % 2 ~= 0 then
    return redis.error_reply('필드와 값은 짝을 이뤄야 한다: ' .. #ARGV)
end

local keep = {}
for i = 1, #ARGV, 2 do
    keep[ARGV[i]] = true
end

redis.call('HSET', KEYS[1], unpack(ARGV))

local stale = {}
for _, field in ipairs(redis.call('HKEYS', KEYS[1])) do
    if not keep[field] then
        stale[#stale + 1] = field
    end
end
if #stale > 0 then
    redis.call('HDEL', KEYS[1], unpack(stale))
end

return {#ARGV / 2, #stale}

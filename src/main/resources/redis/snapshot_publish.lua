-- 스냅샷 발행. **통째로 갈아 끼우되 키가 비는 순간이 없어야 한다.**
--
-- KEYS[1]  gw:snapshot
-- KEYS[2]  {gw:snapshot}:fence  이 해시를 마지막으로 쓴 임기. 태그가 슬롯을 묶는다
-- ARGV[1]  이 발행의 임기(펜스 번호). 0 이면 리더가 아니다
-- ARGV[2]  울타리 표의 수명(ms)
-- ARGV[3]  첫 필드 이름
-- ARGV[4]  첫 필드 값
-- ARGV[n]  이후 이름과 값이 번갈아 온다. 개수는 짝수여야 한다
--
-- 반환  {실린 필드 수, 지운 필드 수}. 임기가 낡아 안 실었으면 {-1, -1, 막은 임기}
--        **센티널을 쓴다** — 0 으로 두면 읽는 쪽이 값 충돌에 기대고, 빈 발행을
--        나중에 허용하는 순간 거절이 조용히 성공으로 읽힌다
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

local fence = tonumber(ARGV[1])
if fence == nil or fence ~= fence or fence ~= math.floor(fence) then
    return redis.error_reply('펜스 번호는 정수여야 한다: ' .. tostring(ARGV[1]))
end
local fenceTtl = tonumber(ARGV[2])
if fenceTtl == nil or fenceTtl ~= fenceTtl or fenceTtl < 1
        or fenceTtl ~= math.floor(fenceTtl) then
    return redis.error_reply('울타리 수명은 1 이상의 정수여야 한다: ' .. tostring(ARGV[2]))
end
table.remove(ARGV, 1)
table.remove(ARGV, 1)

if #ARGV == 0 or #ARGV % 2 ~= 0 then
    return redis.error_reply('필드와 값은 짝을 이뤄야 한다: ' .. #ARGV)
end

-- **0 은 리더가 아니라는 뜻이다.** 강등된 노드가 그 값을 들고 나오므로, 안 막으면
-- 리더가 아닌 노드가 전 클러스터의 판정 재료를 갈아 끼운다.
if fence <= 0 then
    return {-1, -1, 0}
end

-- **옛 임기의 발행은 안 듣는다.** 검사와 쓰기 사이에 리스가 끝나면 유령 리더가
-- 옛 시야로 새 리더의 스냅샷을 덮고, 그러면 전 노드의 대기 수가 뒤로 간다 —
-- 0 이 되면 줄이 없는 것으로 읽혀 사다리가 통과로 갈리고 그것이 추월이다.
-- 같은 번호의 재시도는 막지 않는다 — 막으면 실패한 발행이 영영 안 된다.
local seen = tonumber(redis.call('GET', KEYS[2]))
if seen ~= nil and seen == seen and fence < seen then
    -- 막은 임기를 같이 준다. 안 주면 운영자가 PTTL 을 손으로 봐야 원인을 안다.
    return {-1, -1, seen}
end
-- **수명을 준다.** 안 주면 시계가 뒤로 간 리더가 스스로 못 풀린다 — 새로 뽑힌
-- 정당한 리더의 발행이 영영 거절되고, 그러면 전 노드가 재료를 잃어 줄 없는 쿠폰이
-- 통째로 통과한다. 이 스크립트가 막으려던 것보다 나쁜 상태다.
redis.call('SET', KEYS[2], string.format('%.0f', fence), 'PX', fenceTtl)

local keep = {}
for i = 1, #ARGV, 2 do
    keep[ARGV[i]] = true
end

redis.call('HSET', KEYS[1], unpack(ARGV))

-- **한 번에 다 넘기지 않는다.** unpack 의 개수 상한은 발행자가 아니라 이미 들어 있는
-- 것이 정한다. 넘으면 HSET 만 성공한 채로 터져 발행 시각은 신선한데 끝난 쿠폰이 영영
-- 안 지워진다 — 실패인데 성공처럼 보인다. 지우기는 나눠 해도 결과가 같다.
local CHUNK = 512

local stale = {}
local chunk = {}
for _, field in ipairs(redis.call('HKEYS', KEYS[1])) do
    if not keep[field] then
        stale[#stale + 1] = field
        chunk[#chunk + 1] = field
        if #chunk == CHUNK then
            redis.call('HDEL', KEYS[1], unpack(chunk))
            chunk = {}
        end
    end
end
if #chunk > 0 then
    redis.call('HDEL', KEYS[1], unpack(chunk))
end

return {#ARGV / 2, #stale}

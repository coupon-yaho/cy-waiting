-- 이탈자 청소. **앞부분만 훑는다.**
--
-- KEYS[1]  queue:{cid}   ZSET
-- KEYS[2]  grace:{cid}   이탈 기록 해시
-- ARGV[1]  검사 범위 K. 앞에서 이만큼만 본다
-- ARGV[2]  지금 시각(초). 도메인처럼 주입받는다
-- ARGV[3]  유예 보관 기간(초)
-- ARGV[4]  alive 키 접두사. 'alive:{cid}:' 형태
--
-- 반환  {swept, expired}
--   swept    큐에서 뺀 인원
--   expired  만료돼 정리된 유예 기록 수
--
-- **전체를 훑지 않는다.** 2만 명 큐에서 그건 청소 자체가 부하다. 앞부분만
-- 보는 것으로 충분한 이유는 이탈이 앞에서부터 드러나기 때문이다 — 뒤엣사람은
-- 아직 폴링할 차례가 안 왔을 뿐 죽은 것이 아니다.
--
-- **제거와 기록이 갈리면** 자리도 잃고 재방문자로도 식별 안 되는 사람이 생긴다.

local limit = tonumber(ARGV[1])
if limit == nil or limit < 1 or limit ~= math.floor(limit) then
    return redis.error_reply('검사 범위는 양의 정수여야 한다: ' .. tostring(ARGV[1]))
end

local now = tonumber(ARGV[2])
if now == nil or now < 0 then
    return redis.error_reply('시각은 0 이상이어야 한다: ' .. tostring(ARGV[2]))
end

local retention = tonumber(ARGV[3])
if retention == nil or retention < 1 or retention ~= math.floor(retention) then
    return redis.error_reply('유예 보관 기간은 양의 정수여야 한다: ' .. tostring(ARGV[3]))
end

local prefix = ARGV[4]
if prefix == nil or prefix == '' then
    return redis.error_reply('alive 접두사는 필수다')
end

-- 앞에서 K 명. 뒤는 아직 볼 때가 아니다.
local front = redis.call('ZRANGE', KEYS[1], 0, limit - 1)
local swept = 0

for i = 1, #front do
    local member = front[i]
    if redis.call('EXISTS', prefix .. member) == 0 then
        redis.call('ZREM', KEYS[1], member)
        -- 자리는 안 보관한다. 재방문자로 식별만 한다 (D-11).
        redis.call('HSET', KEYS[2], member, now)
        swept = swept + 1
    end
end

-- **유예 기록이 무한히 자라지 않게 한다** (RD-7). 해시는 필드별 TTL 을
-- 못 걸어서 값에 시각을 담고 여기서 지운다.
local expired = 0
local cutoff = now - retention
local records = redis.call('HGETALL', KEYS[2])
for i = 1, #records, 2 do
    local at = tonumber(records[i + 1])
    if at == nil or at < cutoff then
        redis.call('HDEL', KEYS[2], records[i])
        expired = expired + 1
    end
end

return {swept, expired}

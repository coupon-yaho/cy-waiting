-- 이탈자 청소. **앞부분만 훑고, 정리량에도 상한을 둔다.**
--
-- KEYS[1]  queue:{cid}   ZSET
-- KEYS[2]  grace:{cid}   이탈 기록 해시
-- KEYS[3]  alive:{cid}   생존 신호 ZSET. score 는 만료 시각(초)
-- ARGV[1]  검사 범위 K. 큐 앞에서 이만큼만 본다
-- ARGV[2]  지금 시각(초). 도메인처럼 주입받는다
-- ARGV[3]  유예 보관 기간(초)
-- ARGV[4]  유예 정리 예산. 한 번에 이만큼만 본다
-- ARGV[5]  HSCAN 커서. 첫 호출은 '0'. 반환된 값을 다음에 넘긴다
--
-- 반환  {swept, expired, nextCursor}
--
-- **키를 문자열로 조립하지 않는다.** 사람마다 alive 키를 만들면 KEYS 에
-- 선언되지 않은 키를 만지게 되고 클러스터가 거부한다 (RD-1). 쿠폰당 ZSET
-- 하나에 만료 시각을 score 로 담아 선언된 키만 만진다.
--
-- **전체를 훑지 않는다.** 2만 명 큐에서 그건 청소 자체가 부하다. 뒤엣사람은
-- 아직 폴링할 차례가 안 왔을 뿐 죽은 것이 아니다.

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

local budget = tonumber(ARGV[4])
if budget == nil or budget < 1 or budget ~= math.floor(budget) then
    return redis.error_reply('정리 예산은 양의 정수여야 한다: ' .. tostring(ARGV[4]))
end

-- 만료된 생존 신호를 걷는다. score 가 지금보다 작으면 폴링이 끊긴 것이다.
redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', '(' .. now)

-- 앞에서 K 명. 뒤는 아직 볼 때가 아니다.
local front = redis.call('ZRANGE', KEYS[1], 0, limit - 1)
local swept = 0

if #front > 0 then
    -- **사람마다 ZSCORE 를 부르지 않는다.** K 가 1000 이면 왕복이 아니라
    -- 스크립트 안의 명령이 1000 번이고, 그동안 이벤트 루프가 잡힌다.
    -- 살아 있는 쪽을 한 번에 받아 집합으로 만들고 차집합을 취한다.
    local alive = redis.call('ZRANGEBYSCORE', KEYS[3], now, '+inf')
    local living = {}
    for i = 1, #alive do
        living[alive[i]] = true
    end

    -- 지울 것과 남길 기록을 모아 **한 번의 ZREM·HSET** 으로 끝낸다.
    local gone = {}
    local records = {}
    for i = 1, #front do
        local member = front[i]
        if not living[member] then
            gone[#gone + 1] = member
            -- 자리는 안 보관한다. 재방문자로 식별만 한다 (D-11).
            records[#records + 1] = member
            records[#records + 1] = now
        end
    end

    if #gone > 0 then
        redis.call('ZREM', KEYS[1], unpack(gone))
        redis.call('HSET', KEYS[2], unpack(records))
        swept = #gone
    end
end

-- **정리에도 상한을 둔다** (RD-7). HGETALL 로 전체를 읽으면 기록이 쌓였을 때
-- 이 한 번의 실행이 이벤트 루프를 오래 잡는다. HSCAN 으로 한 묶음만 본다.
--
-- 커서를 끝까지 돌지 않는다. 다음 틱이 이어서 본다 — 한 번에 다 지우려 하면
-- 상한이 있으나 마나다.
local expired = 0
local cutoff = now - retention
local scanned = redis.call('HSCAN', KEYS[2], ARGV[5], 'COUNT', budget)
local fields = scanned[2]
local doomed = {}
for i = 1, #fields, 2 do
    local at = tonumber(fields[i + 1])
    if at == nil or at < cutoff then
        doomed[#doomed + 1] = fields[i]
    end
end

if #doomed > 0 then
    redis.call('HDEL', KEYS[2], unpack(doomed))
    expired = #doomed
end

-- 다음 커서를 돌려준다. 호출부가 이어서 넘긴다.
return {swept, expired, scanned[1]}

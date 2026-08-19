-- 큐 등록. 조회와 등록을 나누면 새로고침 연타에 항목이 둘 생긴다.
--
-- KEYS[1]  queue:{cid}      ZSET. score = Redis TIME 의 마이크로초
-- KEYS[2]  maxscore:{cid}   시계 역행 방어용 바닥값
-- ARGV[1]  memberId
-- ARGV[2]  maxscore TTL(초). 양의 정수
--
-- 반환  {score, floorApplied, alreadyQueued}
--   score          이 사람의 순번
--   floorApplied   바닥값이 적용됐는가. 1 이면 시계가 뒤로 갔다는 뜻이다
--   alreadyQueued  이미 줄에 있었는가. 1 이면 순번을 그대로 돌려준 것이다
--
-- 순번이 카운터가 아니라 **벽시계**다 (A-9). NTP 보정이나 복제본 승격으로
-- 시계가 뒤로 가면 나중에 온 사람이 앞선다 — 불변식 4 가 깨진다.
--
-- 바닥값은 maxscore 하나면 된다. ZSET 의 마지막 원소를 읽는 방식으로는
-- **큐가 빈 동안의 역행**을 못 막는다. 그게 이 키의 존재 이유다.

-- **쓰기 전에 인자를 검증한다.** Lua 는 중간 오류를 되돌리지 않는다 —
-- ZADD 뒤에서 SET 이 터지면 "같이 남거나 같이 사라진다" 는 계약이 깨지고
-- maxscore 없는 ZSET 이 남는다.
local ttl = tonumber(ARGV[2])
if ttl == nil or ttl < 1 or ttl ~= math.floor(ttl) then
    return redis.error_reply('TTL 은 양의 정수여야 한다: ' .. tostring(ARGV[2]))
end

-- **이미 줄에 있으면 그 순번을 지킨다.** 덮어쓰면 새로고침 연타가 자기
-- 자신을 뒤로 민다 — 사용자는 기다릴수록 손해라고 배운다.
local existing = redis.call('ZSCORE', KEYS[1], ARGV[1])
if existing then
    return {existing, 0, 1}
end

local now   = redis.call('TIME')
local score = tonumber(now[1]) * 1000000 + tonumber(now[2])

local floor = tonumber(redis.call('GET', KEYS[2]) or 0)
local applied = 0
if floor >= score then
    score = floor + 1
    applied = 1
end

-- 여기서부터는 둘 다 성공한다. Lua 는 효과 기반 복제라 ZADD 와 SET 이
-- 같이 남거나 같이 사라진다 — maxscore 가 ZSET 보다 뒤처지지 않는다.
redis.call('ZADD', KEYS[1], score, ARGV[1])
redis.call('SET', KEYS[2], score, 'EX', ttl)

return {tostring(score), applied, 0}

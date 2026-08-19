-- 큐 등록. 조회와 등록을 나누면 새로고침 연타에 항목이 둘 생긴다.
--
-- KEYS[1]  queue:{cid}      ZSET. score = Redis TIME 의 마이크로초
-- KEYS[2]  maxscore:{cid}   시계 역행 방어용 바닥값
-- ARGV[1]  memberId
-- ARGV[2]  maxscore TTL(초)
--
-- 반환  {score, floorApplied}
--   score         이 사람의 순번
--   floorApplied  바닥값이 적용됐는가. 1 이면 시계가 뒤로 갔다는 뜻이다
--
-- 순번이 카운터가 아니라 **벽시계**다 (A-9). NTP 보정이나 복제본 승격으로
-- 시계가 뒤로 가면 나중에 온 사람이 앞선다 — 불변식 4 가 깨진다.
--
-- 바닥값은 maxscore 하나면 된다. ZSET 의 마지막 원소를 읽는 방식으로는
-- **큐가 빈 동안의 역행**을 못 막는다. 그게 이 키의 존재 이유다.
--
-- ZADD 와 SET maxscore 는 같이 남거나 같이 사라진다 — Lua 는 효과 기반
-- 복제라 maxscore 가 ZSET 보다 뒤처지는 상태가 존재하지 않는다.

local now   = redis.call('TIME')
local score = tonumber(now[1]) * 1000000 + tonumber(now[2])

local floor = tonumber(redis.call('GET', KEYS[2]) or 0)
local applied = 0
if floor >= score then
    score = floor + 1
    applied = 1
end

redis.call('ZADD', KEYS[1], score, ARGV[1])
redis.call('SET', KEYS[2], score, 'EX', tonumber(ARGV[2]))

return {tostring(score), applied}

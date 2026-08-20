-- 순번 조회. 조회·하트비트·배수 판정이 **한 번에** 일어나야 한다.
--
-- KEYS[1]  queue:{cid}          ZSET
-- KEYS[2]  admitted:{cid}       입장 임계. 개수가 아니라 score 값이다 (D-8)
-- KEYS[3]  alive:{cid}       생존 신호 ZSET. score 는 만료 시각(초)
-- KEYS[4]  grace:{cid}          이탈 기록 해시
-- ARGV[1]  memberId
-- ARGV[2]  alive TTL(초). 양의 정수
-- ARGV[3]  지금 시각(초)
--
-- 반환  {state, rank, score}
--   state  'WAITING' | 'ADMITTED' | 'NOT_QUEUED'
--   rank   내 앞의 인원. 큐에 없으면 -1
--   score  내 순번. 큐에 없으면 '-1'
--
-- **나눠 치면 한쪽만 성공한 상태가 생긴다.** 조회와 하트비트가 갈리면 그때
-- 성실히 새로고침하는 사람이 이탈자로 지워지고, 배수 판정이 갈리면 같은
-- 사람이 두 번 입장한다.

local ttl = tonumber(ARGV[2])
if ttl == nil or ttl < 1 or ttl ~= math.floor(ttl) then
    return redis.error_reply('alive TTL 은 양의 정수여야 한다: ' .. tostring(ARGV[2]))
end

local now = tonumber(ARGV[3])
if now == nil or now < 0 then
    return redis.error_reply('시각은 0 이상이어야 한다: ' .. tostring(ARGV[3]))
end

local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not score then
    -- **0번째와 구분한다.** 없는 것과 맨 앞인 것은 다르다. 뭉치면 유실된
    -- 사람에게 "곧 입장" 을 보여 주게 된다.
    return {'NOT_QUEUED', -1, '-1'}
end

-- 폴링이 곧 생존 신호다. 조회한 김에 갱신한다 — 왕복을 늘리지 않는다.
redis.call('ZADD', KEYS[3], now + ttl, ARGV[1])

local admitted = tonumber(redis.call('GET', KEYS[2]) or -1)
if admitted >= 0 and tonumber(score) <= admitted then
    -- 차례가 왔다. 큐에서 빼지 않으면 대기 인원이 계속 부풀고 ETA 가 틀어진다.
    redis.call('ZREM', KEYS[1], ARGV[1])
    redis.call('ZREM', KEYS[3], ARGV[1])
    redis.call('HSET', KEYS[4], ARGV[1], 'admitted')
    return {'ADMITTED', 0, score}
end

-- **개수를 세지 순위를 저장하지 않는다.** 저장하면 앞사람이 빠질 때마다
-- 전원을 갱신해야 한다.
local rank = redis.call('ZCOUNT', KEYS[1], '-inf', '(' .. score)
return {'WAITING', rank, score}

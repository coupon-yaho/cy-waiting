-- 큐 등록. 조회와 등록을 나누면 새로고침 연타에 항목이 둘 생긴다.
--
-- KEYS[1]  queue:{cid}          ZSET. score = Redis TIME 의 마이크로초
-- KEYS[2]  maxscore:{cid}       시계 역행 방어용 바닥값
-- KEYS[3]  alive:{cid}       생존 신호 ZSET. score 는 만료 시각(초)
-- ARGV[1]  memberId
-- ARGV[2]  maxscore TTL(초). 양의 정수
-- ARGV[3]  alive TTL(초). 양의 정수. 폴링 간격에서 나온 값이라 주입받는다
-- ARGV[4]  큐 길이 상한. **-1 이 상한 없음이고 0 은 전원 거절이다**
--          도메인은 상한 0 을 "배수할 수 없으니 받지 않는다" 로 읽는다. 여기서
--          0 을 상한 없음으로 읽으면 그 뜻이 정반대가 되고, 배수가 멎은 쿠폰의
--          줄이 무한히 자란다 — 갇힌 사람만 늘어난다
-- ARGV[5]  지금 시각(초). 생존 신호의 만료 시각을 계산한다
--
-- 반환  {score, floorApplied, alreadyQueued, rank}
--   score          이 사람의 순번. 거부되면 '-1'
--   floorApplied   바닥값이 적용됐는가. 1 이면 시계가 뒤로 갔다는 뜻이다
--   alreadyQueued  이미 줄에 있었는가. 1 이면 순번을 그대로 돌려준 것이다
--   rank           내 앞의 인원. 거부되면 -1
--
-- **rank 를 여기서 함께 낸다.** 등록하고 따로 물으면 그 사이 앞사람이 빠져
-- 자기 순번보다 작은 수를 받는다 — 사용자가 보기엔 줄이 뒤로 간 것이다.
--
-- 순번이 카운터가 아니라 **벽시계**다 (A-9). NTP 보정이나 복제본 승격으로
-- 시계가 뒤로 가면 나중에 온 사람이 앞선다 — 불변식 4 가 깨진다.
--
-- 바닥값은 maxscore 하나면 된다. ZSET 의 마지막 원소를 읽는 방식으로는
-- **큐가 빈 동안의 역행**을 못 막는다. 그게 이 키의 존재 이유다.

-- **쓰기 전에 인자를 검증한다.** Lua 는 중간 오류를 되돌리지 않는다 —
-- ZADD 뒤에서 SET 이 터지면 "같이 남거나 같이 사라진다" 는 계약이 깨지고
-- maxscore 없는 ZSET 이 남는다.
local function positive_int(value, name)
    local n = tonumber(value)
    if n == nil or n < 1 or n ~= math.floor(n) then
        return nil, name .. ' 은 양의 정수여야 한다: ' .. tostring(value)
    end
    return n
end

local scoreTtl, err = positive_int(ARGV[2], 'maxscore TTL')
if not scoreTtl then return redis.error_reply(err) end

local aliveTtl
aliveTtl, err = positive_int(ARGV[3], 'alive TTL')
if not aliveTtl then return redis.error_reply(err) end

local now = tonumber(ARGV[5])
if now == nil or now < 0 then
    return redis.error_reply('시각은 0 이상이어야 한다: ' .. tostring(ARGV[5]))
end

local maxLen = tonumber(ARGV[4])
if maxLen == nil or maxLen < -1 or maxLen ~= math.floor(maxLen) then
    return redis.error_reply('큐 길이 상한은 -1 이상 정수여야 한다: ' .. tostring(ARGV[4]))
end

-- **이미 줄에 있으면 그 순번을 지킨다.** 덮어쓰면 새로고침 연타가 자기
-- 자신을 뒤로 민다 — 사용자는 기다릴수록 손해라고 배운다.
--
-- 상한 검사보다 앞이다. 이미 선 사람을 상한 때문에 쫓아내면, 줄이 길어진
-- 것이 그 사람 잘못이 아닌데 그가 자리를 잃는다.
local existing = redis.call('ZSCORE', KEYS[1], ARGV[1])
if existing then
    redis.call('ZADD', KEYS[3], now + aliveTtl, ARGV[1])
    return {existing, 0, 1, redis.call('ZCOUNT', KEYS[1], '-inf', '(' .. existing)}
end

-- 2차 방어다. 1차는 도메인이 낡은 스냅샷으로 판정하므로 여기서 한 번 더 본다.
-- **0 도 상한이다.** 배수할 수 없는 쿠폰은 한 명도 안 받는다.
if maxLen >= 0 and redis.call('ZCARD', KEYS[1]) >= maxLen then
    return {'-1', 0, 0, -1}
end

-- 이름을 now 와 겹치지 않게 둔다. 주입받은 시각(초)과 Redis 시계(μs)는
-- 단위도 출처도 다른 값이라 한 이름을 쓰면 조용히 섞인다.
local redisTime = redis.call('TIME')
local score = tonumber(redisTime[1]) * 1000000 + tonumber(redisTime[2])

local floor = tonumber(redis.call('GET', KEYS[2]) or 0)
local applied = 0
if floor >= score then
    score = floor + 1
    applied = 1
end

-- **복제 단위로는 함께 움직인다.** Lua 는 효과 기반 복제라 이 스크립트가
-- 남긴 쓰기는 복제본과 AOF 에 통째로 가거나 통째로 안 간다 — maxscore 가
-- ZSET 보다 뒤처진 채 복제되는 상태는 없다.
--
-- **다만 스크립트 안의 롤백은 없다.** 아래 세 명령 중 하나가 런타임 오류를
-- 내면 앞의 것은 그대로 남는다. 그래서 실패할 수 있는 것(인자 검증)을 전부
-- 위로 올려 뒀다 — 여기 도달하면 남는 실패 경로는 메모리 부족뿐이고,
-- 그건 maxmemory 로 막는다.
redis.call('ZADD', KEYS[1], score, ARGV[1])
redis.call('SET', KEYS[2], score, 'EX', scoreTtl)
redis.call('ZADD', KEYS[3], now + aliveTtl, ARGV[1])

-- **tostring 을 쓰지 않는다.** Lua 5.1 은 수를 %.14g 로 문자열화하는데
-- 마이크로초 score 는 16자리라 과학 표기로 접히며 최대 100μs 가 반올림된다.
-- ZSET 에는 정확한 값이 들어가므로 **돌려준 값과 실제 자리가 어긋난다** —
-- 내림 쪽으로 접히면 앞사람보다 작은 score 를 쥐고 추월한다 (불변식 4).
--
-- %d 가 아니라 %.0f 다. %d 는 정수로 캐스팅해 32비트 런타임에서 넘친다.
-- %.0f 는 배정밀도 그대로 찍으므로 2^53 아래에서 정확하고 지수도 안 붙는다.
-- **개수를 세지 순위를 저장하지 않는다.** 저장하면 앞사람이 빠질 때마다
-- 전원을 갱신해야 한다.
local rank = redis.call('ZCOUNT', KEYS[1], '-inf', '(' .. string.format('%.0f', score))
return {string.format('%.0f', score), applied, 0, rank}

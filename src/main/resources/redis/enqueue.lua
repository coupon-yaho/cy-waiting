-- 큐 등록. 조회와 등록을 나누면 새로고침 연타에 항목이 둘 생긴다.
--
-- KEYS[1]  queue:{cid}          ZSET. score = Redis TIME 의 마이크로초
-- KEYS[2]  maxscore:{cid}       시계 역행 방어용 바닥값
-- KEYS[3]  alive:{cid}       생존 신호 ZSET. score 는 만료 시각(초)
-- KEYS[4]  admitted:{cid}       배분이 올린 임계. 이 값 이하는 이미 들여보낸 사람
-- KEYS[5]  grace:{cid}          이탈 기록 해시. 돌아온 사람을 알아보는 자리
-- ARGV[1]  memberId
-- ARGV[2]  maxscore TTL(초). 양의 정수
-- ARGV[3]  alive TTL(초). 양의 정수. 폴링 간격에서 나온 값이라 주입받는다
-- ARGV[4]  큐 길이 상한. **-1 이 상한 없음, 0 은 전원 거절이다** — 도메인이 0 을
--          "배수할 수 없으니 받지 않는다" 로 읽으므로 뒤집으면 배수가 멎은 쿠폰의
--          줄이 무한히 자란다. 상한은 신규 등록에만 걸리고, 자기 샤드의 수와
--          비교하므로 샤드가 여럿이 되면 나눠 넘겨야 한다 (지금은 샤드 1 만 허용)
-- ARGV[5]  지금 시각(초). 생존 신호의 만료 시각을 계산한다
-- ARGV[6]  이탈 기록 보관 기간(초). 이보다 낡은 기록은 재방문으로 안 본다
--
-- 반환  {score, floorApplied, alreadyQueued, rank, rejoined}
--   score          이 사람의 순번. 거부되면 '-1'
--   floorApplied   바닥값이나 입장 커서가 점수를 밀어 올렸는가. 1 이면 시계가 뒤로 갔다는 뜻이다
--   alreadyQueued  이미 줄에 있었는가. 1 이면 순번을 그대로 돌려준 것이다
--   rank           내 앞의 인원. 거부되면 -1
--   rejoined       자리를 비웠다 돌아왔는가
--
-- **돌아와도 순번은 안 돌려준다.** 비운 사이에 온 사람을 뒤로 밀면 그건
-- 추월이다. rank 를 여기서 함께 내는 것도 같은 이유다 — 따로 물으면 그 사이 앞사람이
-- 빠져 자기 순번보다 작은 수를 받고, 사용자가 보기엔 줄이 뒤로 간 것이다.
--
-- 순번이 카운터가 아니라 **벽시계**다. 시계가 뒤로 가면 나중에 온 사람이 앞서 줄 선
-- 사람을 추월하므로 maxscore 와 ZSET 의 마지막 원소 중 큰 쪽으로 바닥을 깐다. 마지막 원소로는
-- **큐가 빈 동안의 역행**을, maxscore 로는 **키가 만료된 뒤의 역행**을 못 막아 둘 다 본다.

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

local retention
retention, err = positive_int(ARGV[6], '유예 보관 기간')
if not retention then return redis.error_reply(err) end

local maxLen = tonumber(ARGV[4])
if maxLen == nil or maxLen < -1 or maxLen ~= math.floor(maxLen) then
    return redis.error_reply('큐 길이 상한은 -1 이상 정수여야 한다: ' .. tostring(ARGV[4]))
end

-- **앞 인원의 기준은 하나다.** 입장 커서 위에서 센다 — 형제(queue_status)가 그렇게 세므로,
-- 여기서만 -inf 에서 세면 등록 응답과 그 직후 첫 폴링이 같은 사람에게 다른 수를 답한다.
-- **깨진 임계로 비교하면 그 쿠폰의 등록이 전부 예외로 떨어진다.** 부르는 쪽이 그것을
-- 삼켜 fail-open 으로 흘리므로, 등록이 아니라 통과가 된다.
local admittedRaw = redis.call('GET', KEYS[4])
local admitted = admittedRaw and tonumber(admittedRaw) or -1
if admitted ~= admitted or admitted == math.huge or admitted == -math.huge then
    admitted = -1
end
local from = admitted >= 0 and ('(' .. string.format('%.0f', admitted)) or '-inf'

-- **이미 줄에 있으면 그 순번을 지킨다.** 덮어쓰면 새로고침 연타가 자기 자신을 뒤로
-- 민다. 상한 검사보다 앞인 것은, 줄이 길어진 것이 그 사람 잘못이 아닌데 이미 선
-- 사람이 상한 때문에 자리를 잃으면 안 되기 때문이다.
local existing = redis.call('ZSCORE', KEYS[1], ARGV[1])
if existing then
    redis.call('ZADD', KEYS[3], now + aliveTtl, ARGV[1])
    return {existing, 0, 1, redis.call('ZCOUNT', KEYS[1], from, '(' .. existing), 0}
end

-- 2차 방어다. 1차는 도메인이 낡은 스냅샷으로 판정하므로 여기서 한 번 더 본다.
-- **0 도 상한이고, 기다리는 사람만 센다.** ZSET 은 입장자를 안 지우므로 ZCARD 를
-- 그대로 쓰면 실제로 0 명 기다리는데 신규가 영구 거절되는 상태가 된다.
if maxLen >= 0 then
    if redis.call('ZCOUNT', KEYS[1], from, '+inf') >= maxLen then
        return {'-1', 0, 0, -1, 0}
    end
end

-- 이름을 now 와 겹치지 않게 둔다. 주입받은 시각(초)과 Redis 시계(μs)는
-- 단위도 출처도 다른 값이라 한 이름을 쓰면 조용히 섞인다.
local redisTime = redis.call('TIME')
local score = tonumber(redisTime[1]) * 1000000 + tonumber(redisTime[2])

local floor = tonumber(redis.call('GET', KEYS[2]) or 0)
-- **줄의 가장 뒤 점수도 바닥이다.** 바닥값 키는 수명이 있어, 앞선 시계로 줄이 쌓인 채 만료되면 새 사람이
-- 대기자 앞에 선다. 빈 줄은 바닥값이, 찬 줄은 줄 자신이 막는다.
local last = redis.call('ZRANGE', KEYS[1], -1, -1, 'WITHSCORES')
if last[2] ~= nil and tonumber(last[2]) > floor then
    floor = tonumber(last[2])
end
local applied = 0
if floor >= score then
    score = floor + 1
    applied = 1
end
-- **입장 커서 위에 세운다** (CY-942). 시계가 뒤처진 복제본이 승격되면 새 점수가 커서 아래로 나오고, 그 사람은
-- 첫 폴링에 바로 입장이 된다 — 배분이 크레딧을 안 쓴 사람이 줄 선 사람을 앞지른다. 바닥값만으로는 못 막는다:
-- 바닥값은 복제된 점수 위만 보장하고, 배분이 되살린 커서는 그보다 앞설 수 있다. 정상 구간에서 커서는 이미
-- 선 사람의 점수라 시계보다 뒤에 있으므로, 이 갈래에 들어오면 시계가 뒤로 간 것이다.
if admitted >= 0 and admitted >= score then
    score = math.floor(admitted) + 1
    applied = 1
end

-- **세 쓰기는 함께 가거나 함께 안 간다.** 복제본은 효과 기반 복제가 한 단위로 보내고,
-- AOF 는 적재가 잘려도 마지막 유효 명령까지만 되감아(`aof-load-truncated yes`) 중간에
-- 끊긴 명령을 통째로 뺀다. 스크립트 안의 롤백은 없어서 실패할 수 있는 인자 검증을 전부
-- 위로 올려 뒀다 — 여기 남는 실패 경로는 maxmemory 로 막는 메모리 부족뿐이다.
redis.call('ZADD', KEYS[1], score, ARGV[1])
redis.call('SET', KEYS[2], score, 'EX', scoreTtl)
redis.call('ZADD', KEYS[3], now + aliveTtl, ARGV[1])

-- **tostring 도 %d 도 아니라 %.0f 다.** Lua 5.1 의 %.14g 는 16자리 마이크로초 score 를
-- 과학 표기로 접어 내림 쪽이면 앞사람보다 작은 score 로 추월하고, %d 는 32비트
-- 런타임에서 넘친다. 순위는 저장하지 않고 센다 — 저장하면 매번 전원을 갱신해야 한다.
local rank = redis.call('ZCOUNT', KEYS[1], from, '(' .. string.format('%.0f', score))
-- **이탈 기록만 소비하고 입장 표시는 안 건드린다.** 같은 해시에 종류가 둘이라, 입장
-- 표시를 지우면 입장 복구가 통째로 없어진다. 보관 기간을 여기서도 재는 것은 스위퍼가
-- 안 도는 구간에 낡은 기록이 남아 몇 시간 전에 떠난 사람이 재방문자로 나와서다.
local rejoined = 0
local record = redis.call('HGET', KEYS[5], ARGV[1])
if record and string.sub(record, 1, 2) == 'd:' then
    local at = tonumber(string.sub(record, 3))
    if at ~= nil and at >= now - retention then
        rejoined = 1
    end
    redis.call('HDEL', KEYS[5], ARGV[1])
end

return {string.format('%.0f', score), applied, 0, rank, rejoined}

-- 이탈자 청소. **한 번의 실행이 하는 일에 상한을 둔다.**
--
-- KEYS[1]  queue:{cid}   ZSET
-- KEYS[2]  grace:{cid}   이탈 기록 해시
-- KEYS[3]  alive:{cid}   생존 신호 ZSET. score 는 만료 시각(초)
-- KEYS[4]  admitted:{cid} 입장 임계. **창의 시작점**이고, 이 값 이하는 안 걷는다
-- KEYS[5]  applyfence:{cid} 이 쿠폰에 마지막으로 적용한 임기. 같은 슬롯이라 여기서 읽는다
-- ARGV[1]  검사 범위 K. **입장 임계 위에서** 이만큼만 본다. 1..3999
-- ARGV[2]  지금 시각(초). 도메인처럼 주입받는다
-- ARGV[3]  유예 보관 기간(초)
-- ARGV[4]  정리 예산. 만료 신호와 유예 기록을 각각 이만큼까지 지운다. 1..7999
-- ARGV[5]  HSCAN 커서. 첫 호출은 '0'. 반환된 값을 다음에 넘긴다
-- ARGV[6]  앞줄에서 빼도 되는가 (1/0). **0 이어도 정리는 돈다** — 승계 유예 구간에
--          대상까지 비우면 기록이 한 방향으로만 자라고 커서가 전진을 못 한다
-- ARGV[7]  이 회차의 임기. 울타리보다 낮으면 **앞줄만 안 뺀다** — 정리는 돈다
--
-- 반환  {swept, expiredSignals, expiredGrace, nextCursor, 막혔는가(1/0)}
--        **막힌 것을 따로 낸다.** 0 으로 접으면 아무것도 안 걷은 회차와 구분이 안 되고,
--        정리는 그때도 돌므로 걷은 수만으로는 못 가린다
--
-- 이탈 기록 해시의 값  'd:<초>' 이탈 기록 · 'a:<초>' 입장 표시 · 접두사 없는 값은
-- 종류가 생기기 전의 이탈 기록. queue_status 가 같은 자리에 입장 표시를 쓰므로,
-- 안 가르면 입장한 사람이 1초 뒤 폴링에서 종료를 받는다.
--
-- **키를 문자열로 조립하지 않는다.** KEYS 에 선언되지 않은 키를 만지면 클러스터가
-- 거부한다.
--
-- **모든 순회에 상한이 걸려 있어야 한다.** Lua 는 도는 동안 다른 요청을 전부 막으므로,
-- 어딘가에서 전체를 훑으면 K 를 작게 준 것이 아무 의미가 없다.

-- **unpack 한계를 호출부가 우회 못 하게 여기서 막는다.** 넘기면 ZMSCORE 나
-- HSET 이 'too many results to unpack' 으로 죽고, 같은 자리가 매 틱 반복되면
-- 큐가 영구 정지한다. 기록은 인자가 쌍이라 검사 범위 쪽이 먼저 걸린다.
local MAX_SCAN = 3999
local MAX_BUDGET = 7999

local limit = tonumber(ARGV[1])
if limit == nil or limit < 1 or limit ~= math.floor(limit) then
    return redis.error_reply('검사 범위는 양의 정수여야 한다: ' .. tostring(ARGV[1]))
end
if limit > MAX_SCAN then
    return redis.error_reply(
            '검사 범위는 ' .. MAX_SCAN .. ' 이하여야 한다: ' .. string.format('%.0f', limit))
end

local now = tonumber(ARGV[2])
if now == nil or now < 0 then
    return redis.error_reply('시각은 0 이상이어야 한다: ' .. tostring(ARGV[2]))
end
-- **nan 과 무한은 비교로 안 걸린다.** 0 이상인지만 보면 통과하는데,
-- 그 값이 기록에 굳으면 그 항목은 어떤 보관 기간으로도 안 걷힌다.
if now ~= now or now == math.huge or now == -math.huge then
    return redis.error_reply('시각은 유한해야 한다: ' .. tostring(ARGV[2]))
end

local retention = tonumber(ARGV[3])
if retention == nil or retention < 1 or retention ~= math.floor(retention) then
    return redis.error_reply('유예 보관 기간은 양의 정수여야 한다: ' .. tostring(ARGV[3]))
end

local budget = tonumber(ARGV[4])
if budget == nil or budget < 1 or budget ~= math.floor(budget) then
    return redis.error_reply('정리 예산은 양의 정수여야 한다: ' .. tostring(ARGV[4]))
end
if budget > MAX_BUDGET then
    return redis.error_reply(
            '정리 예산은 ' .. MAX_BUDGET .. ' 이하여야 한다: ' .. string.format('%.0f', budget))
end

-- 종류를 뗀 시각. 못 읽으면 nil 이고, 부르는 쪽이 그것을 낡음으로 본다 —
-- 형식이 깨진 값은 남겨 둘 근거가 없다.
local function stampOf(value)
    if type(value) ~= 'string' then
        return nil
    end
    local kind = string.sub(value, 1, 2)
    local at = (kind == 'd:' or kind == 'a:')
            and tonumber(string.sub(value, 3))
            or tonumber(value)
    -- **nan 과 무한은 어떤 비교도 참으로 안 만든다.** 그대로 돌려주면 그 항목이
    -- 영영 안 걷힌다 — 형식이 깨진 값은 남겨 둘 근거가 없으므로 낡음으로 본다.
    if at == nil or at ~= at or at == math.huge or at == -math.huge or at < 0 then
        return nil
    end
    return at
end

-- 기본은 1 이다. 안 넘기면 이 스크립트를 부르던 자리가 그대로 돈다.
local removeFront = ARGV[6] == nil and 1 or tonumber(ARGV[6])
if removeFront ~= 0 and removeFront ~= 1 then
    return redis.error_reply('제거 여부는 0 또는 1 이어야 한다: ' .. tostring(ARGV[6]))
end

local term = tonumber(ARGV[7])
if term == nil or term ~= term or term ~= math.floor(term) then
    return redis.error_reply('임기는 정수여야 한다: ' .. tostring(ARGV[7]))
end

-- **낡은 임기는 앞줄을 못 뺀다.** 리더 판정은 회차 시작에 로컬 플래그를 한 번 읽는
-- 것뿐이라, 회차 도중 리스가 끝난 유령이 그대로 걷으러 온다.
--
-- **막는 것은 되돌릴 수 없는 쓰기뿐이다.** 정리까지 막으면 유예 기록과 만료 신호의
-- 유일한 리퍼가 표 수명 내내 멎어 한 방향으로만 자란다 — `ARGV[6]` 이 같은 이유로
-- 이미 그렇게 갈라져 있다.
--
-- 0 이하는 리더가 아니라는 뜻이라 표가 없어도 막는다. 표가 있는데 번호가 낮은 것도
-- 막는다. 표가 없고 번호가 성하면 세운 적이 없다는 뜻이라 들인다 — 막으면 첫 회차가
-- 영영 안 돈다. 같은 번호의 재시도도 안 막는다.
-- **깨진 표는 없는 것으로 본다.** 다른 울타리 여섯이 같은 줄을 쓴다 — 막는 쪽으로
-- 접으면 지수 표기 하나가 그 쿠폰의 앞줄 제거를 영영 세운다.
local fenced = tonumber(redis.call('GET', KEYS[5]))
local fencedOut = term <= 0
        or (fenced ~= nil and fenced == fenced and term < fenced)

-- **커서도 쓰기 전에 본다.** 형식이 틀리면 HSCAN 이 오류를 내는데, 그때는 이미
-- 앞의 쓰기가 끝나 있다. Lua 는 롤백하지 않는다.
local cursor = ARGV[5]
if cursor == nil or not string.match(cursor, '^%d+$') then
    return redis.error_reply('커서는 숫자여야 한다: ' .. tostring(cursor))
end

-- **차례가 온 사람은 안 건드린다.** 배분은 임계만 올리고 큐에서 빼지 않는다 — 빼는
-- 것은 그 사람이 폴링할 때다. 걷으면 그가 다음 폴링에서 종료를 받아 다시 서고,
-- 그동안 온 사람들 뒤로 밀린다.
local rawAdmitted = redis.call('GET', KEYS[4])

-- **없는 것과 깨진 것을 가른다.** -1 은 보수적인 값이 아니라 **가장 공격적인** 값이다 —
-- 창이 큐 전체로 열린다. 없으면 새 쿠폰이라 그게 맞지만, 깨진 값을 그리로 접으면
-- 차례가 왔던 사람까지 통째로 걷힌다.
local admitted = -1
local usableAdmitted = rawAdmitted == false
if rawAdmitted then
    -- nan 은 비교가 전부 거짓이고, inf 는 `(inf` 로 나가 Redis 가 오류 없이 빈 집합을
    -- 준다 — 뒤엣것이 더 나쁘다. 아무 신호 없이 청소만 멎는다. 그래서 깨진 값은 앞줄
    -- 제거만 접고, 아래 정리는 그대로 돌려 커서를 전진시킨다.
    admitted = tonumber(rawAdmitted)
    usableAdmitted = admitted ~= nil and admitted == admitted
            and admitted ~= math.huge and admitted ~= -math.huge
    if not usableAdmitted then
        admitted = -1
    end
end

-- **살아 있는 신호가 하나도 없으면 앞줄을 안 걷는다.** 줄에 사람이 있는데 아무도 안
-- 살아 있다는 것은 전원이 떠난 것이 아니라 **그 저장소를 잃었다** 는 뜻이다 — 뒤처진
-- 복제본 승격, AOF 유실, 매진이 길어져 폴링이 멎은 뒤의 회복이 전부 이 모양이다.

-- **`ZCARD` 가 아니라 `ZCOUNT` 다.** 만료된 신호는 아래 정리가 걷기 전까지 남아 있어
-- 개수만 보면 "전부 만료" 를 "살아 있다" 로 읽는다. `removeFront` 가 0 이거나 울타리에
-- 막히면 `and` 가 왼쪽에서 끊어 `ZCOUNT` 조차 안 돈다 — 읽기끼리의 교환이라 첫 쓰기
-- 앞은 그대로다.
local removing = removeFront == 1 and not fencedOut
        and redis.call('ZCOUNT', KEYS[3], now, '+inf') > 0

-- **임계 위에서 K 명을 센다.** 순번 0 부터 세면 안 걷어 간 사람이 쌓인 만큼 창이
-- 막혀 살아 있는 구간에 영영 안 닿는다 — 실측에서 이탈 30% 에 낭비 36.9% 였다.
-- 창의 크기는 그대로고, 자리만 살아 있는 쪽으로 옮긴다.

-- **임계를 내림한 정수로 적는다.** 그냥 이어 붙이면 Lua 가 유효숫자 열넷으로 줄이는데
-- 큐 score 는 마이크로초라 열여섯 자리다. 반올림이 위로 가면 곧 차례가 올 사람이 창에서
-- 빠지고, 내리면 창이 한 칸 넓어질 뿐이라 아래에서 그 칸을 되잡는다.

-- **순번은 같이 안 받는다.** 원소당 비용이 정확히 두 배인데 쓰는 곳은 그 한 칸을
-- 되잡는 자리뿐이다. 걷을 회차가 아니면 창을 읽지도 않는다 — 승계 유예는 모든 쿠폰에
-- 대해 접고 도는데 그 회차가 실측으로 3.7~7.6ms 였다.
local front = {}
local nFront = 0
local skip = 0
if usableAdmitted and removing then
    local bound = string.format('%.0f', math.floor(admitted))
    front = redis.call('ZRANGEBYSCORE', KEYS[1], '(' .. bound, '+inf', 'LIMIT', 0, limit)
    -- **`#t` 를 한 번만 쓴다.** Lua 5.1 의 길이 연산자는 매번 이진 탐색을 돌아,
    -- 원소마다 부르면 누적이 O(n log n) 이 된다. 원소 3,000 개에서 649μs 대 141μs 다.
    nFront = #front
    -- **넓어진 한 칸에 임계 이하가 든다.** 걷으면 아직 차례가 안 온 사람이 다시 서서
    -- 통째로 추월당한다. 순번은 앞에서부터 오르고 동점도 붙어 나오니 그런 사람은 맨
    -- 앞에 몰린다 — 몇 명인지만 세고 그만큼 건너뛴다. 내려 적은 값이 임계와 같으면
    -- (정상 경로가 임계를 정수로만 적으므로 늘 그렇다) 세지도 않는다.
    --
    -- **자릿수를 박아 넘긴다.** 수를 그대로 넘기면 Lua 가 유효숫자 열넷으로 줄이는데
    -- 큐 순번은 마이크로초라 열여섯 자리다. 17 자리면 배정도가 그대로 왕복한다.
    if tonumber(bound) < admitted then
        skip = redis.call('ZCOUNT', KEYS[1],
                '(' .. bound, string.format('%.17g', admitted))
        -- 창은 K 로 잘리는데 세는 것은 안 잘린다. 넘으면 전부 건너뛴다.
        if skip > nFront then
            skip = nFront
        end
    end
end
local swept = 0

if nFront > 0 then
    -- **앞부분의 score 만 묻는다.** ZRANGEBYSCORE 로 살아 있는 쪽을 다 받으면
    -- K 를 1 로 줘도 alive 전체 크기에 비례해 이벤트 루프를 잡는다.
    local scores = redis.call('ZMSCORE', KEYS[3], unpack(front))
    -- 만료 시각을 한 번만 푼다. 가드와 본 루프가 같은 배열을 쓴다.
    local aliveAt = {}
    for i = 1, nFront do
        aliveAt[i] = tonumber(scores[i])
    end

    -- **창 안으로 좁히지 않는다.** 회복 구간은 정의상 누군가 먼저 돌아오는 회차라
    -- 창 안에 살아난 한 명이 생기고 그가 나머지를 걷는다. 좁히면 창 안이 전부 진짜
    -- 이탈자일 때 아무도 못 걷고, 크레딧이 0 이면 임계도 안 움직여 그대로 굳는다.

    -- 회복 구간을 지키는 것은 SweepGate 의 재개 유예다. 그것이 리더 메모리라
    -- 승계에서 사라지는 것이 진짜 구멍이고, 레디스로 내려야 풀린다.
    local gone = {}
    local records = {}
    local nGone = 0
    local nRecords = 0
    -- 넓어진 칸에 든 앞쪽은 위에서 이미 걸러 냈다.
    for i = skip + 1, removing and nFront or 0 do
        -- score 가 없거나 이미 지난 것은 폴링이 끊긴 것이다
        local at = aliveAt[i]
        -- **입장 표시는 덮어쓴다.** 차례가 왔던 사람이 다시 줄을 서면 그 표시가 남은
        -- 채로 임계 위에 선다. 큐에서만 빼고 표시를 남기면 다음 폴링에 조회가 입장이라
        -- 답해 줄 전체를 추월하고 초과 발급이 된다.

        -- **임계 위의 `a:` 는 낡은 값임이 증명된다.** 지금 차례가 온 사람은 임계 아래라
        -- 창에 없다. 건너뛰면 그 사이 임계가 그를 지나가 영영 안 걷히고, 줄 길이가
        -- 영구히 부풀어 그 쿠폰이 한산으로 안 돌아간다.

        -- 기록이 제거보다 먼저라 "표시만 남고 큐에서 빠진" 순간은 없다.
        if at == nil or at < now then
            nGone = nGone + 1
            gone[nGone] = front[i]
            -- 자리는 안 보관한다. 재방문자로 식별만 한다.
            records[nRecords + 1] = front[i]
            records[nRecords + 2] = 'd:' .. string.format('%.0f', now)
            nRecords = nRecords + 2
        end
    end

    if nGone > 0 then
        -- **기록이 먼저다.** 제거를 먼저 하면 그 뒤가 터졌을 때 자리도 잃고 재방문자로도
        -- 식별 안 되는 사람이 남는다. 기록을 먼저 하면 실패 시 남는 것이 "아직 안 빠진
        -- 사람" 이라 다음 틱에 다시 처리된다 — 이 청소를 Lua 로 둔 이유가 그것이다.

        -- 인자가 쌍이라 records 가 gone 의 두 배다. unpack 한계에 이쪽이 먼저 걸리므로
        -- 걸리는 순간 큐는 아직 그대로다. 메모리 상한에서도 같은 순서가 산다 — HSET 은
        -- 거부 대상이지만 ZREM 은 아니라, 먼저 두면 큐만 계속 지운다.

        -- **비면 안 부른다.** 걷을 사람이 전부 입장 표시를 들고 있으면 쓸 기록이 없는데,
        -- 인자 없는 HSET 은 오류다 — ZREM 앞에서 죽어 그 쿠폰의 청소가 매 틱 실패한다.
        if nRecords > 0 then
            redis.call('HSET', KEYS[2], unpack(records))
        end
        redis.call('ZREM', KEYS[1], unpack(gone))
        swept = nGone
    end
end

-- 만료된 생존 신호도 예산 안에서만 걷는다. ZREMRANGEBYSCORE 는 대상 수만큼
-- 도므로 한 번에 다 지우려 하면 그 자체가 오래 걸린다.
local staleSignals = redis.call('ZRANGE', KEYS[3], '-inf', '(' .. now,
        'BYSCORE', 'LIMIT', 0, budget)
local expiredSignals = 0
if #staleSignals > 0 then
    redis.call('ZREM', KEYS[3], unpack(staleSignals))
    expiredSignals = #staleSignals
end

-- **COUNT 는 힌트지 상한이 아니다.** 해시가 조밀하게 인코딩돼 있으면 한 번에
-- budget 보다 많이 돌아온다. 받은 것 중 예산만큼만 지우고 나머지는 다음
-- 호출로 미룬다 — 커서만 넘기면 초과분을 막을 수 없다.
local scanned = redis.call('HSCAN', KEYS[2], cursor, 'COUNT', budget)
local fields = scanned[2]
local cutoff = now - retention
local doomed = {}
local stamped = {}
local nDoomed = 0
local nStamped = 0
-- **받은 것은 끝까지 분류한다.** 커서는 응답 전체 뒤로 전진하므로, 중간에
-- 끊으면 그 뒤 항목이 이번 순회에서 통째로 빠진다 — 다음 한 바퀴가 돌 때까지
-- 안 걷힌다. 예산은 분류가 아니라 **쓰기**에 건다. 비용은 COUNT 로 잡는다.
for i = 1, #fields, 2 do
    local value = fields[i + 1]
    if value == 'admitted' then
        -- **옛 표시에는 시각이 없다.** 그대로 두면 나이를 못 재 영영 안 걷히고, 지금으로
        -- 쳐 주면 매 회차 다시 젊어진다. 지금을 못 박아 다음 회차부터 늙게 한다 — 한
        -- 보관 기간 뒤 옛 값이 사라지면 그때 이 분기를 뗀다.
        stamped[nStamped + 1] = fields[i]
        stamped[nStamped + 2] = 'a:' .. string.format('%.0f', now)
        nStamped = nStamped + 2
    else
        local at = stampOf(value)
        if at == nil or at < cutoff then
            nDoomed = nDoomed + 1
            doomed[nDoomed] = fields[i]
        end
    end
end

-- 예산만큼만 쓴다. 남은 것은 다음 한 바퀴에서 다시 만난다.
local function firstOf(list, count)
    if #list <= count then
        return list
    end
    local cut = {}
    for i = 1, count do
        cut[i] = list[i]
    end
    return cut
end

-- **인자가 쌍이라 예산의 두 배가 아니라 상한의 두 배로 잰다.** budget 이
-- MAX_BUDGET 이면 쌍이 16,000 개가 되어 unpack 한계를 넘는다 — 그러면 그 쿠폰의
-- 청소가 매 틱 같은 자리에서 죽고 커서가 전진을 못 한다.
stamped = firstOf(stamped, math.min(budget, MAX_SCAN) * 2)
-- 자른 뒤라 센 값이 안 맞는다. 여기서는 "쓸 것이 있는가" 판정에만 쓰이므로
-- 결과가 안 갈리지만, 아래 `doomed` 와 같은 모양으로 둔다.
nStamped = #stamped
if nStamped > 0 then
    redis.call('HSET', KEYS[2], unpack(stamped))
end

local expiredGrace = 0
doomed = firstOf(doomed, budget)
-- **여기는 다시 재야 한다.** `firstOf` 가 새 목록으로 바꿔치기하는데 이 수가
-- 반환값에 실린다 — 안 다시 재면 예산에 잘린 회차가 자르기 전 수를 지운 것으로
-- 보고한다. 실제로 그렇게 냈다가 기존 시험이 잡았다.
nDoomed = #doomed
if nDoomed > 0 then
    redis.call('HDEL', KEYS[2], unpack(doomed))
    expiredGrace = nDoomed
end

-- 다음 커서를 돌려준다. 호출부가 이어서 넘긴다.
return {swept, expiredSignals, expiredGrace, scanned[1], fencedOut and 1 or 0}

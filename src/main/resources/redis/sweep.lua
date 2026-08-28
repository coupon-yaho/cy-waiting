-- 이탈자 청소. **한 번의 실행이 하는 일에 상한을 둔다.**
--
-- KEYS[1]  queue:{cid}   ZSET
-- KEYS[2]  grace:{cid}   이탈 기록 해시
-- KEYS[3]  alive:{cid}   생존 신호 ZSET. score 는 만료 시각(초)
-- KEYS[4]  admitted:{cid} 입장 임계. 이 값 이하의 score 는 안 걷는다
-- ARGV[1]  검사 범위 K. 큐 앞에서 이만큼만 본다. 1..3999
-- ARGV[2]  지금 시각(초). 도메인처럼 주입받는다
-- ARGV[3]  유예 보관 기간(초)
-- ARGV[4]  정리 예산. 만료 신호와 유예 기록을 각각 이만큼까지 지운다. 1..7999
-- ARGV[5]  HSCAN 커서. 첫 호출은 '0'. 반환된 값을 다음에 넘긴다
--
-- 반환  {swept, expiredSignals, expiredGrace, nextCursor}
--
-- **이탈 기록 해시에는 writer 가 둘이다.** 여기는 이탈 시각을, queue_status 는
-- 입장 표시를 같은 자리에 쓴다. 값에 종류를 실어 가른다 — 안 가르면 이쪽이
-- 입장 표시를 숫자로 못 읽어 낡은 것으로 보고 다음 청소에서 지운다. 그러면
-- 입장한 사람이 1초 뒤 폴링에서 종료를 받는다.
--
--   'd:<초>'  이탈 기록      'a:<초>'  입장 표시
--
-- 접두사가 없는 값은 종류가 생기기 전의 이탈 기록이다. 숫자로 읽는다.
--
-- **키를 문자열로 조립하지 않는다.** 사람마다 alive 키를 만들면 KEYS 에
-- 선언되지 않은 키를 만지게 되고 클러스터가 거부한다 (RD-1).
--
-- **상한은 unpack 한계에서 왔다.** 넘기면 ZMSCORE 나 HSET 이 죽고, 같은 자리가
-- 매 틱 반복되면 큐가 영구 정지한다. 기록이 인자 쌍이라 검사 범위가 먼저 걸린다.
--
-- **모든 순회에 상한이 걸려 있어야 한다.** Lua 는 통째로 도는 동안 다른
-- 요청을 전부 막는다. K 를 작게 줘도 어딘가에서 전체를 훑으면 그 K 는
-- 아무 의미가 없다.

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

-- **커서도 쓰기 전에 본다.** 형식이 틀리면 HSCAN 이 오류를 내는데, 그때는
-- 이미 앞의 쓰기가 끝나 있다. Lua 는 롤백하지 않는다.
local cursor = ARGV[5]
if cursor == nil or not string.match(cursor, '^%d+$') then
    return redis.error_reply('커서는 숫자여야 한다: ' .. tostring(cursor))
end

-- 앞에서 K 명. 뒤는 아직 볼 때가 아니다.
local front = redis.call('ZRANGE', KEYS[1], 0, limit - 1)
local swept = 0

-- **살아 있는 신호가 하나도 없으면 앞줄을 안 걷는다.**
--
-- 줄에 사람이 있는데 아무도 살아 있지 않다는 것은 "전원이 떠났다" 가 아니라
-- **그 저장소를 잃었다** 는 뜻이다. 뒤처진 복제본 승격, AOF 유실, 매진 구간이
-- 길어져 폴링이 멎은 뒤의 회복 — 전부 이 모양이다.
--
-- **`ZCARD` 가 아니라 `ZCOUNT` 다.** 만료된 신호는 아래 정리가 걷기 전까지
-- 물리적으로 남아 있어, 개수만 보면 "전부 만료" 를 "살아 있다" 로 읽는다 —
-- 회복 첫 판이 정확히 그 상태다.
--
-- **정리까지 건너뛰지는 않는다.** 앞줄 제거만 접고 만료 신호와 낡은 기록은
-- 그대로 걷는다. 안 그러면 이 구간이 길어질 때 해시가 한 방향으로만 자라고
-- 커서도 전진을 못 한다.
local anyAlive = redis.call('ZCOUNT', KEYS[3], now, '+inf') > 0

if #front > 0 and anyAlive then
    -- **앞부분의 score 만 묻는다.** ZRANGEBYSCORE 로 살아 있는 쪽을 다 받으면
    -- K 를 1 로 줘도 alive 전체 크기에 비례해 이벤트 루프를 잡는다.
    local scores = redis.call('ZMSCORE', KEYS[3], unpack(front))
    -- **차례가 온 사람은 안 건드린다.** 배분은 임계만 올리고 큐에서 빼지
    -- 않는다 — 빼는 것은 그 사람이 폴링할 때다. 그래서 앞줄에는 "입장
    -- 확정인데 아직 안 걷어간 사람" 이 섞인다. 걷으면 그가 다음 폴링에서
    -- 종료를 받고, 다시 서면 그동안 온 사람 뒤로 간다 (불변식 4).
    local admitted = tonumber(redis.call('GET', KEYS[4])) or -1

    local gone = {}
    local records = {}
    for i = 1, #front do
        -- score 가 없거나 이미 지난 것은 폴링이 끊긴 것이다
        local at = tonumber(scores[i])
        local rank = tonumber(redis.call('ZSCORE', KEYS[1], front[i]))
        if (at == nil or at < now) and (rank == nil or rank > admitted) then
            gone[#gone + 1] = front[i]
            -- **입장 표시는 안 덮는다.** 같은 자리에 종류가 둘이고, `a:` 는
            -- 차례가 왔던 사람을 지키는 표시다. 덮으면 그 사람이 다음 폴링에서
            -- 종료를 받고, 다시 서면 그동안 온 사람 뒤로 간다 (불변식 4).
            local kept = redis.call('HGET', KEYS[2], front[i])
            if not (kept and string.sub(kept, 1, 2) == 'a:') then
                -- 자리는 안 보관한다. 재방문자로 식별만 한다 (D-11).
                records[#records + 1] = front[i]
                records[#records + 1] = 'd:' .. string.format('%.0f', now)
            end
        end
    end

    if #gone > 0 then
        -- **기록이 먼저다.** 제거를 먼저 하면 그 뒤가 터졌을 때 자리도 잃고
        -- 재방문자로도 식별 안 되는 사람이 남는다 — 이 청소를 Lua 로 둔
        -- 이유가 정확히 그것을 막는 것이다. 기록을 먼저 하면 실패 시 남는
        -- 것이 "아직 안 빠진 사람" 이라 다음 틱에 다시 처리된다.
        --
        -- 인자가 쌍이라 records 가 gone 의 두 배다. unpack 한계에 이쪽이
        -- 먼저 걸리므로, 걸리는 순간 큐는 아직 그대로다.
        --
        -- **메모리 상한에서도 이 순서가 산다.** HSET 은 거부 대상이라
        -- 첫 쓰기에서 통째로 막히는데, ZREM 은 거부 대상이 아니라 먼저
        -- 두면 아무도 하트비트를 못 하는 동안 큐만 계속 지운다.
        -- **비면 안 부른다.** 걷을 사람이 전부 입장 표시를 들고 있으면 쓸
        -- 기록이 없는데, 인자 없는 HSET 은 오류다 — 그러면 ZREM 앞에서
        -- 스크립트가 죽고 그 쿠폰의 청소가 매 틱 같은 자리에서 실패한다.
        if #records > 0 then
            redis.call('HSET', KEYS[2], unpack(records))
        end
        redis.call('ZREM', KEYS[1], unpack(gone))
        swept = #gone
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
-- **받은 것은 끝까지 분류한다.** 커서는 응답 전체 뒤로 전진하므로, 중간에
-- 끊으면 그 뒤 항목이 이번 순회에서 통째로 빠진다 — 다음 한 바퀴가 돌 때까지
-- 안 걷힌다. 예산은 분류가 아니라 **쓰기**에 건다. 비용은 COUNT 로 잡는다.
for i = 1, #fields, 2 do
    local value = fields[i + 1]
    if value == 'admitted' then
        -- **옛 표시에는 시각이 없다.** 그대로 두면 나이를 못 재 영영 안 걷히고,
        -- 지금으로 쳐 주면 매 판 다시 젊어져 같은 일이 난다. 지금을 못 박아
        -- 다음 판부터 늙게 한다 — 그러면 한 보관 기간 뒤 옛 값이 사라지고,
        -- 그때 이 분기를 뗀다.
        stamped[#stamped + 1] = fields[i]
        stamped[#stamped + 1] = 'a:' .. string.format('%.0f', now)
    else
        local at = stampOf(value)
        if at == nil or at < cutoff then
            doomed[#doomed + 1] = fields[i]
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
if #stamped > 0 then
    redis.call('HSET', KEYS[2], unpack(stamped))
end

local expiredGrace = 0
doomed = firstOf(doomed, budget)
if #doomed > 0 then
    redis.call('HDEL', KEYS[2], unpack(doomed))
    expiredGrace = #doomed
end

-- 다음 커서를 돌려준다. 호출부가 이어서 넘긴다.
return {swept, expiredSignals, expiredGrace, scanned[1]}

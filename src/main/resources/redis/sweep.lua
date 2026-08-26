-- 이탈자 청소. **한 번의 실행이 하는 일에 상한을 둔다.**
--
-- KEYS[1]  queue:{cid}   ZSET
-- KEYS[2]  grace:{cid}   이탈 기록 해시
-- KEYS[3]  alive:{cid}   생존 신호 ZSET. score 는 만료 시각(초)
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

if #front > 0 then
    -- **앞부분의 score 만 묻는다.** ZRANGEBYSCORE 로 살아 있는 쪽을 다 받으면
    -- K 를 1 로 줘도 alive 전체 크기에 비례해 이벤트 루프를 잡는다.
    local scores = redis.call('ZMSCORE', KEYS[3], unpack(front))

    local gone = {}
    local records = {}
    for i = 1, #front do
        -- score 가 없거나 이미 지난 것은 폴링이 끊긴 것이다
        local at = tonumber(scores[i])
        if at == nil or at < now then
            gone[#gone + 1] = front[i]
            -- 자리는 안 보관한다. 재방문자로 식별만 한다 (D-11).
            records[#records + 1] = front[i]
            records[#records + 1] = 'd:' .. string.format('%.0f', now)
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
        redis.call('HSET', KEYS[2], unpack(records))
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
-- 안 걷힌다. 비용은 COUNT 로 잡는다.
for i = 1, #fields, 2 do
    if #doomed >= budget or #stamped >= budget then
        break
    end
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

if #stamped > 0 then
    redis.call('HSET', KEYS[2], unpack(stamped))
end

local expiredGrace = 0
if #doomed > 0 then
    redis.call('HDEL', KEYS[2], unpack(doomed))
    expiredGrace = #doomed
end

-- 다음 커서를 돌려준다. 호출부가 이어서 넘긴다.
return {swept, expiredSignals, expiredGrace, scanned[1]}

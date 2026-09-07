-- 게이트웨이 하트비트. **시각은 레디스가 찍는다.**
--
-- KEYS[1]  gw:instances   HASH
--            field = instanceId      → 서버 시각(초)
--            field = '#c:'..id       → 그 노드가 본 뒷단 서킷
--            field = '#p:'..id       → 그 노드가 초당 통과시킨 수
-- ARGV[1]  instanceId
-- ARGV[2]  죽은 항목 임계(초). 이보다 오래된 field 는 지운다
-- ARGV[3]  이 노드가 본 뒷단 서킷. 없으면 안 본 것으로 친다
-- ARGV[4]  표를 인정하는 신선도(초). 분모의 임계보다 훨씬 짧다
-- ARGV[5]  이 노드가 초당 통과시킨 수. 없으면 안 잰 것으로 보고 그 field 를 지운다
--
-- 반환  {살아있는 수, 서버 시각(초), 열린 수, 반쯤 열린 수, 표를 낸 수,
--        통과 수 합, 통과 수를 실은 수}
--
-- **서킷을 별도 field 에 싣는다** (CY-791). 값에 붙이면 옛 노드의 tonumber 가
-- nil 을 내고 그 노드를 죽은 것으로 판정해 **지운다** — 롤아웃 내내 옛 리더가
-- 새 노드를 못 세고, 분모가 줄어 남은 노드가 각자 큰 몫을 쓴다. 초과 발급
-- 방향이라 롤아웃 자체가 안전하지 않다. field 를 나누면 옛 노드는 표만 지우고
-- 다음 틱에 다시 실린다 — 그 사이 표가 없는 것은 CLOSED, 안 조이는 쪽이다.
--
-- **열린 것과 반쯤 열린 것을 따로 센다.** 합쳐서 세면 전 노드가 동시에 반쯤
-- 열린 순간이 과반으로 접혀 배분이 0 이 되고, 0 이면 뒷단에 닿는 호출이 없어
-- 서킷이 표본을 못 채운다 — 진입은 있고 해제가 없는 교착이다.
--
-- **노드가 제 벽시계를 찍으면 안 된다.** 시계가 앞선 노드는 영영 신선하고
-- 뒤진 노드는 즉시 만료된다. 한 시계로 재야 비교가 성립한다.
--
-- **쓰기와 정리를 한 스크립트에 둔다.** 나누면 그 사이에 다른 노드가 세어
-- 방금 지운 항목을 살아 있는 것으로 본다.

local VOTE = '#c:'
-- **회복 봉우리를 정상과 견줄 재료다** (RC4). 그 수는 노드마다 제 것만 알고,
-- 배분은 리더 한 대가 돈다. **지표 축이다** — 이 합은 배분의 출력 그 자체라
-- 상한의 기준값이 못 된다 (AIJ-0250).
local PASS = '#p:'

-- **상한과 무한을 함께 막는다.** tonumber 는 1e400 을 inf 로 주는데
-- math.floor(inf) == inf 라 정수 검사를 그냥 통과한다. 그러면 아무것도 영영
-- 안 지워져 해시가 배포 이력만큼 자라고, 매 틱 그걸 다 읽는다.
local MAX_REAP = 86400
local function positiveInt(raw, what, limit)
    local n = tonumber(raw)
    if n == nil or n ~= n or n < 1 or n > limit or n ~= math.floor(n) then
        return nil, redis.error_reply(
                what .. ' 1..' .. limit .. ' 의 정수여야 한다: ' .. tostring(raw))
    end
    return n
end

local reapAfter, err = positiveInt(ARGV[2], '임계는', MAX_REAP)
if reapAfter == nil then
    return err
end
-- 표는 분모보다 훨씬 빨리 낡는다. 죽은 노드의 마지막 표가 분모의 임계만큼
-- 살아 있으면, 시체 하나가 멀쩡한 클러스터를 그 시간 내내 조인다.
local voteFresh
voteFresh, err = positiveInt(ARGV[4], '표 신선도는', reapAfter)
if voteFresh == nil then
    return err
end
if ARGV[1] == nil or ARGV[1] == '' then
    return redis.error_reply('instanceId 는 필수다')
end
-- 예약 접두사를 쓰면 자기 표를 자기 항목으로 오해한다. **짝이 되는 leave 와
-- 같은 문턱이라야 한다** — 여기만 좁으면 등록은 되는데 퇴장만 못 하는 id 가 난다.
if string.sub(ARGV[1], 1, 1) == '#' then
    return redis.error_reply('instanceId 는 # 로 시작할 수 없다')
end

-- **모르는 값은 거절한다.** 합산에 들어가면 전 클러스터의 상한이 그것으로 정해진다.
-- 0 이 정상값이라 positiveInt 를 못 쓴다. 나머지 검사는 같게 둔다 — 여기만
-- 헐거우면 실수·inf 가 합에 섞여 다른 인자를 막은 이유가 무의미해진다.
--
-- **상한을 epoch 근처까지 올리면 안 된다.** 옛 스크립트는 이 field 를 인스턴스
-- 시각으로 읽으므로, 지금 값이 낮아야 죽은 항목으로 보고 지운다. 그것이 새
-- 노드를 분모에 안 세는 유일한 이유다.
local MAX_PASS = 1e9
local passed = ARGV[5]
local measured = passed ~= nil and passed ~= ''
if measured then
    passed = tonumber(passed)
    if passed == nil or passed ~= passed or passed < 0
            or passed > MAX_PASS or passed ~= math.floor(passed) then
        return redis.error_reply(
                '통과 수는 0..' .. MAX_PASS .. ' 의 정수여야 한다: ' .. tostring(ARGV[5]))
    end
end

local now = tonumber(redis.call('TIME')[1])

-- **내 하트비트를 먼저 쓴다.** 정리를 먼저 하면 그 사이 터졌을 때 나까지
-- 빠진 채로 남고, 그러면 남은 노드가 큰 몫을 쓴다.
-- **아는 것만 받는다.** ARGV[1]·ARGV[2] 는 엄격히 막으면서 여기만 그대로
-- 이어 붙이면, 모르는 값이 표로 세어져 전 클러스터의 배분을 멈출 수 있다.
local mine = ARGV[3]
if mine == nil or mine == '' then
    mine = 'CLOSED'
elseif mine ~= 'CLOSED' and mine ~= 'OPEN' and mine ~= 'HALF_OPEN' then
    return redis.error_reply('모르는 서킷 상태다: ' .. tostring(mine))
end
-- **한 번에 쓴다.** 같은 키라 나눌 이유가 없고, 노드마다 매 틱 도는 자리다.
-- **안 잰 노드는 0 이 아니라 없는 것이다** — 0 을 쓰면 "안 잰 노드" 가 "0 을
-- 잰 노드" 로 세어져, 합이 모자란 것을 아무도 모른다.
if measured then
    redis.call('HSET', KEYS[1], ARGV[1], now, VOTE .. ARGV[1], mine,
            PASS .. ARGV[1], passed)
else
    redis.call('HSET', KEYS[1], ARGV[1], now, VOTE .. ARGV[1], mine)
    redis.call('HDEL', KEYS[1], PASS .. ARGV[1])
end

-- 정리는 읽으면서 한다. 배포 이력만큼 해시가 자라면 매 틱 그걸 다 읽는다.
-- **순서를 배열로 고정한다.** pairs 는 순서가 정해져 있지 않아, 같은 해시를
-- 두 번 읽어도 HDEL 인자 순서가 달라진다. 효과는 같지만 재현이 안 되는 것을
-- 스크립트에 남길 이유가 없다.
local ids = {}
local votes = {}
local passes = {}
local seenOf = {}
local voteOf = {}
local passOf = {}
local entries = redis.call('HGETALL', KEYS[1])
for i = 1, #entries, 2 do
    local field = entries[i]
    if string.sub(field, 1, #VOTE) == VOTE then
        local id = string.sub(field, #VOTE + 1)
        voteOf[id] = entries[i + 1]
        votes[#votes + 1] = id
    elseif string.sub(field, 1, #PASS) == PASS then
        local id = string.sub(field, #PASS + 1)
        passOf[id] = tonumber(entries[i + 1])
        passes[#passes + 1] = id
    else
        seenOf[field] = tonumber(entries[i + 1])
        ids[#ids + 1] = field
    end
end

local alive = 0
local open = 0
local halfOpen = 0
local reported = 0
local passSum = 0
-- **합만으로는 "진짜 0" 과 "아무도 안 실었다" 가 안 갈린다.** 롤아웃 중 옛
-- 노드는 새 필드를 죽은 항목으로 보고 매 틱 지운다. 표에는 reported 가 그
-- 구분을 주는데 통과 수에는 대응물이 없어, 상한으로 쓰는 순간 없는 부하를
-- 근거로 조인다. 읽는 쪽이 alive 와 견줘 "모름" 으로 다룰 수 있게 같이 낸다.
local passReported = 0
local dead = {}
for _, id in ipairs(ids) do
    local seen = seenOf[id]
    -- **미래 시각은 죽은 것으로 본다.** 복제본이 승격하면서 시계가 뒤로 가면
    -- 기존 항목이 전부 미래가 되고, now - seen 이 음수라 영영 안 지워진다.
    -- 죽은 노드가 계속 세어져 분모가 부풀고 스스로 회복되지 않는다.
    if seen == nil or seen > now or now - seen > reapAfter then
        dead[#dead + 1] = id
        dead[#dead + 1] = VOTE .. id
        dead[#dead + 1] = PASS .. id
    else
        alive = alive + 1
        -- 죽은 노드의 표는 안 센다. 낡은 표도 안 센다 — 둘 다 이미 없는
        -- 관측이라, 세면 지나간 장애가 지금의 배분을 정한다. **통과 수도 같다.**
        if now - seen <= voteFresh then
            if passOf[id] ~= nil then
                passSum = passSum + passOf[id]
                passReported = passReported + 1
            end
            local vote = voteOf[id]
            if vote ~= nil then
                reported = reported + 1
                if vote == 'OPEN' then
                    open = open + 1
                elseif vote == 'HALF_OPEN' then
                    halfOpen = halfOpen + 1
                end
            end
        end
    end
end

-- 항목이 사라진 표는 남겨 둘 이유가 없다. 안 지우면 해시가 배포 이력만큼 자란다.
for _, id in ipairs(votes) do
    if seenOf[id] == nil then
        dead[#dead + 1] = VOTE .. id
    end
end
for _, id in ipairs(passes) do
    if seenOf[id] == nil then
        dead[#dead + 1] = PASS .. id
    end
end

-- **unpack 한계를 넘기지 않는다.** 한 번에 다 못 지우면 다음 틱이 마저 지운다 —
-- 남은 것은 어차피 죽은 항목이라 세는 값에 영향이 없다.
if #dead > 0 then
    local limit = 4000
    for i = 1, #dead, limit do
        local last = math.min(i + limit - 1, #dead)
        redis.call('HDEL', KEYS[1], unpack(dead, i, last))
    end
end

-- **읽는 쪽의 폭에 맞춰 묶는다.** 노드당 상한만 두면 합이 그 폭을 넘어 감기고,
-- 감긴 음수는 낡은 값을 그대로 쓰게 만든다.
return {alive, now, open, halfOpen, reported,
        math.min(math.floor(passSum), 2147483647), passReported}

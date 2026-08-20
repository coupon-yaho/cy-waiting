-- 리더 획득·연장. **획득과 확인이 갈리면 두 리더가 생긴다.**
--
-- KEYS[1]  scheduler:leader
-- ARGV[1]  ownerId. 이 노드를 가리키는 값
-- ARGV[2]  리스(밀리초). 양의 정수
--
-- 반환  {acquired, owner, ttlMillis}
--   acquired   1 이면 내가 리더다 (새로 잡았거나 연장했다)
--   owner      지금 락을 쥔 노드
--   ttlMillis  남은 리스
--
-- **소유자 ID 를 값에 담는다.** 안 담으면 남의 락을 지울 수 있고, 그러면
-- 리더가 둘이 되어 배분 총합이 전역 크레딧을 넘는다.
--
-- 재진입은 연장이다. 매 틱 새로 잡으려 하면 리더십이 흔들리고, 그때마다
-- 평활화 상태가 초기화된다 (F9).

local lease = tonumber(ARGV[2])
if lease == nil or lease < 1 or lease ~= math.floor(lease) then
    return redis.error_reply('리스는 양의 정수여야 한다: ' .. tostring(ARGV[2]))
end
if ARGV[1] == nil or ARGV[1] == '' then
    return redis.error_reply('ownerId 는 필수다')
end

local current = redis.call('GET', KEYS[1])

if not current then
    -- 아무도 안 잡았다. NX 로 잡아 **경합에서 하나만 이기게** 한다.
    if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', lease) then
        return {1, ARGV[1], lease}
    end
    -- 그 사이 다른 노드가 잡았다. 다시 읽어 사실대로 알린다.
    current = redis.call('GET', KEYS[1])
    return {0, current or '', redis.call('PTTL', KEYS[1])}
end

if current == ARGV[1] then
    -- 내 락이다. 연장한다 — 새로 잡으려 하면 그 틈에 남이 가져간다.
    redis.call('PEXPIRE', KEYS[1], lease)
    return {1, ARGV[1], lease}
end

return {0, current, redis.call('PTTL', KEYS[1])}

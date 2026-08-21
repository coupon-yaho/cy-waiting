-- 자발적 종료. **임계를 안 기다리고 즉시 뺀다.**
--
-- KEYS[1]  gw:instances
-- ARGV[1]  instanceId
--
-- 반환  지운 항목 수 (0 또는 1)
--
-- 죽음이 아니라 통보라 즉시 빼도 된다. 안 그러면 배포마다 임계 시간 동안
-- 분모가 부풀어 전 노드가 몫을 덜 쓴다.

-- 짝이 되는 하트비트 스크립트가 같은 값을 거절한다. 한쪽만 지키는 계약은
-- 계약이 아니다 — 빈 값을 그냥 받으면 아무 일도 안 하고 0 을 돌려줘서,
-- 부른 쪽은 지웠다고 믿는다.
if ARGV[1] == nil or ARGV[1] == '' then
    return redis.error_reply('instanceId 는 필수다')
end

return redis.call('HDEL', KEYS[1], ARGV[1])

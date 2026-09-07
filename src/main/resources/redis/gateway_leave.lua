-- 자발적 종료. **임계를 안 기다리고 즉시 뺀다.**
--
-- KEYS[1]  gw:instances
-- ARGV[1]  instanceId
--
-- 반환  지운 field 수 (0~3). 항목·표·통과 수를 함께 뺀다
--
-- 죽음이 아니라 통보라 즉시 빼도 된다. 안 그러면 배포마다 임계 시간 동안
-- 분모가 부풀어 전 노드가 몫을 덜 쓴다.

-- 짝이 되는 하트비트 스크립트가 같은 값을 거절한다. 한쪽만 지키는 계약은
-- 계약이 아니다 — 빈 값을 그냥 받으면 아무 일도 안 하고 0 을 돌려줘서,
-- 부른 쪽은 지웠다고 믿는다.
if ARGV[1] == nil or ARGV[1] == '' then
    return redis.error_reply('instanceId 는 필수다')
end

-- 짝이 되는 하트비트가 쓰는 접두어와 같아야 한다. 한쪽만 바뀌면 곁딸린
-- field 가 안 지워지고, 그것을 잡는 것은 마지막 노드가 나간 뒤 해시가 비는지
-- 보는 시험뿐이다.
if string.sub(ARGV[1], 1, 1) == '#' then
    return redis.error_reply('instanceId 는 # 로 시작할 수 없다')
end

-- **곁딸린 field 도 같이 뺀다.** 하트비트는 항목이 사라진 표를 다음 틱에
-- 치우지만, 마지막 노드가 나가면 그 틱이 영영 안 온다 — 남은 field 가 다음
-- 기동까지 살아서 첫 HGETALL 이 그걸 다 읽는다.
return redis.call('HDEL', KEYS[1], ARGV[1], '#c:' .. ARGV[1], '#p:' .. ARGV[1])

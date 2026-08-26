-- 배분 대상과 <b>그것을 읽은 시각</b>을 한 번에 읽는다.
--
-- KEYS[1]  coupons:active   배분 대상 쿠폰 집합
-- ARGV     없다. 시각을 밖에서 주면 이 스크립트를 둔 이유가 사라진다
--
-- 반환  {now, coupon1, coupon2, ...}
--   now  이 노드가 아니라 **레디스의** 지금 시각(초)
--
-- **발행 시각은 재료를 읽은 시각이어야 한다.** 리더가 자기 벽시계로 찍으면
-- 각 노드가 자기 벽시계와 비교해 나이를 재므로, 시계가 어긋난 만큼 같은
-- 스냅샷이 노드마다 다르게 낡는다 — 어떤 노드는 fail-open 으로 열리고 어떤
-- 노드는 안 열린다. 리더는 옮겨 다니므로 그 편차가 승계마다 바뀐다.

local now = redis.call('TIME')[1]
local coupons = redis.call('SMEMBERS', KEYS[1])
table.insert(coupons, 1, now)
return coupons

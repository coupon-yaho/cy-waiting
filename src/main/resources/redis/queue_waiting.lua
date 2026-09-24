-- 배분 수요로 쓸 한 샤드의 대기 수. **읽기만 한다.**
--
-- KEYS[1]  queue:{cid}      줄 ZSET. score 는 등록 점수(마이크로초)
-- KEYS[2]  admitted:{cid}   입장 커서. 이 점수 이하가 입장했다
-- KEYS[3]  alive:{cid}      생존 신호 ZSET. score 는 만료 시각(초)
--
-- 반환  max(커서 위의 수, 살아 있는 신호 수)
--
-- **커서 위만 세면 추월이 난다.** 커서가 마지막 사람을 넘는 순간 쿠폰이 한산으로 내려가, 입장했지만 아직 폴링해
-- 오지 않은 사람보다 새로 온 사람이 먼저 뒷단에 닿는다. **줄 전체를 세면 한산으로 영영 못 돌아온다** — 입장한
-- 사람은 폴링해 와야 줄에서 빠져, 안 오고 떠난 한 명이 쿠폰을 붙잡는다. 입장자는 폴링하면 신호에서도 빠지고, 떠난
-- 사람은 신호 수명 뒤에 빠지므로 살아 있는 신호 수가 그 사이를 메운다.
--
-- **세 키를 한 스크립트에서 읽는다.** 같은 슬롯이라 한 왕복이고, 신호의 만료를 레디스 시계로 잰다.

local cursor = tonumber(redis.call('GET', KEYS[2]) or '')
local waiting
-- 커서가 없거나 깨졌으면 모르는 것이다. 줄 전체를 센다 — 덜 세면 줄 선 사람이 있는데 한산으로 읽는다.
if cursor == nil or cursor ~= cursor or cursor == math.huge or cursor == -math.huge then
    waiting = redis.call('ZCARD', KEYS[1])
else
    waiting = redis.call('ZCOUNT', KEYS[1], '(' .. string.format('%.0f', cursor), '+inf')
end
local now = tonumber(redis.call('TIME')[1])
local alive = redis.call('ZCOUNT', KEYS[3], now, '+inf')
if alive > waiting then
    return alive
end
return waiting

-- 리더 해제. **자기 락만 지운다.**
--
-- KEYS[1]  scheduler:leader
-- ARGV[1]  ownerId
--
-- 반환  1 이면 지웠다. 0 이면 내 락이 아니었다
--
-- GET 과 DEL 을 나누면 **그 사이에 리스가 만료되고 다른 노드가 잡는다.**
-- 그 상태에서 DEL 하면 새 리더의 락이 사라져 배분이 멎는다. 확인과 삭제가
-- 한 스크립트 안에 있어야 하는 이유다.

-- 값의 형식은 `<펜스 번호>|<ownerId>` 다. **옛 형식(번호 없음)도 읽는다** —
-- 롤아웃 구간에 자기가 옛 형식으로 남긴 락을 못 알아보면 안 지우고 나간다.
local current = redis.call('GET', KEYS[1])
if not current then
    return 0
end
local sep = string.find(current, '|', 1, true)
local owner = sep == nil and current or string.sub(current, sep + 1)
if owner == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0

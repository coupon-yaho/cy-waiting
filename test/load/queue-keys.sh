#!/usr/bin/env bash
# 한 쿠폰의 줄 키 목록. **세 러너가 같은 목록을 쓴다.**
#
# 갈려 있던 것이 결함을 냈다 — 어떤 러너는 셋만 지워서 이탈 기록과 생존 신호와
# 배분 펜스가 앞 회차 값을 들고 넘어갔다. 재고는 시더가 관리하므로 안 건드린다.
# 키가 늘면 여기만 고친다.
#
#   사용: . test/load/queue-keys.sh   (source 한다)
#         redis-cli DEL $(queue_keys c1)

[ -n "${QUEUE_KEYS_LOADED:-}" ] && return 0
QUEUE_KEYS_LOADED=1

# 태그는 쿠폰 id 다. 샤딩이 열리면 `RedisKeys` 가 `tag(couponId, shards, shard)` 로 만들므로
# 그때 이 목록도 샤드를 받아야 한다. 지금은 샤드가 하나뿐이라 같다.
queue_keys() {
    local c=$1
    printf 'queue:{%s} admitted:{%s} maxscore:{%s} grace:{%s} alive:{%s} dropfence:{%s} applyfence:{%s}' \
        "$c" "$c" "$c" "$c" "$c" "$c" "$c"
}

#!/usr/bin/env bash
# 9.4.1·9.4.2 실측 — **한 대가 열화하면 유입이 몇 초 만에 주는가.** 그리고
# 열화를 걷으면 몇 초 만에 돌아오는가.
#
# 열화를 **지연이 아니라 낮춘 보고**로 넣는다. 가용량은 뒷단이 스스로 올리는
# 것이고 게이트웨이는 추측하지 않으므로, 지연만 넣으면 여유만 보는 전략은
# 아예 반응할 재료가 없다.
#
# 부하를 계속 흘리면서 뒷단 계수를 초마다 떠 **초당 도착**을 만든다. 누적으로
# 보면 열화 전 도착이 섞여 언제 줄었는지가 흐려진다.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1
# 여유 200/40/120. 큰 대를 낮춰 그 대의 몫이 55% 에서 11% 로 가는 것을 본다.
# **라이브러리를 읽기 전에 정한다** — 읽은 뒤에 정하면 무시된다.
BIG_CAP="${BIG_CAP:-200}"
SMALL_CAP="${SMALL_CAP:-40}"
MID_CAP="${MID_CAP:-120}"

. test/load/routing-lib.sh || exit 2

WARMUP_SEC="${WARMUP_SEC:-15}"
BEFORE_SEC="${BEFORE_SEC:-5}"
AFTER_SEC="${AFTER_SEC:-15}"

# 큰 대를 이 값으로 낮춘다. 200 → 20 이면 그 대의 몫이 55% 에서 11% 가 된다.
DEGRADED_CREDITS="${DEGRADED_CREDITS:-20}"

# 어느 방향을 재는가. `degrade` 는 열화를 넣고 유입이 주는지(9.4.1),
# `recover` 는 열화를 걷고 유입이 돌아오는지(9.4.2) 본다. **되돌아오는 쪽도
# 재야 한다** — 한 방향만 보면 영영 안 돌아오는 구현도 통과한다.
DIRECTION="${DIRECTION:-degrade}"
case "$DIRECTION" in
    degrade|recover) ;;
    *) echo "DIRECTION 은 degrade 또는 recover 여야 한다: '$DIRECTION'"; exit 2 ;;
esac

require_positive_int BEFORE_SEC AFTER_SEC DEGRADED_CREDITS || exit 2
require_non_negative_int WARMUP_SEC || exit 2

# **P2C 는 이 하네스로 못 잰다.** 아래에서 부하를 한 건씩 흘리므로 물린 건수가
# 늘 0 에 가깝고, 그러면 여유가 비교에서 통째로 빠져 열화를 넣어도 몫이 안
# 움직인다. 그 구간에서 재고 "P2C 는 안 줄었다" 고 적을 뻔했다 — 안 줄어든 것이
# 아니라 잴 수 없는 조건이었다.
case "$STRATEGY" in
    *p2c*)
        echo "판정 불가 — 전략 ${STRATEGY} 는 이 하네스로 못 잰다"
        echo "  부하를 한 건씩 흘려 물린 건수가 0 에 가깝다. 이 기준의 적용 범위 밖이다"
        exit 2 ;;
esac

work=$(mktemp -d) || exit 1
trap 'reap_children; rm -rf "$work"' EXIT

if [ "$DIRECTION" = degrade ]; then
  START_CREDITS=$BIG_CAP; END_CREDITS=$DEGRADED_CREDITS
else
  START_CREDITS=$DEGRADED_CREDITS; END_CREDITS=$BIG_CAP
fi
echo "$(banner) · 큰 대를 ${START_CREDITS} → ${END_CREDITS} 로 바꾼다"

bring_up "$work/up.log" || exit 2
wait_for_ramp || exit 2
sleep "$WARMUP_SEC"

# 회복을 재려면 회복 **전** 상태가 있어야 한다. 예열은 정상값으로 마치고,
# 표본을 뜨기 전에 열화 상태로 내려 앉힌다.
if [ "$DIRECTION" = recover ]; then
  $COMPOSE exec -T redis redis-cli SET sim:credits:stub-1 "$START_CREDITS" >/dev/null
  sleep 6
fi

wait_for_idle_queue || exit 2

# **부하를 끊지 않고 흘린다.** 한 건씩 보내야 쿠폰이 줄 모드로 안 켜진다.
( i=0; while :; do i=$(( i + 1 )); issue "$(( 3000000 + i ))" >/dev/null; done ) &

: > "$work/samples"
for name in "${NAMES[@]}"; do served "$name" || exit 2; done > "$work/prev"

# 표본을 초마다 뜬다. 바꾸는 순간이 0 초다.
# **라벨은 실제 경과 초다.** 순번을 그대로 쓰면 표본 하나를 뜨는 데 드는 시간
# (뒷단 셋에 각각 도커 명령)이 라벨에서 빠져, 예산 5초로 적힌 결과가 벽시계로는
# 그보다 한참 뒤가 된다 — 게이트가 요구하는 수가 조용히 늘어난다.
sample() {
  local second=$1 total=0 degraded=0 idx now prev delta
  for idx in 0 1 2; do
    now=$(served "${NAMES[idx]}") || return 1
    echo "$now" >> "$work/cur"
  done
  for idx in 0 1 2; do
    prev=$(sed -n "$((idx + 1))p" "$work/prev")
    now=$(sed -n "$((idx + 1))p" "$work/cur")
    delta=$(( now - prev ))
    [ "$idx" -eq 0 ] && degraded=$delta
    total=$(( total + delta ))
  done
  mv "$work/cur" "$work/prev"
  [ -z "${changed_at:-}" ] || second=$(( $(date +%s) - changed_at ))
  echo "$second $degraded $total" >> "$work/samples"
}

for s in $(seq -- "-$BEFORE_SEC" -1); do sleep 1; sample "$s" || exit 2; done
$COMPOSE exec -T redis redis-cli SET sim:credits:stub-1 "$END_CREDITS" >/dev/null
changed_at=$(date +%s)
for s in $(seq 0 "$AFTER_SEC"); do sleep 1; sample "$s" || exit 2; done

kill %1 2>/dev/null; wait %1 2>/dev/null

# 바꾼 뒤 큰 대의 목표 몫. 나머지 둘의 여유는 그대로다.
target=$(( END_CREDITS * 100 / (END_CREDITS + SMALL_CAP + MID_CAP) ))
echo
echo "바꾼 뒤 큰 대의 목표 몫: ${target}%"
echo
test/load/evaluate-redistribute.sh "$work/samples" "$target"

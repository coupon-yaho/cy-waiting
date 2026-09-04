#!/usr/bin/env bash
# **한 대만 즉시 실패하면 유입이 끊기는가, 그리고 되돌아올 때 안 튀는가.**
#
# 기존 시나리오에 없던 자극이다. 지금 있는 것은 지연과 낮춘 보고와 종료뿐인데,
# 셋 다 물린 건수가 쌓이거나 후보에서 아예 사라지는 고장이다. **즉시 실패는
# 반대로 움직인다** — 물린 건수가 안 쌓여 그 대가 가장 한가해 보이고, 부하율로
# 고르는 이상 트래픽을 오히려 끌어당긴다.
#
# 되돌아오는 쪽도 같이 잰다. 배제 동안 트래픽이 0 이었으니 풀리는 순간에도
# 그 대가 가장 한가해 보인다 — 그대로 두면 복귀가 절벽이고, 아직 아프면
# 곧바로 다시 빠져 배제 시간 주기의 사각파가 된다.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1
# 여유를 셋 다 같게 둔다. **몫의 차이가 아니라 배제만 보고 싶다.**
BIG_CAP="${BIG_CAP:-120}"
SMALL_CAP="${SMALL_CAP:-120}"
MID_CAP="${MID_CAP:-120}"

. test/load/routing-lib.sh || exit 2

WARMUP_SEC="${WARMUP_SEC:-15}"
BEFORE_SEC="${BEFORE_SEC:-5}"
FAULT_SEC="${FAULT_SEC:-20}"
AFTER_SEC="${AFTER_SEC:-20}"

# 고장 난 대가 낼 코드. 5xx 여야 배제기가 실패로 센다.
FAULT_STATUS="${FAULT_STATUS:-503}"

require_positive_int BEFORE_SEC FAULT_SEC AFTER_SEC FAULT_STATUS || exit 2
require_non_negative_int WARMUP_SEC || exit 2

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"; kill %1 2>/dev/null' EXIT

echo "$(banner) · 한 대를 즉시 ${FAULT_STATUS} 로 만든다"

bring_up "$work/up.log" || exit 2
wait_for_ramp || exit 2
sleep "$WARMUP_SEC"
wait_for_idle_queue || exit 2

# **부하를 끊지 않고 흘린다.** 한 건씩 보내야 쿠폰이 줄 모드로 안 켜진다.
( i=0; while :; do i=$(( i + 1 )); issue "$(( 5000000 + i ))" >/dev/null; done ) &

: > "$work/samples"
for name in "${NAMES[@]}"; do arrived "$name" || exit 2; done > "$work/prev"

# 고장 난 대에 닿은 수와 전체를 초마다 뜬다. **닿은 수**여야 한다 — 즉시 실패는
# 처리 건수에 안 들어가므로, 그것만 보면 트래픽을 다 받는 중에도 0 으로 보인다.
# **셋을 나란히 읽는다.** 차례로 읽으면 표본 하나에 도커 명령이 여섯 번 들어
# 2.8초가 걸리고, 그러면 진입 예산 5초가 판정기 해상도 아래로 내려간다 —
# 0.2초 만에 끊는 구현이 미달로 적힌다. 실제로 그렇게 한 번 적혔다.
sample() {
  local second=$1 total=0 broken=0 idx now prev delta
  for idx in 0 1 2; do
    arrived "${NAMES[idx]}" > "$work/n$idx" &
  done
  wait
  for idx in 0 1 2; do
    now=$(cat "$work/n$idx")
    case "$now" in ''|*[!0-9]*) echo "표본을 못 읽었다: ${NAMES[idx]}" >&2; return 1 ;; esac
    echo "$now" >> "$work/cur"
  done
  for idx in 0 1 2; do
    prev=$(sed -n "$((idx + 1))p" "$work/prev")
    now=$(sed -n "$((idx + 1))p" "$work/cur")
    delta=$(( now - prev ))
    [ "$idx" -eq 0 ] && broken=$delta
    total=$(( total + delta ))
  done
  mv "$work/cur" "$work/prev"
  [ -z "${marked_at:-}" ] || second=$(( $(date +%s) - marked_at ))
  echo "$second $broken $total" >> "$work/samples"
}

fault() {
  $COMPOSE exec -T "${NAMES[0]}" sh -c \
      "wget -qO- 'http://localhost:8090/stub/fault?status=$1'" >/dev/null
}

for s in $(seq -- "-$BEFORE_SEC" -1); do sleep 1; sample "$s" || exit 2; done
fault "$FAULT_STATUS" || exit 2
marked_at=$(date +%s)
for s in $(seq 0 "$FAULT_SEC"); do sleep 1; sample "$s" || exit 2; done

# 고장을 걷고 복귀를 잰다. **파일을 갈라 둔다** — 한 벌로 보면 진입과 복귀의
# 판정 기준이 섞이고, 그러면 어느 쪽이 미달인지 못 가른다.
mv "$work/samples" "$work/fault"
: > "$work/samples"
fault 0 || exit 2
marked_at=$(date +%s)
for s in $(seq 0 "$AFTER_SEC"); do sleep 1; sample "$s" || exit 2; done
mv "$work/samples" "$work/recover"

kill %1 2>/dev/null; wait %1 2>/dev/null

# 셋의 여유가 같으므로 한 대의 정상 몫은 3분의 1 이다.
echo
test/load/evaluate-eject.sh "$work/fault" "$work/recover" 33

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
# **램프보다 길게 본다.** 배제가 풀린 뒤 제 몫으로 돌아오는 데 램프(기본 60초)가
# 걸리는데, 20초만 보면 아직 오르는 중인 값을 "복귀 못 했다" 로 적는다 — 계기가
# 짧아서 나는 미달이다. 실제로 그렇게 한 번 났다.
AFTER_SEC="${AFTER_SEC:-80}"

# 고장 난 대가 낼 코드. 5xx 여야 배제기가 실패로 센다.
FAULT_STATUS="${FAULT_STATUS:-503}"

require_positive_int BEFORE_SEC FAULT_SEC AFTER_SEC FAULT_STATUS || exit 2
# **배제가 무는 코드여야 한다.** 아무 양수나 받으면 200 으로 부른 회차가 고장을
# 한 번도 안 낸 채 "유입이 안 끊겼다" 도 아니고 그냥 통과한다 — 배제를 재는
# 하네스가 배제를 안 밟고 초록이 되는 자리다. 게이트웨이가 그 대의 실패로 세는
# 것은 5xx 와 포화를 알리는 429 뿐이다.
case "$FAULT_STATUS" in
    429|5[0-9][0-9]) ;;
    *) echo "FAULT_STATUS 는 5xx 나 429 여야 한다 — 배제가 무는 코드다: $FAULT_STATUS"; exit 2 ;;
esac
require_non_negative_int WARMUP_SEC || exit 2

work=$(mktemp -d) || exit 1
trap 'reap_children; rm -rf "$work"' EXIT

echo "$(banner) · 한 대를 즉시 ${FAULT_STATUS} 로 만든다"

bring_up "$work/up.log" || exit 2

# **앞 실행이 남긴 고장을 걷는다.** 겹침은 이미지가 그대로면 컨테이너를 다시
# 안 만들므로, 앞 회차가 켜 둔 고장이 그대로 살아 있다. 그 상태로 시작하면
# 고장 전 몫이 처음부터 0 이라 아무것도 못 재는데, 그 사실은 판정기가
# "잴 것이 없다" 로 끊어 줄 때에야 드러난다 — 실제로 두 번 그랬다.
for name in "${NAMES[@]}"; do
  $COMPOSE exec -T "$name" sh -c \
      "wget -qO- 'http://localhost:8090/stub/fault?status=0'" >/dev/null || exit 2
done

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
  local pids=()
  for idx in 0 1 2; do
    arrived "${NAMES[idx]}" > "$work/n$idx" &
    pids+=($!)
  done
  # **셋만 기다린다.** 인자 없는 wait 는 부하 루프까지 기다리는데 그것은
  # 끝나지 않는다 — 표본 하나에서 통째로 멎는다. 실제로 15분을 멎었다.
  wait "${pids[@]}"
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

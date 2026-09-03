#!/usr/bin/env bash
# 9.4.1 실측 — **한 대가 열화하면 그 대의 유입이 몇 초 만에 주는가.**
#
# 열화를 **지연이 아니라 낮춘 보고**로 넣는다. 가용량은 뒷단이 스스로 올리는
# 것이고 게이트웨이는 추측하지 않으므로(`CapacityReport`), 지연만 넣으면 여유만
# 보는 전략은 아예 반응할 재료가 없다. 지연 쪽은 따로 잰다.
#
# 부하를 계속 흘리면서 뒷단 계수를 초마다 떠 **초당 도착**을 만든다. 누적으로
# 보면 열화 전 도착이 섞여 언제 줄었는지가 흐려진다.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1

COMPOSE="docker compose -f test/load/compose.yml -f test/load/compose.routing.yml"

STRATEGY="${STRATEGY:-round-robin}"
COUPON="${COUPON:-c1}"
GATEWAY="${GATEWAY:-http://localhost:18080}"
WARMUP_SEC="${WARMUP_SEC:-15}"
# 열화 전후로 표본을 뜨는 시간(초).
BEFORE_SEC="${BEFORE_SEC:-5}"
AFTER_SEC="${AFTER_SEC:-15}"
# 큰 대를 이 값으로 낮춘다. 200 → 20 이면 그 대의 몫이 55% 에서 11% 가 된다.
DEGRADED_CREDITS="${DEGRADED_CREDITS:-20}"

# 어느 방향을 재는가. `degrade` 는 열화를 넣고 유입이 주는지(9.4.1),
# `recover` 는 열화를 걷고 유입이 돌아오는지(9.4.2) 본다. **되돌리는 쪽도
# 재야 한다** — 한 방향만 보면 영영 안 돌아오는 구현도 통과한다.
DIRECTION="${DIRECTION:-degrade}"
case "$DIRECTION" in
    degrade|recover) ;;
    *) echo "DIRECTION 은 degrade 또는 recover 여야 한다: '$DIRECTION'"; exit 2 ;;
esac

NAMES=(backend backend-small backend-mid)
# **씨더가 쓰는 값과 같아야 한다.** 갈리면 기대값이 실제 보고와 달라져,
# 맞게 도착한 것이 미달로 적힌다.
CREDITS=("${BIG_CAP:-200}" "${SMALL_CAP:-40}" "${MID_CAP:-120}")

for setting in WARMUP_SEC BEFORE_SEC AFTER_SEC DEGRADED_CREDITS; do
    value=$(eval "printf '%s' \"\$$setting\"")
    case "$value" in
        ''|*[!0-9]*) echo "$setting 은 0 이상의 정수여야 한다: '$value'"; exit 2 ;;
    esac
done

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"; kill %1 2>/dev/null' EXIT

served() {
  local raw count
  raw=$($COMPOSE exec -T "$1" sh -c 'wget -qO- http://localhost:8090/stub/health')
  count=$(printf '%s' "$raw" | sed -n 's/.*"served":\([0-9][0-9]*\).*/\1/p')
  case "$count" in
      ''|*[!0-9]*) echo "[$1] 처리 건수를 못 읽었다: ${raw:-응답 없음}" >&2; return 1 ;;
  esac
  printf '%d\n' "$((10#$count))"
}

if [ "$DIRECTION" = degrade ]; then
  START_CREDITS=${CREDITS[0]}; END_CREDITS=$DEGRADED_CREDITS
else
  START_CREDITS=$DEGRADED_CREDITS; END_CREDITS=${CREDITS[0]}
fi
echo "전략 ${STRATEGY} · 큰 대를 ${START_CREDITS} → ${END_CREDITS} 로 바꾼다"

# **여유를 먼저 되돌리고 기다린다.** 앞 실행이 낮춰 둔 값은 볼륨에 남는다.
# 그 상태로 기다리면 예열이 요구하는 크레딧에 영영 못 닿아 기동이 실패한다 —
# 열화 전 표본이 이미 열화 상태인 것보다 먼저 걸리는 문제다.
if ! $COMPOSE up -d redis > "$work/up.log" 2>&1; then
  echo "레디스를 못 세웠다"; tail -20 "$work/up.log" | sed 's/^/  /'; exit 2
fi
for _ in $(seq 1 30); do
  $COMPOSE exec -T redis redis-cli SET sim:credits:stub-1 "${CREDITS[0]}" >/dev/null 2>&1 && break
  sleep 1
done
$COMPOSE exec -T redis redis-cli SET sim:credits:stub-2 "${CREDITS[1]}" >/dev/null 2>&1
$COMPOSE exec -T redis redis-cli SET sim:credits:stub-3 "${CREDITS[2]}" >/dev/null 2>&1

if ! ROUTING_STRATEGY="$STRATEGY" $COMPOSE up -d --wait > "$work/up.log" 2>&1; then
  echo "겹침을 못 세웠다"; tail -20 "$work/up.log" | sed 's/^/  /'; exit 2
fi
sleep "$WARMUP_SEC"

# 회복을 재려면 회복 **전** 상태가 있어야 한다. 예열은 정상값으로 마치고,
# 표본을 뜨기 전에 열화 상태로 내려 앉힌다.
if [ "$DIRECTION" = recover ]; then
  $COMPOSE exec -T redis redis-cli SET sim:credits:stub-1 "$START_CREDITS" >/dev/null
  sleep 6
fi

$COMPOSE exec -T redis redis-cli DEL "queue:{$COUPON}" >/dev/null 2>&1
for _ in $(seq 1 30); do
  state=$($COMPOSE exec -T redis redis-cli HGET gw:snapshot "$COUPON" 2>/dev/null)
  case "$state" in *QUEUEING*) sleep 1 ;; *) break ;; esac
done
case "${state:-}" in
  *QUEUEING*) echo "줄 모드가 안 꺼진다 ($state) — 이 상태로는 분배를 못 잰다"; exit 2 ;;
esac

# **부하를 끊지 않고 흘린다.** 한 건씩 보내야 쿠폰이 줄 모드로 안 켜진다.
# 초당 도착을 봐야 하므로 표본 뜨는 동안 계속 흘러야 한다.
(
  i=0
  while :; do
    i=$(( i + 1 ))
    curl -s -o /dev/null --max-time 5 -X POST \
      "$GATEWAY/api/v1/coupons/$COUPON/issue" \
      -H "X-Member-Id: $(( 3000000 + i ))" \
      -H "X-Member-Grade: GOLD" \
      -H "X-Forwarded-For: 10.13.$(( i / 250 % 250 + 1 )).$(( i % 250 + 1 ))"
  done
) &

# 표본을 초마다 뜬다. 열화를 넣는 순간이 0 초다.
sample() {
  local second=$1 total=0 degraded=0 idx now
  for idx in 0 1 2; do
    now=$(served "${NAMES[idx]}") || return 1
    echo "$now" >> "$work/cur"
  done
  local prev
  for idx in 0 1 2; do
    prev=$(sed -n "$((idx + 1))p" "$work/prev" 2>/dev/null)
    now=$(sed -n "$((idx + 1))p" "$work/cur")
    local delta=$(( now - ${prev:-now} ))
    [ "$idx" -eq 0 ] && degraded=$delta
    total=$(( total + delta ))
  done
  mv "$work/cur" "$work/prev"
  echo "$second $degraded $total" >> "$work/samples"
}

: > "$work/samples"
for idx in 0 1 2; do served "${NAMES[idx]}" || exit 2; done > "$work/prev"

for s in $(seq -- "-$BEFORE_SEC" -1); do sleep 1; sample "$s" || exit 2; done

$COMPOSE exec -T redis redis-cli SET sim:credits:stub-1 "$END_CREDITS" >/dev/null
for s in $(seq 0 "$AFTER_SEC"); do sleep 1; sample "$s" || exit 2; done

kill %1 2>/dev/null; wait %1 2>/dev/null

# 바꾼 뒤 큰 대의 목표 몫. 나머지 둘의 여유는 그대로다.
after_sum=$(( END_CREDITS + CREDITS[1] + CREDITS[2] ))
target=$(( END_CREDITS * 100 / after_sum ))
echo
echo "바꾼 뒤 큰 대의 목표 몫: ${target}%"
echo
test/load/evaluate-redistribute.sh "$work/samples" "$target"

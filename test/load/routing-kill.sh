#!/usr/bin/env bash
# 9.4.3 실측 — **한 대를 죽이면 사용자에게 오류가 새는가.**
#
# 여기서 재는 것은 비율이 아니라 **응답 코드**다. 죽은 대로 간 요청이 그대로
# 5xx 가 되면 사용자가 그 장애를 본다. 나머지로 다시 보내면 안 본다.
#
# **보고를 멈추는 것으로는 못 잰다.** 그러면 신선도가 지난 뒤 후보에서 조용히
# 빠지고, 그건 정상 이탈이라 재시도 경로를 아예 안 지난다. 컨테이너를 죽이고
# 보고는 계속 올리게 둬야 게이트웨이가 살아 있다고 믿고 보내고, 그때 연결
# 실패에 무는 재시도가 실제로 걸린다.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1

COMPOSE="docker compose -f test/load/compose.yml -f test/load/compose.routing.yml"

STRATEGY="${STRATEGY:-round-robin}"
COUPON="${COUPON:-c1}"
GATEWAY="${GATEWAY:-http://localhost:18080}"
WARMUP_SEC="${WARMUP_SEC:-15}"
# 죽일 대. 가장 여유 있는 대를 죽여야 유출이 있으면 드러난다.
VICTIM="${VICTIM:-backend}"
# 죽인 뒤 이만큼 보낸다(초). 신선도(3초)가 지나 후보에서 빠질 때까지가
# 위험 구간이라, 그보다 넉넉히 잡는다.
AFTER_SEC="${AFTER_SEC:-12}"
# 허용 5xx. **0 이다.** 하나라도 새면 사용자가 장애를 본 것이다.
MAX_5XX="${MAX_5XX:-0}"

for setting in WARMUP_SEC AFTER_SEC MAX_5XX; do
    value=$(eval "printf '%s' \"\$$setting\"")
    case "$value" in
        ''|*[!0-9]*) echo "$setting 은 0 이상의 정수여야 한다: '$value'"; exit 2 ;;
    esac
done

work=$(mktemp -d) || exit 1
# 죽인 대를 반드시 되살린다. 안 살리면 다음 실행이 두 대로 시작한다.
trap '$COMPOSE start "$VICTIM" >/dev/null 2>&1; rm -rf "$work"' EXIT

echo "전략 ${STRATEGY} · ${VICTIM} 을 죽이고 ${AFTER_SEC}초 동안 응답을 센다"

# 앞 실행이 낮춰 뒀을 수 있어 여유를 먼저 되돌린다. 예열이 크레딧을 요구한다.
$COMPOSE up -d redis > "$work/up.log" 2>&1
for _ in $(seq 1 30); do
  $COMPOSE exec -T redis redis-cli SET sim:credits:stub-1 200 >/dev/null 2>&1 && break
  sleep 1
done
if ! ROUTING_STRATEGY="$STRATEGY" $COMPOSE up -d --wait >> "$work/up.log" 2>&1; then
  echo "겹침을 못 세웠다"; tail -20 "$work/up.log" | sed 's/^/  /'; exit 2
fi
sleep "$WARMUP_SEC"

$COMPOSE exec -T redis redis-cli DEL "queue:{$COUPON}" >/dev/null 2>&1
for _ in $(seq 1 30); do
  state=$($COMPOSE exec -T redis redis-cli HGET gw:snapshot "$COUPON" 2>/dev/null)
  case "$state" in *QUEUEING*) sleep 1 ;; *) break ;; esac
done
case "${state:-}" in
  *QUEUEING*) echo "줄 모드가 안 꺼진다 ($state) — 이 상태로는 못 잰다"; exit 2 ;;
esac

# 한 건씩 보내며 코드를 적는다. 죽이기 전과 후를 파일로 나눈다.
drive() {
  local out=$1 seconds=$2 base=$3 i=0 deadline
  deadline=$(( $(date +%s) + seconds ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    i=$(( i + 1 ))
    curl -s -o /dev/null --max-time 5 -w '%{http_code}\n' -X POST \
      "$GATEWAY/api/v1/coupons/$COUPON/issue" \
      -H "X-Member-Id: $(( base + i ))" \
      -H "X-Member-Grade: GOLD" \
      -H "X-Forwarded-For: 10.15.$(( i / 250 % 250 + 1 )).$(( i % 250 + 1 ))"
  done > "$out"
}

drive "$work/before" 4 5000000
$COMPOSE kill "$VICTIM" >/dev/null 2>&1 || { echo "$VICTIM 을 못 죽였다"; exit 2; }
drive "$work/after" "$AFTER_SEC" 6000000

echo
for phase in before after; do
  label=$([ "$phase" = before ] && echo "죽이기 전" || echo "죽인 뒤")
  echo "  $label: $(sort "$work/$phase" | uniq -c | tr -s ' \n' ' ')"
done

MAX_5XX="$MAX_5XX" test/load/evaluate-kill.sh "$work/after"

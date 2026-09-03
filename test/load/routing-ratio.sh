#!/usr/bin/env bash
# 9.4.5 실측 — **고르개가 정한 비율이 실제 유입으로 나오는가.**
#
# 여유가 다른 뒷단 셋(200/40/120)을 세우고 같은 부하를 넣은 뒤, 각 대에
# **실제로 도착한 건수**를 센다. 기대는 여유 비율 그대로다. 고르개 단위
# 시험은 고른 결과만 보므로, 그 사이에 있는 배선·램프·상한·재시도가 비율을
# 어떻게 바꾸는지는 여기서만 드러난다.
#
# **도착은 뒷단이 센 값을 쓴다.** 게이트웨이 쪽 지표를 쓰면 고르개가 고른
# 것을 다시 읽는 셈이라 배선이 틀려도 맞게 나온다.
#
# **재시작하지 않고 차분으로 센다.** 부하 직전에 뒷단을 재시작하면 그 순간의
# 실패로 램프가 내려앉고, 회복하기 전에 부하가 들어가 표본이 몇 건만 남는다.
# 실제로 그렇게 돌아서 600 건 중 25 건만 통과한 적이 있다.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1

COMPOSE="docker compose -f test/load/compose.yml -f test/load/compose.routing.yml"

STRATEGY="${STRATEGY:-round-robin}"
REQUESTS="${REQUESTS:-600}"
# **한 건씩 보낸다.** 여럿을 한꺼번에 보내면 순간 유입이 임계를 넘어 쿠폰이
# 줄 모드로 켜지고, 그때부터 요청은 뒷단으로 안 가고 줄로 간다 — 200 대신
# 202 가 오고 도착이 0 이 된다. 실제로 열 건씩 보내 600 건이 전부 그렇게
# 끊겼다. 여기서 재려는 것은 판정이 아니라 **고르개의 분배**다.
CONCURRENCY="${CONCURRENCY:-1}"

# 묶음 사이에 쉬는 시간(초). 동시성을 올려 볼 때 유입 속도를 낮추는 손잡이다.
PACE_SEC="${PACE_SEC:-0}"
COUPON="${COUPON:-c1}"
GATEWAY="${GATEWAY:-http://localhost:18080}"

# 가장 여유 있는 대에 넣는 지연. 열화한 대의 유입이 실제로 주는지를 볼 때
# 쓴다 (9.4.1). 작은 대를 느리게 하면 원래 적게 가던 것이 더 적게 갈 뿐이라
# 무엇이 원인인지 못 가른다.
BIG_LATENCY_MS="${BIG_LATENCY_MS:-5}"

# 허용 편차(%). 비워 두면 판정기의 기본값(게이트와 같은 ±15%)이 선다 —
# 여기에 숫자를 또 적으면 게이트와 갈라질 자리가 하나 더 생긴다.
MAX_DEVIATION="${MAX_DEVIATION:-}"

# 램프가 오른 뒤 한 번 더 두는 여유(초). 기다리는 일 자체는 겹침의 warmup
# 서비스가 한다 — 크레딧이 목표에 닿을 때까지 healthcheck 가 붙들고, `--wait`
# 가 그것을 본다. 여기서는 배분이 한두 틱 더 돌 틈만 준다.
WARMUP_SEC="${WARMUP_SEC:-15}"

# 이름과 보고한 여유. seeder 가 쓰는 값과 같아야 한다.
NAMES=(backend backend-small backend-mid)
CREDITS=(200 40 120)

# **손잡이부터 본다.** 동시성이 0 이면 나머지 연산이 0 으로 나누고, 그 오류는
# 부하 루프 한가운데서 터져 절반쯤 보낸 상태로 끝난다.
for setting in REQUESTS CONCURRENCY; do
    value=$(eval "printf '%s' \"\$$setting\"")
    case "$value" in
        ''|*[!0-9]*) echo "$setting 은 양의 정수여야 한다: '$value'"; exit 2 ;;
    esac
    [ "$((10#$value))" -gt 0 ] || { echo "$setting 은 0 보다 커야 한다"; exit 2; }
done

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

# 뒷단이 누적으로 센 처리 건수를 읽는다.
#
# **못 읽으면 거기서 멈춘다.** 오류를 삼키고 빈 값을 돌려주면 그 값이 산술로
# 흘러 들어가 엉뚱한 도착 수가 나오고, 그 수로 게이트를 적게 된다. 실제로
# 컨테이너가 뜨는 중에 빈 값을 받아 "도착 0" 을 기록한 적이 있다.
served() {
  local raw count
  raw=$($COMPOSE exec -T "$1" sh -c 'wget -qO- http://localhost:8090/stub/health')
  count=$(printf '%s' "$raw" | sed -n 's/.*"served":\([0-9][0-9]*\).*/\1/p')
  case "$count" in
      ''|*[!0-9]*) echo "[$1] 처리 건수를 못 읽었다: ${raw:-응답 없음}" >&2; return 1 ;;
  esac
  # 앞자리 0 은 셸 산술이 8진수로 읽는다. 십진임을 명시한다.
  printf '%d\n' "$((10#$count))"
}

echo "전략 ${STRATEGY} · 큰 대 지연 ${BIG_LATENCY_MS}ms · 부하 ${REQUESTS} 건"

# **실패하면 왜인지 보여준다.** 통째로 버리면 "겹침을 못 세웠다" 한 줄만 남고,
# 그 한 줄로는 다시 세워 보는 것 말고 할 수 있는 일이 없다.
if ! BIG_LATENCY_MS="$BIG_LATENCY_MS" ROUTING_STRATEGY="$STRATEGY" \
     $COMPOSE up -d --wait > "$work/up.log" 2>&1; then
  echo "겹침을 못 세웠다"
  tail -20 "$work/up.log" | sed 's/^/  /'
  exit 1
fi

sleep "$WARMUP_SEC"

# **줄을 비우고, 줄 모드가 꺼진 것을 보고 시작한다.** 앞 회차가 남긴 줄이
# 있으면 이번 부하는 전부 대기로 돌아간다. 지우자마자 보내면 게이트웨이가
# 아직 옛 스냅샷을 들고 있어 같은 일이 난다.
$COMPOSE exec -T redis redis-cli DEL "queue:{$COUPON}" >/dev/null 2>&1
for _ in $(seq 1 30); do
  state=$($COMPOSE exec -T redis redis-cli HGET gw:snapshot "$COUPON" 2>/dev/null)
  case "$state" in *QUEUEING*) sleep 1 ;; *) break ;; esac
done
case "${state:-}" in
  *QUEUEING*) echo "줄 모드가 안 꺼진다 ($state) — 이 상태로는 분배를 못 잰다"; exit 2 ;;
esac

for name in "${NAMES[@]}"; do served "$name" || exit 2; done > "$work/before"

# 회원 번호가 겹치면 같은 사람의 재요청으로 걸러진다. 매 실행마다 다른 구간을
# 쓴다. 앞자리가 0 이면 형식 검증에서 400 이 난다.
base=$(( 1000000 + (RANDOM % 8000) * 1000 ))
for i in $(seq 1 "$REQUESTS"); do
  # **주소도 흩는다.** 한 주소로 몰아 보내면 주소별 한도에 걸려 429 가 나고,
  # 그 요청은 뒷단에 닿지 않아 비율 표본에서 통째로 빠진다. 동시에 보내는
  # 이상 한 주소로는 못 잰다.
  curl -s -o /dev/null -w '%{http_code}\n' -X POST \
    "$GATEWAY/api/v1/coupons/$COUPON/issue" \
    -H "X-Member-Id: $((base + i))" \
    -H "X-Member-Grade: GOLD" \
    -H "X-Forwarded-For: 10.12.$(( i / 250 + 1 )).$(( i % 250 + 1 ))" &
  if [ $(( i % CONCURRENCY )) -eq 0 ]; then
    wait
    [ "$PACE_SEC" = 0 ] || sleep "$PACE_SEC"
  fi
done > "$work/codes"
wait

for name in "${NAMES[@]}"; do served "$name" || exit 2; done > "$work/after"

total=0
specs=()
for idx in 0 1 2; do
  b=$(sed -n "$((idx + 1))p" "$work/before")
  a=$(sed -n "$((idx + 1))p" "$work/after")
  arrived=$(( a - b ))
  total=$(( total + arrived ))
  specs+=("${NAMES[idx]}:${CREDITS[idx]}:$arrived")
done

echo
echo "응답 코드: $(sort "$work/codes" | uniq -c | tr -s ' \n' ' ')"
echo "도착 합계: $total (부하 $REQUESTS)"
echo

# **다 통과하지 못했으면 비율을 논하지 않는다.** 일부만 닿아도 그 일부의
# 비율은 맞을 수 있어서, 못 잰 실행이 충족으로 적힌다. 몇 건이 어떤 코드로
# 끊겼는지는 위에 이미 찍혀 있으니 여기서는 판정만 막는다.
bad=$(grep -cv '^200$' "$work/codes")
if [ "$bad" -ne 0 ]; then
  echo "판정 불가 — $REQUESTS 건 중 $bad 건이 200 이 아니다. 이 실행으로는 분배를 못 잰다"
  exit 2
fi

MAX_DEVIATION="$MAX_DEVIATION" EXPECTED_TOTAL="$REQUESTS" \
  test/load/evaluate-routing-ratio.sh "${specs[@]}"

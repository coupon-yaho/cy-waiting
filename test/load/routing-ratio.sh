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
# **동시성이 판정의 일부다.** 한 건씩 보내면 물린 건수가 늘 0 이고, 그러면
# 여유 대비 부하로 고르는 전략은 비교할 것이 없어 균등 무작위가 된다 —
# 설계가 리틀의 법칙을 근거로 드는 것이 정확히 이 지점이다. 그 조건에서 잰
# 값으로 "여유를 안 본다" 고 적으면 안 되는 것을 잰 것이다.
#
# 반대로 유입이 한 틱 몫(여유 합계)을 넘으면 쿠폰이 줄 모드로 켜져 요청이
# 뒷단에 아예 안 닿는다. **느린 뒷단 + 낮은 동시성**이 둘 다 피하는 자리다 —
# 물린 건수 = 유입 × 지연이므로, 지연을 올리면 유입을 안 올리고도 물린다.
CONCURRENCY="${CONCURRENCY:-1}"

# 묶음 사이에 쉬는 시간(초). 동시성을 올려 볼 때 유입 속도를 낮추는 손잡이다.
PACE_SEC="${PACE_SEC:-0}"
COUPON="${COUPON:-c1}"
GATEWAY="${GATEWAY:-http://localhost:18080}"

# 뒷단 **셋 다**에 넣는 지연(ms). 비율을 재는 자리에서는 한 대만 느리게 하면
# 안 된다 — 빠른 대가 물린 건수를 안 쌓아 늘 뽑히고, 그러면 고르개가 아니라
# 지연 차이를 재게 된다. 열화 주입은 `routing-redistribute.sh` 가 따로 한다.
#
# 물린 건수 = 유입 × 지연이다. 지연을 올리면 유입을 안 올리고도 물린다.
STUB_LATENCY_MS="${STUB_LATENCY_MS:-5}"

# 허용 편차(%). 비워 두면 판정기의 기본값(게이트와 같은 ±15%)이 선다 —
# 여기에 숫자를 또 적으면 게이트와 갈라질 자리가 하나 더 생긴다.
MAX_DEVIATION="${MAX_DEVIATION:-}"

# 램프가 오른 뒤 한 번 더 두는 여유(초). 기다리는 일 자체는 겹침의 warmup
# 서비스가 한다 — 크레딧이 목표에 닿을 때까지 healthcheck 가 붙들고, `--wait`
# 가 그것을 본다. 여기서는 배분이 한두 틱 더 돌 틈만 준다.
WARMUP_SEC="${WARMUP_SEC:-15}"

# 이름과 보고한 여유. seeder 가 쓰는 값과 같아야 한다.
NAMES=(backend backend-small backend-mid)
# **씨더가 쓰는 값과 같아야 한다.** 갈리면 기대값이 실제 보고와 달라져,
# 맞게 도착한 것이 미달로 적힌다.
CREDITS=("${BIG_CAP:-200}" "${SMALL_CAP:-40}" "${MID_CAP:-120}")

# **손잡이부터 본다.** 동시성이 0 이면 나머지 연산이 0 으로 나누고, 그 오류는
# 부하 루프 한가운데서 터져 절반쯤 보낸 상태로 끝난다.
for setting in REQUESTS CONCURRENCY STUB_LATENCY_MS; do
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

echo "전략 ${STRATEGY} · 뒷단 지연 ${STUB_LATENCY_MS}ms · 동시 ${CONCURRENCY} · 부하 ${REQUESTS} 건"

# **실패하면 왜인지 보여준다.** 통째로 버리면 "겹침을 못 세웠다" 한 줄만 남고,
# 그 한 줄로는 다시 세워 보는 것 말고 할 수 있는 일이 없다.

# **이미지가 JAR 보다 낡았으면 다시 짓는다.** compose 는 JAR 이 바뀌어도 이미
# 있는 이미지를 그대로 쓴다. 그러면 고친 코드가 아니라 **옛 바이너리를 재고**
# 그 값을 실측으로 적게 된다 — 실제로 "고쳐도 값이 그대로다" 를 한 번 겪었다.
jar=${WAITING_JAR:-build/libs/waiting.jar}
if [ ! -f "$jar" ]; then
  echo "실행 JAR 이 없다: $jar — ./gradlew build 를 먼저 돌린다"; exit 2
fi
$COMPOSE build gateway > "$work/build.log" 2>&1 || {
  echo "게이트웨이 이미지를 못 지었다"; tail -20 "$work/build.log" | sed 's/^/  /'; exit 2
}
if ! STUB_LATENCY_MS="$STUB_LATENCY_MS" ROUTING_STRATEGY="$STRATEGY" \
     BIG_CAP="${CREDITS[0]}" SMALL_CAP="${CREDITS[1]}" MID_CAP="${CREDITS[2]}" \
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

# **일꾼을 따로 돌린다.** 묶음마다 다 끝나기를 기다리면 그 경계에서 물린
# 건수가 0 으로 떨어지고, 그 순간의 결정은 여유를 못 본다 — 재려던 조건이
# 회차마다 무너진다. 일꾼 각자가 제 차례를 이어 보내야 물린 건수가 동시성
# 근처에서 유지된다.
#
# **주소도 흩는다.** 한 주소로 몰아 보내면 주소별 한도에 걸려 429 가 나고,
# 그 요청은 뒷단에 닿지 않아 표본에서 통째로 빠진다.
worker() {
  local slot=$1 n=$2 k i
  for k in $(seq 1 "$n"); do
    i=$(( (k - 1) * CONCURRENCY + slot ))
    curl -s -o /dev/null -w '%{http_code}\n' -X POST \
      "$GATEWAY/api/v1/coupons/$COUPON/issue" \
      -H "X-Member-Id: $((base + i))" \
      -H "X-Member-Grade: GOLD" \
      -H "X-Forwarded-For: 10.12.$(( i / 250 % 250 + 1 )).$(( i % 250 + 1 ))"
    [ "$PACE_SEC" = 0 ] || sleep "$PACE_SEC"
  done
}

per_worker=$(( REQUESTS / CONCURRENCY ))
[ "$per_worker" -lt 1 ] && per_worker=1
for slot in $(seq 1 "$CONCURRENCY"); do
  worker "$slot" "$per_worker" &
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
echo "도착 합계: $total (보낸 것 $(grep -c '' "$work/codes"))"
echo

# **다 통과하지 못했으면 비율을 논하지 않는다.** 일부만 닿아도 그 일부의
# 비율은 맞을 수 있어서, 못 잰 실행이 충족으로 적힌다. 몇 건이 어떤 코드로
# 끊겼는지는 위에 이미 찍혀 있으니 여기서는 판정만 막는다.
bad=$(grep -cv '^200$' "$work/codes")
if [ "$bad" -ne 0 ]; then
  echo "판정 불가 — 보낸 것 중 $bad 건이 200 이 아니다. 이 실행으로는 분배를 못 잰다"
  exit 2
fi

# **보낸 것과 도착한 것을 견준다.** 일꾼으로 나누면 나머지가 생겨 보낸 수가
# 요청 수와 다를 수 있다. 요청 수로 견주면 멀쩡한 실행이 판정 불가가 된다.
sent=$(grep -c '' "$work/codes")
MAX_DEVIATION="$MAX_DEVIATION" EXPECTED_TOTAL="$sent" \
  test/load/evaluate-routing-ratio.sh "${specs[@]}"

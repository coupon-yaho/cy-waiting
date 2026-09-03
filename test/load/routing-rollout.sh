#!/usr/bin/env bash
# 9.4.4 실측 — **새로 뜬 인스턴스로 처음부터 몰리지는 않는가.**
#
# 롤링 배포에서 새 인스턴스는 차갑다. 캐시도 커넥션 풀도 비어 있어서, 제 몫을
# 처음부터 받으면 그 대만 무너진다. 램프가 그것을 막는다.
#
# **식별자를 바꾸는 것이 배포다.** 재기동한 인스턴스는 새 식별자로 온다는 것이
# 계약이고(R-3), 램프는 처음 본 식별자에만 걸린다. 컨테이너를 재시작해도
# 씨더가 같은 이름으로 보고하면 램프를 안 타서 아무것도 안 잰다.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1

COMPOSE="docker compose -f test/load/compose.yml -f test/load/compose.routing.yml"

STRATEGY="${STRATEGY:-round-robin}"
COUPON="${COUPON:-c1}"
GATEWAY="${GATEWAY:-http://localhost:18080}"
WORKERS="${WORKERS:-12}"
BEFORE_SEC="${BEFORE_SEC:-4}"
AFTER_SEC="${AFTER_SEC:-20}"
BIG_CAP="${BIG_CAP:-2000}"
SMALL_CAP="${SMALL_CAP:-400}"
MID_CAP="${MID_CAP:-1200}"
# 새로 뜬 것처럼 보이게 할 식별자. 앞 실행과 겹치면 램프를 안 탄다.
FRESH_ID="${FRESH_ID:-stub-1-$$}"

for setting in WORKERS BEFORE_SEC AFTER_SEC BIG_CAP SMALL_CAP MID_CAP; do
    value=$(eval "printf '%s' \"\$$setting\"")
    case "$value" in
        ''|*[!0-9]*) echo "$setting 은 0 이상의 정수여야 한다: '$value'"; exit 2 ;;
    esac
done

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"; kill %1 2>/dev/null' EXIT

echo "전략 ${STRATEGY} · 큰 대를 ${FRESH_ID} 로 갈아 끼운다"

$COMPOSE up -d redis > "$work/up.log" 2>&1
for _ in $(seq 1 30); do
  $COMPOSE exec -T redis redis-cli SET sim:credits:stub-1 "$BIG_CAP" >/dev/null 2>&1 && break
  sleep 1
done
# 앞 실행이 바꿔 둔 이름으로 시작하면 이번 갈아 끼우기가 램프를 안 탄다.
$COMPOSE exec -T redis redis-cli SET sim:id:stub-1 stub-1 >/dev/null 2>&1


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
if ! ROUTING_STRATEGY="$STRATEGY" BIG_CAP="$BIG_CAP" SMALL_CAP="$SMALL_CAP" \
     MID_CAP="$MID_CAP" $COMPOSE up -d --wait >> "$work/up.log" 2>&1; then
  echo "겹침을 못 세웠다"; tail -20 "$work/up.log" | sed 's/^/  /'; exit 2
fi

# **램프가 다 오를 때까지 기다린다.** 안 기다리면 유입이 그때의 몫을 넘어
# 줄이 켜지고, 요청이 뒷단에 안 닿는다.
target=$(( (BIG_CAP + SMALL_CAP + MID_CAP) * 9 / 10 ))
for _ in $(seq 1 120); do
  credit=$($COMPOSE exec -T redis redis-cli HGET gw:snapshot '#credit' 2>/dev/null)
  case "$credit" in ''|*[!0-9]*) sleep 1; continue ;; esac
  [ "$credit" -ge "$target" ] && break
  sleep 1
done
if [ "${credit:-0}" -lt "$target" ]; then
  echo "크레딧이 ${credit:-0} 에서 안 오른다 (목표 $target) — 이 상태로는 못 잰다"; exit 2
fi

$COMPOSE exec -T redis redis-cli DEL "queue:{$COUPON}" >/dev/null 2>&1
for _ in $(seq 1 30); do
  state=$($COMPOSE exec -T redis redis-cli HGET gw:snapshot "$COUPON" 2>/dev/null)
  case "$state" in *QUEUEING*) sleep 1 ;; *) break ;; esac
done
case "${state:-}" in
  *QUEUEING*) echo "줄 모드가 안 꺼진다 ($state) — 이 상태로는 못 잰다"; exit 2 ;;
esac

served() {
  local raw count
  raw=$($COMPOSE exec -T "$1" sh -c 'wget -qO- http://localhost:8090/stub/health')
  count=$(printf '%s' "$raw" | sed -n 's/.*"served":\([0-9][0-9]*\).*/\1/p')
  case "$count" in
      ''|*[!0-9]*) echo "[$1] 처리 건수를 못 읽었다: ${raw:-응답 없음}" >&2; return 1 ;;
  esac
  printf '%d\n' "$((10#$count))"
}

NAMES=(backend backend-small backend-mid)

# 부하를 끊지 않고 흘린다.
(
  i=0
  while :; do
    i=$(( i + 1 ))
    curl -s -o /dev/null --max-time 5 -X POST \
      "$GATEWAY/api/v1/coupons/$COUPON/issue" \
      -H "X-Member-Id: $(( 7000000 + i ))" \
      -H "X-Member-Grade: GOLD" \
      -H "X-Forwarded-For: 10.16.$(( i / 250 % 250 + 1 )).$(( i % 250 + 1 ))"
  done
) &

: > "$work/samples"
for name in "${NAMES[@]}"; do served "$name" || exit 2; done > "$work/prev"

sample() {
  local second=$1 total=0 fresh=0 idx now prev delta
  for idx in 0 1 2; do
    now=$(served "${NAMES[idx]}") || return 1
    echo "$now" >> "$work/cur"
  done
  for idx in 0 1 2; do
    prev=$(sed -n "$((idx + 1))p" "$work/prev")
    now=$(sed -n "$((idx + 1))p" "$work/cur")
    delta=$(( now - prev ))
    [ "$idx" -eq 0 ] && fresh=$delta
    total=$(( total + delta ))
  done
  mv "$work/cur" "$work/prev"
  echo "$second $fresh $total" >> "$work/samples"
}

for s in $(seq -- "-$BEFORE_SEC" -1); do sleep 1; sample "$s" || exit 2; done

# **갈아 끼운다.** 주소는 그대로고 식별자만 바뀐다 — 같은 대가 새로 뜬 모양이다.
$COMPOSE exec -T redis redis-cli SET sim:id:stub-1 "$FRESH_ID" >/dev/null
for s in $(seq 0 "$AFTER_SEC"); do sleep 1; sample "$s" || exit 2; done

kill %1 2>/dev/null; wait %1 2>/dev/null

steady=$(( BIG_CAP * 100 / (BIG_CAP + SMALL_CAP + MID_CAP) ))
echo
echo "큰 대의 평상시 몫: ${steady}%"
echo
test/load/evaluate-rollout.sh "$work/samples" "$steady"

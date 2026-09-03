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
# 허용 유출. **0 이다.** 하나라도 새면 사용자가 장애를 본 것이다.
MAX_5XX="${MAX_5XX:-0}"

# 동시에 보내는 일꾼 수. 죽은 대로 간 요청은 시간 제한까지 매달리므로,
# 하나로는 창 안에 표본이 안 쌓인다.
WORKERS="${WORKERS:-12}"

# **여유를 크게 잡는다.** 작게 두면 일꾼 열둘이 보내는 속도가 한 틱 몫을 넘겨
# 쿠폰이 줄 모드로 켜지고, 그때부터 요청은 뒷단에 안 닿는다 — 죽은 대로 가는
# 경로를 한 번도 안 밟은 실행이 "유출 0" 으로 나온다. 실제로 그랬다.
BIG_CAP="${BIG_CAP:-2000}"
SMALL_CAP="${SMALL_CAP:-400}"
MID_CAP="${MID_CAP:-1200}"

for setting in WARMUP_SEC AFTER_SEC MAX_5XX WORKERS BIG_CAP SMALL_CAP MID_CAP; do
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
  $COMPOSE exec -T redis redis-cli SET sim:credits:stub-1 "$BIG_CAP" >/dev/null 2>&1 && break
  sleep 1
done

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
# **램프가 다 오를 때까지 기다린다.** 겹침의 warmup 은 크레딧이 하한을 넘기만
# 하면 통과시키는데, 램프는 60초짜리라 그 시점의 크레딧은 목표의 몇 분의 일이다.
# 그 상태로 일꾼 열둘이 보내면 유입이 그때의 몫을 넘어 줄이 켜지고, 요청이
# 뒷단에 안 닿는다 — 죽은 대로 가는 경로를 못 밟는다.
target=$(( (BIG_CAP + SMALL_CAP + MID_CAP) * 9 / 10 ))
for _ in $(seq 1 90); do
  now=$($COMPOSE exec -T redis redis-cli HGET gw:snapshot '#credit' 2>/dev/null)
  case "$now" in ''|*[!0-9]*) sleep 1; continue ;; esac
  [ "$now" -ge "$target" ] && break
  sleep 1
done
if [ "${now:-0}" -lt "$target" ]; then
  echo "크레딧이 ${now:-0} 에서 안 오른다 (목표 $target) — 이 상태로는 못 잰다"; exit 2
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

# **일꾼 여럿이 보낸다.** 한 건씩 보내면 죽은 대로 간 요청이 시간 제한까지
# 매달려, 12초에 네 건밖에 못 보낸다 — 그 표본으로는 유출을 말할 수 없다.
# 실제로 그렇게 돌아서 "판정 불가" 가 나왔다.
drive() {
  local out=$1 seconds=$2 base=$3 slot deadline
  deadline=$(( $(date +%s) + seconds ))
  for slot in $(seq 1 "$WORKERS"); do
    (
      local_i=0
      while [ "$(date +%s)" -lt "$deadline" ]; do
        local_i=$(( local_i + 1 ))
        i=$(( local_i * WORKERS + slot ))
        curl -s -o /dev/null --max-time 5 -w '%{http_code}\n' -X POST \
          "$GATEWAY/api/v1/coupons/$COUPON/issue" \
          -H "X-Member-Id: $(( base + i ))" \
          -H "X-Member-Grade: GOLD" \
          -H "X-Forwarded-For: 10.15.$(( i / 250 % 250 + 1 )).$(( i % 250 + 1 ))"
      done
    ) &
  done > "$out"
  wait
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

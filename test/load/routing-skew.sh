#!/usr/bin/env bash
# 9.4.5 실측 — **게이트웨이 여러 대의 쏠림.**
#
# 기본 전략을 라운드로빈으로 뒤집는 조건이 이 실측 하나다 (90-decisions R-4).
# 지금까지 잰 것은 전부 게이트웨이 한 대짜리라, P2C 를 고른 원래 이유 — M 대가
# 같은 인스턴스로 몰린다 — 를 한 번도 안 봤다. 한 대짜리로는 원리적으로 못 잰다:
# 누적이 하나뿐이라 어긋날 상대가 없다.
#
# **집계 비율로는 안 드러난다.** 게이트웨이 둘이 같은 순서로 고르면 각자는
# 여유대로 나눈 것이라 합계가 맞는다. 깨지는 것은 같은 순간의 동시성이라,
# 뒷단에 물린 건수를 짧은 간격으로 떠서 순간 점유를 따로 본다.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1
BIG_CAP="${BIG_CAP:-50}"
SMALL_CAP="${SMALL_CAP:-10}"
MID_CAP="${MID_CAP:-30}"
STUB_LATENCY_MS="${STUB_LATENCY_MS:-1500}"
# 게이트웨이를 늘리는 겹침. 라이브러리가 이 값을 $COMPOSE 에 얹는다.
export COMPOSE_EXTRA="${COMPOSE_EXTRA:-test/load/compose.skew.yml}"

. test/load/routing-lib.sh || exit 2

GATEWAYS="${GATEWAYS:-2}"
RATE="${RATE:-66}"
DURATION_SEC="${DURATION_SEC:-40}"
IDLE_RATIO="${IDLE_RATIO:-0.9}"
WARMUP_SEC="${WARMUP_SEC:-15}"
# 표본 간격. 지연 1.5초 안에 여러 번 떠야 한 회차의 몰림이 표본에 남는다.
SAMPLE_MS="${SAMPLE_MS:-250}"
MAX_DEVIATION="${MAX_DEVIATION:-15}"
# 순간 점유의 문턱. **실측에서 나온 값이다** — 라운드로빈이 두 회차에서 9.1%
# 와 10.2% 였고 P2C 가 65.3% 였다. 25 는 라운드로빈의 두 배 반이고 P2C 의
# 2.6 분의 1 이라 양쪽에 여유가 있다. 회차 편차(최댓값으로 보면 10~20%)를
# 덮으려고 상위 5% 백분위로 재는 것과 짝이다.
MAX_SKEW="${MAX_SKEW:-25}"

require_positive_int RATE DURATION_SEC STUB_LATENCY_MS GATEWAYS SAMPLE_MS || exit 2
require_non_negative_int WARMUP_SEC || exit 2
command -v k6 >/dev/null || { echo "k6 가 없다 — 이 하네스는 고정 유입 실행기가 필요하다"; exit 2; }
[ "$GATEWAYS" -ge 2 ] || { echo "게이트웨이가 둘 이상이어야 잴 수 있다: $GATEWAYS"; exit 2; }

export BIG_INFLIGHT="${BIG_INFLIGHT:-200}" SMALL_INFLIGHT="${SMALL_INFLIGHT:-200}" \
       MID_INFLIGHT="${MID_INFLIGHT:-200}" ROUTING_PER_INSTANCE_CAP="${ROUTING_PER_INSTANCE_CAP:-200}" \
       GATEWAY_SCALE="$GATEWAYS"

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

CREDITS=("$BIG_CAP" "$SMALL_CAP" "$MID_CAP")
# 표본을 뜨는 자리. 겹침이 이 포트로 세 대를 밖에 연다.
STUB_PORTS=(18090 18091 18092)
echo "$(banner) · 게이트웨이 ${GATEWAYS}대 · 유입 ${RATE}/s 고정 · ${DURATION_SEC}초"

$COMPOSE rm -sf gateway >> "$work/up.log" 2>&1
bring_up "$work/up.log" || exit 2

case "$IDLE_RATIO" in
    0.[1-9]|0.[1-9][0-9]) ;;
    *) echo "IDLE_RATIO 는 0.1~0.9 여야 한다: '$IDLE_RATIO'"; exit 2 ;;
esac
saved=$($COMPOSE exec -T redis redis-cli GET gw:tunables 2>/dev/null)
$COMPOSE exec -T redis redis-cli SET gw:tunables \
  "{\"idleCreditRatio\":$IDLE_RATIO,\"inFlightSeconds\":3}" >/dev/null
applied=$($COMPOSE exec -T redis redis-cli GET gw:tunables 2>/dev/null)
case "$applied" in
    *"\"idleCreditRatio\":$IDLE_RATIO"*) ;;
    *) echo "튜너블을 못 세웠다 — 읽은 값: ${applied:-없음}"; exit 2 ;;
esac
restore() {
  if [ -n "$saved" ]; then $COMPOSE exec -T redis redis-cli SET gw:tunables "$saved" >/dev/null 2>&1
  else $COMPOSE exec -T redis redis-cli DEL gw:tunables >/dev/null 2>&1; fi
}
trap 'reap_children; restore; rm -rf "$work"' EXIT

# **실제로 열린 포트를 찾는다.** 범위로 열면 어느 컨테이너가 어느 포트를 받는지
# 순서가 안 정해진다. 박아 두면 한 대에만 전부 보내면서 둘에 나눠 보냈다고 적는다.
bases=""
for idx in $(seq 1 "$GATEWAYS"); do
    port=$($COMPOSE port --index "$idx" gateway 8080 2>/dev/null | sed 's/.*://')
    case "$port" in
        ''|*[!0-9]*) echo "게이트웨이 $idx 의 포트를 못 찾았다 — 늘어난 것이 맞는지 본다"; exit 2 ;;
    esac
    bases="${bases:+$bases,}http://localhost:$port"
done
echo "게이트웨이: $bases"

# **정말 여러 대가 붙었는지 본다.** 한 대만 등록되면 분모가 안 갈리고, 그 회차는
# 한 대짜리를 두 대라고 적은 것이 된다.
# 등록부는 해시고, 투표 항목(`#c:` 접두어)이 같이 들어 있다. 그것까지 세면
# 한 대만 붙어도 둘로 보인다 — 게이트웨이 수를 두 배로 세는 셈이다.
registered=$($COMPOSE exec -T redis redis-cli --raw HKEYS gw:instances 2>/dev/null \
    | grep -cv '^#c:')
case "$registered" in
    ''|*[!0-9]*) echo "등록부를 못 읽었다"; exit 2 ;;
esac
[ "$registered" -ge "$GATEWAYS" ] || {
    echo "등록된 게이트웨이가 $registered 대다 — $GATEWAYS 대를 기대했다"; exit 2; }
echo "등록된 게이트웨이: $registered 대"

# **적힌 조건과 실제 조건이 같은지 본다.** 게이트웨이가 늘면 노드당 예산이
# 갈리므로, 한 대짜리에서 쓰던 유입이 그대로면 상한을 넘겨 줄이 켜진다 — 그
# 회차는 분배가 아니라 줄을 잰 것이 된다.
credit=$($COMPOSE exec -T redis redis-cli HGET gw:snapshot '#credit' 2>/dev/null)
case "$credit" in ''|*[!0-9]*) credit=0 ;; esac
node_cap=$(( credit / registered ))
idle_cap=$(awk -v c="$node_cap" -v r="$IDLE_RATIO" 'BEGIN{printf "%d", c*r}')
window=$(( idle_cap * registered ))
echo "노드당 예산 $node_cap · 한산 상한 $idle_cap · 전체 창 ${window}/s (유입 ${RATE}/s)"
if [ "$RATE" -ge "$window" ]; then
  echo "판정 불가 — 유입 ${RATE}/s 이 전체 창 ${window}/s 위다. 줄이 켜져 분배 대신 줄을 잰다"
  exit 2
fi

wait_for_ramp || exit 2
sleep "$WARMUP_SEC"
wait_for_idle_queue || exit 2

for name in "${NAMES[@]}"; do arrived "$name" || exit 2; done > "$work/before"

# 물린 건수를 짧은 간격으로 뜬다. 부하와 같이 돌고 부하가 끝나면 멎는다.
sample_loop() {
    local sleep_sec
    sleep_sec=$(awk -v ms="$SAMPLE_MS" 'BEGIN{printf "%.3f", ms/1000}')
    while :; do
        local line="" p raw depth
        for p in "${STUB_PORTS[@]}"; do
            raw=$(curl -s --max-time 1 "http://localhost:$p/stub/health")
            depth=$(printf '%s' "$raw" | sed -n 's/.*"inflight":\([0-9][0-9]*\).*/\1/p')
            case "$depth" in ''|*[!0-9]*) depth="" ;; esac
            [ -z "$depth" ] && { line=""; break; }
            line="${line:+$line }$depth"
        done
        [ -n "$line" ] && printf '%s\n' "$line" >> "$work/samples"
        sleep "$sleep_sec"
    done
}
sample_loop &
sampler=$!

BASE_URLS="$bases" COUPON="$COUPON" RATE="$RATE" DURATION="${DURATION_SEC}s" \
  k6 run --quiet --summary-export="$work/summary.json" \
  test/load/routing-skew.js > "$work/k6.log" 2>&1
k6_rc=$?

kill "$sampler" 2>/dev/null
wait "$sampler" 2>/dev/null

sleep 3
for name in "${NAMES[@]}"; do arrived "$name" || exit 2; done > "$work/after"

if [ "$k6_rc" -ne 0 ]; then
  echo "판정 불가 — k6 가 임계를 못 지켰다 (도착을 떨어뜨렸다). 물린 건수 상한(VUS)을 올린다"
  tail -5 "$work/k6.log" | sed 's/^/  /'
  exit 2
fi

python3 - "$work/summary.json" > "$work/codes" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))["metrics"]
def n(k):
    v = m.get(k, {})
    if "count" in v: return int(v["count"])
    return int(v.get("values", {}).get("count", 0))
print("200", n("issue_200")); print("202", n("issue_202")); print("other", n("issue_other"))
print("total", n("http_reqs"))
PY
sent=$(awk '$1=="total"{print $2}' "$work/codes")
bad=$(awk '$1=="202"||$1=="other"{s+=$2} END{print s+0}' "$work/codes")
echo
echo "응답 코드: $(awk '$1!="total"{printf "%s×%s ", $2, $1}' "$work/codes")"

if [ "$bad" -ne 0 ]; then
  echo "판정 불가 — 보낸 것 중 $bad 건이 200 이 아니다. 유입이 창을 벗어났다"
  exit 2
fi

specs=(); total=0
for idx in 0 1 2; do
  b=$(sed -n "$((idx + 1))p" "$work/before"); a=$(sed -n "$((idx + 1))p" "$work/after")
  got=$(( a - b )); total=$(( total + got ))
  specs+=("${NAMES[idx]}:${CREDITS[idx]}:$got")
done
echo "도착 합계: $total (보낸 것 $sent) · 표본 $(wc -l < "$work/samples" 2>/dev/null || echo 0) 개"
echo

SAMPLES="$work/samples" MAX_DEVIATION="$MAX_DEVIATION" MAX_SKEW="$MAX_SKEW" \
  test/load/evaluate-skew.sh "${specs[@]}"

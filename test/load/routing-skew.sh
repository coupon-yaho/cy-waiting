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
# **계획서의 여유 그대로 쓴다** (9.4.5). 4 분의 1 로 줄이면 작은 대의 한 건이
# 점유의 9% 가 되어, 계기의 최소 눈금이 문턱의 3 분의 1 을 먹는다 — 행동이
# 같은 구현이 유입이나 지연을 바꾸면 미달로 적힌다. 여유를 키우면 눈금이
# 촘촘해진다: 여기서 작은 대의 한 건은 2.3% 다.
BIG_CAP="${BIG_CAP:-200}"
SMALL_CAP="${SMALL_CAP:-40}"
MID_CAP="${MID_CAP:-120}"
STUB_LATENCY_MS="${STUB_LATENCY_MS:-1500}"
# 게이트웨이를 늘리는 겹침. 라이브러리가 이 값을 $COMPOSE 에 얹는다.
export COMPOSE_EXTRA="${COMPOSE_EXTRA:-test/load/compose.skew.yml}"

. test/load/routing-lib.sh || exit 2

# **1 도 받는다.** 게이트웨이 한 대는 대조군이다 — 이 지표는 전략의 무작위성
# 만으로도 갈리므로, 한 대의 값을 모르면 두 대의 값을 게이트웨이 수에 못
# 돌린다. 한 대일 때는 재기만 하고 쏠림으로 판정하지 않는다.
GATEWAYS="${GATEWAYS:-2}"
# 깊이 = 유입 × 지연 ≥ 여유 합(360), 상한 = 한산 비율 × 여유 합(324).
RATE="${RATE:-264}"
DURATION_SEC="${DURATION_SEC:-40}"
IDLE_RATIO="${IDLE_RATIO:-0.9}"
WARMUP_SEC="${WARMUP_SEC:-15}"
# 표본 간격. 지연 1.5초 안에 여러 번 떠야 한 회차의 몰림이 표본에 남는다.
SAMPLE_MS="${SAMPLE_MS:-250}"
MAX_DEVIATION="${MAX_DEVIATION:-15}"
# 문턱은 판정기가 든다. **여기서 다시 적지 않는다** — 둘이 갈리면 게이트가
# 적힌 것과 다른 값으로 문다.
MAX_SKEW="${MAX_SKEW:-}"

require_positive_int RATE DURATION_SEC STUB_LATENCY_MS GATEWAYS SAMPLE_MS || exit 2
require_non_negative_int WARMUP_SEC || exit 2
command -v k6 >/dev/null || { echo "k6 가 없다 — 이 하네스는 고정 유입 실행기가 필요하다"; exit 2; }
[ "$GATEWAYS" -ge 1 ] || { echo "게이트웨이는 하나 이상이어야 한다: $GATEWAYS"; exit 2; }

# **상한을 기대 깊이 위로 올린다.** 여유 합의 1.1 배가 물리므로 가장 큰 대에
# 218 건이 몰리는데, 상한이 200 이면 그 대가 먼저 닿아 후보에서 빠진다 — 재려던
# 비율 대신 상한을 재게 되고, 큰 대가 −9% 로 밀리며 나머지가 그만큼 부푼다.
# 실제로 첫 회차가 그렇게 나왔다.
export BIG_INFLIGHT="${BIG_INFLIGHT:-500}" SMALL_INFLIGHT="${SMALL_INFLIGHT:-500}" \
       MID_INFLIGHT="${MID_INFLIGHT:-500}" ROUTING_PER_INSTANCE_CAP="${ROUTING_PER_INSTANCE_CAP:-500}" \
       GATEWAY_SCALE="$GATEWAYS"

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

CREDITS=("$BIG_CAP" "$SMALL_CAP" "$MID_CAP")
# 표본을 뜨는 자리. 겹침이 이 포트로 세 대를 밖에 연다.
STUB_PORTS=(18090 18091 18092)
echo "$(banner) · 게이트웨이 ${GATEWAYS}대 · 유입 ${RATE}/s 고정 · ${DURATION_SEC}초"
# **상한이 기대 깊이 위인지 본다.** 아래면 큰 대가 먼저 상한에 닿아 후보에서
# 빠지고, 그 회차는 비율이 아니라 상한을 잰 것이 된다.
peak=$(( RATE * STUB_LATENCY_MS / 1000 * BIG_CAP / (BIG_CAP + SMALL_CAP + MID_CAP) ))
if [ "$ROUTING_PER_INSTANCE_CAP" -le "$peak" ]; then
  echo "판정 불가 — 인스턴스별 상한 $ROUTING_PER_INSTANCE_CAP 이 기대 깊이 $peak 이하다"
  exit 2
fi

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
# **정확히 그 수여야 한다.** 앞 회차의 낡은 등록이 남으면 노드당 예산의 분모만
# 바뀌고, 그 회차는 적힌 것과 다른 조건에서 잰 것이 된다.
case "$registered" in
    ''|*[!0-9]*) echo "등록부를 못 읽었다"; exit 2 ;;
esac
[ "$registered" -eq "$GATEWAYS" ] || {
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

# 물린 건수 상한. 유입 × 지연보다 넉넉해야 k6 가 도착을 안 떨어뜨린다.
vus=$(( RATE * STUB_LATENCY_MS / 1000 * 3 / 2 + 50 ))
BASE_URLS="$bases" COUPON="$COUPON" RATE="$RATE" DURATION="${DURATION_SEC}s" \
  VUS="${VUS:-$vus}" GATEWAYS="$GATEWAYS" \
  k6 run --quiet --summary-export="$work/summary.json" \
  test/load/routing-skew.js > "$work/k6.log" 2>&1
k6_rc=$?

kill "$sampler" 2>/dev/null
wait "$sampler" 2>/dev/null

# 마지막 요청이 아직 물려 있다. 상수로 두면 지연을 올린 회차가 도착을 놓친다.
sleep "$(awk -v ms="$STUB_LATENCY_MS" 'BEGIN{printf "%.1f", ms/1000 + 1.5}')"
for name in "${NAMES[@]}"; do arrived "$name" || exit 2; done > "$work/after"

if [ "$k6_rc" -ne 0 ]; then
  echo "판정 불가 — k6 가 임계를 못 지켰다 (도착을 떨어뜨렸다). 물린 건수 상한(VUS)을 올린다"
  tail -5 "$work/k6.log" | sed 's/^/  /'
  exit 2
fi

GATEWAYS="$GATEWAYS" python3 - "$work/summary.json" > "$work/codes" <<'PY'
import json, os, sys
m = json.load(open(sys.argv[1]))["metrics"]
def n(k):
    v = m.get(k, {})
    if "count" in v: return int(v["count"])
    return int(v.get("values", {}).get("count", 0))
print("200", n("issue_200")); print("202", n("issue_202")); print("other", n("issue_other"))
print("total", n("http_reqs"))
for i in range(int(os.environ["GATEWAYS"])):
    print("gw%d" % i, n("issue_gw%d" % i))
PY
sent=$(awk '$1=="total"{print $2}' "$work/codes")
bad=$(awk '$1=="202"||$1=="other"{s+=$2} END{print s+0}' "$work/codes")
echo
echo "응답 코드: $(awk '$1!="total"{printf "%s×%s ", $2, $1}' "$work/codes")"

if [ "$bad" -ne 0 ]; then
  echo "판정 불가 — 보낸 것 중 $bad 건이 200 이 아니다. 유입이 창을 벗어났다"
  exit 2
fi
case "$sent" in ''|*[!0-9]*) echo "판정 불가 — k6 요약을 못 읽었다"; exit 2 ;; esac
[ "$sent" -gt 0 ] || { echo "판정 불가 — 보낸 것이 0 이다"; exit 2; }

# **생성기가 고르게 나눴는지 본다.** 한쪽으로 기울면 쏠림이 아니라 생성기를
# 잰 것이다. 게이트웨이가 하나면 볼 것이 없다.
if [ "$GATEWAYS" -gt 1 ]; then
  echo "게이트웨이별 통과: $(awk '$1 ~ /^gw/{printf "%s=%s ", $1, $2}' "$work/codes")"
  low=$(awk '$1 ~ /^gw/{if (min == "" || $2 < min) min = $2} END{print min+0}' "$work/codes")
  want=$(( sent / GATEWAYS * 9 / 10 ))
  [ "$low" -ge "$want" ] || {
    echo "판정 불가 — 가장 적게 받은 게이트웨이가 $low 건이다 (기대 $want 이상)"; exit 2; }
fi

specs=(); total=0
for idx in 0 1 2; do
  b=$(sed -n "$((idx + 1))p" "$work/before"); a=$(sed -n "$((idx + 1))p" "$work/after")
  got=$(( a - b )); total=$(( total + got ))
  specs+=("${NAMES[idx]}:${CREDITS[idx]}:$got")
done
echo "도착 합계: $total (보낸 것 $sent) · 표본 $(wc -l < "$work/samples" 2>/dev/null || echo 0) 개"
echo

SAMPLES="$work/samples" MAX_DEVIATION="$MAX_DEVIATION" \
  ${MAX_SKEW:+MAX_SKEW="$MAX_SKEW"} \
  test/load/evaluate-skew.sh "${specs[@]}"

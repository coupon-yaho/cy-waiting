#!/usr/bin/env bash
# 9.4.5 실측 — **고정 유입으로 P2C 의 깊이를 만든다.**
#
# 일꾼 하네스(`routing-ratio.sh`)는 유입을 초당 몇 건 폭 안에 못 고정한다.
# 깊이를 만들려면 유입이 하한 위여야 하고, 줄 모드를 안 켜려면 한산 통과
# 상한 아래여야 하는데 그 사이가 초당 몇 건이다. 여기서는 k6 의 고정 도착
# 실행기로 그 폭 안에 유입을 박는다. 근거는 AIJ-0220.
#
# **여유를 작게 둔다.** 고르개가 보는 것은 물린 건수 자체가 아니라 여유 대비
# 비율이라, 여유를 줄이면 같은 비율을 훨씬 낮은 동시성으로 만들 수 있다.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1
# 200/40/120 의 4분의 1. 비율은 같고 규모만 작다. **더 줄이지 않는다** —
# 유입과 한산 상한의 여유는 여유 합에 비례하므로(상한 0.9C, 필요 유입 C/지연),
# 합이 작을수록 창이 초당 한두 건 폭으로 좁아진다. 합 90 이면 열 건 남짓이다.
BIG_CAP="${BIG_CAP:-50}"
SMALL_CAP="${SMALL_CAP:-10}"
MID_CAP="${MID_CAP:-30}"
# **지연도 라이브러리를 읽기 전에 정한다.** 읽은 뒤에 정하면 5ms 가 이미 서
# 있어 무시되고, 깊이가 안 만들어진 채 도는데 배너는 그 사실을 그대로 찍는다.
# 지연에 위아래가 걸린다 — 아래는 깊이(유입 × 지연 ≥ 여유 합), 위는 서킷의
# 느림 임계 1.9초다. 그 위로 두면 모든 호출이 느린 호출로 세어져 서킷이 열린다.
STUB_LATENCY_MS="${STUB_LATENCY_MS:-1500}"

. test/load/routing-lib.sh || exit 2

# 초당 도착. 여유 합 90 · 한산 비율 0.9 면 상한이 81 이고, 지연 1.5초에서
# 깊이 90 을 만들려면 60 이 필요하다. 그 사이다.
RATE="${RATE:-60}"
DURATION_SEC="${DURATION_SEC:-40}"
# 측정 조건으로 바꾸는 운영 손잡이. 기본 0.7 이면 상한이 25 라 창이 안 열린다.
IDLE_RATIO="${IDLE_RATIO:-0.9}"
WARMUP_SEC="${WARMUP_SEC:-15}"
MAX_DEVIATION="${MAX_DEVIATION:-}"

require_positive_int RATE DURATION_SEC STUB_LATENCY_MS || exit 2
require_non_negative_int WARMUP_SEC || exit 2
command -v k6 >/dev/null || { echo "k6 가 없다 — 이 하네스는 고정 유입 실행기가 필요하다"; exit 2; }

# 스텁 한도와 인스턴스별 상한은 여유 위로 올린다. 여유를 넘긴 구간을 재는
# 것이라 둘 중 하나가 먼저 막으면 비율 대신 상한을 잰다.
export BIG_INFLIGHT="${BIG_INFLIGHT:-200}" SMALL_INFLIGHT="${SMALL_INFLIGHT:-200}" \
       MID_INFLIGHT="${MID_INFLIGHT:-200}" ROUTING_PER_INSTANCE_CAP="${ROUTING_PER_INSTANCE_CAP:-200}"

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

CREDITS=("$BIG_CAP" "$SMALL_CAP" "$MID_CAP")
echo "$(banner) · 유입 ${RATE}/s 고정 · ${DURATION_SEC}초 · 한산 비율 ${IDLE_RATIO}"

# **게이트웨이를 지우고 새로 띄운다.** 겹침은 바뀐 컨테이너만 다시 만들므로
# 앞 회차가 열어 둔 서킷이 그대로 살아 있고, 열린 서킷은 배분 게이트를 조여
# 크레딧을 1 로 떨어뜨린다 — 예열이 영영 안 끝난다. 세우기 전에 지워야 한다.
# 세운 뒤에 다시 띄우면 이미 예열에서 죽은 뒤다. 실제로 세 회차가 그렇게 죽었다.
$COMPOSE rm -sf gateway >> "$work/up.log" 2>&1
bring_up "$work/up.log" || exit 2

# **측정 조건을 적고 되돌린다.** 운영 손잡이라 남겨 두면 다음 회차가 다른
# 조건으로 재고 다른 결론을 낸다.
# **값을 검증하고 쓴 것을 확인한다.** 범위 밖이거나 모양이 어긋나면 제품이
# 조용히 기본값으로 돌고, 배너는 요청한 값을 그대로 찍는다 — 그 회차는 적힌
# 것과 다른 조건에서 잰 것이 된다.
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

wait_for_ramp || exit 2
sleep "$WARMUP_SEC"
wait_for_idle_queue || exit 2

for name in "${NAMES[@]}"; do arrived "$name" || exit 2; done > "$work/before"

BASE_URL="$GATEWAY" COUPON="$COUPON" RATE="$RATE" DURATION="${DURATION_SEC}s" \
  k6 run --quiet --summary-export="$work/summary.json" \
  test/load/routing-ratio-rate.js > "$work/k6.log" 2>&1
k6_rc=$?

# 표를 놓는 시간을 준다. 마지막 요청이 아직 뒷단에 물려 있을 수 있다.
sleep 3
for name in "${NAMES[@]}"; do arrived "$name" || exit 2; done > "$work/after"

# **k6 가 도착을 떨어뜨렸으면 고정 유입이 아니다.** 임계가 그것을 문다.
if [ "$k6_rc" -ne 0 ]; then
  echo "판정 불가 — k6 가 임계를 못 지켰다 (도착을 떨어뜨렸다). 물린 건수 상한(VUS)을 올린다"
  tail -5 "$work/k6.log" | sed 's/^/  /'
  exit 2
fi

# 상태 코드를 k6 요약에서 읽는다. **코드별 카운터가 따로 있다** — 요약은 태그를
# 접어 내보내므로 태그로 갈랐다면 여기서 200 과 202 를 못 가른다.
python3 - "$work/summary.json" > "$work/codes" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))["metrics"]
# **두 모양을 다 받는다.** k6 판에 따라 카운터가 `count` 바로 아래에 있기도
# 하고 `values.count` 로 한 겹 더 들어가기도 한다. 한쪽만 읽으면 다른 판에서
# 전부 0 이 되고, 그러면 판정기가 매 회차를 판정 불가로 끊는다.
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

specs=(); total=0
for idx in 0 1 2; do
  b=$(sed -n "$((idx + 1))p" "$work/before"); a=$(sed -n "$((idx + 1))p" "$work/after")
  arrived=$(( a - b )); total=$(( total + arrived ))
  specs+=("${NAMES[idx]}:${CREDITS[idx]}:$arrived")
done
echo "도착 합계: $total (보낸 것 $sent)"
echo

if [ "$bad" -ne 0 ]; then
  echo "판정 불가 — 보낸 것 중 $bad 건이 200 이 아니다. 유입이 창을 벗어났다"
  exit 2
fi

MAX_DEVIATION="${MAX_DEVIATION:-15}" EXPECTED_TOTAL="$sent" \
  test/load/evaluate-routing-ratio.sh "${specs[@]}"

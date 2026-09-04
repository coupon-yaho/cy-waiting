#!/usr/bin/env bash
# 9.4 실측 — **죽은 주소와 산 주소를 나란히 놓는다** (G9.11 · 9.3.11).
#
# 배선과 설정이 섰다는 것은 시험이 본다. 여기서 재는 것은 **보고에 죽은 주소가
# 섞여 있는 동안 5xx 가 새지 않는가** 다. 인스턴스가 사라진 직후가 정확히 그
# 모양이다 — 보고는 낡음 창(3초) 동안 살아 있고, 그 사이 요청이 그리로 간다.
#
# **닿았는지까지 봐야 한다.** 죽은 주소를 아무도 안 골랐으면 이 회차는 재시도를
# 한 번도 안 밟은 것이고, 그것을 "유출 0" 으로 적으면 게이트가 사라진다.
# 배제 지표가 오르는 것이 닿았다는 증거다.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1
BIG_CAP="${BIG_CAP:-200}"
SMALL_CAP="${SMALL_CAP:-40}"
MID_CAP="${MID_CAP:-120}"
STUB_LATENCY_MS="${STUB_LATENCY_MS:-1500}"

. test/load/routing-lib.sh || exit 2

RATE="${RATE:-264}"
DURATION_SEC="${DURATION_SEC:-30}"
IDLE_RATIO="${IDLE_RATIO:-0.9}"
WARMUP_SEC="${WARMUP_SEC:-15}"
# 죽은 대의 여유. 살아 있는 대들 사이에 섞이는 몫이라 작게 둔다 — 크게 두면
# 배제될 때까지 유입의 절반이 재시도를 타고, 재시도가 아니라 상한을 재게 된다.
DEAD_CAP="${DEAD_CAP:-40}"
# 이 주소는 열려 있지 않다. 이름은 풀리고 포트는 닫혀 있어 **즉시 거절**이다 —
# 사라진 직후의 모양이 그것이다. 안 풀리는 이름을 쓰면 DNS 실패라 다른 갈래다.
DEAD_ADDR="${DEAD_ADDR:-backend:9999}"

require_positive_int RATE DURATION_SEC STUB_LATENCY_MS DEAD_CAP || exit 2
require_non_negative_int WARMUP_SEC || exit 2
command -v k6 >/dev/null || { echo "k6 가 없다 — 이 하네스는 고정 유입 실행기가 필요하다"; exit 2; }

export BIG_INFLIGHT="${BIG_INFLIGHT:-500}" SMALL_INFLIGHT="${SMALL_INFLIGHT:-500}" \
       MID_INFLIGHT="${MID_INFLIGHT:-500}" ROUTING_PER_INSTANCE_CAP="${ROUTING_PER_INSTANCE_CAP:-500}"

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

echo "$(banner) · 유입 ${RATE}/s 고정 · ${DURATION_SEC}초 · 죽은 주소 ${DEAD_ADDR} (여유 ${DEAD_CAP})"

$COMPOSE rm -sf gateway >> "$work/up.log" 2>&1
bring_up "$work/up.log" || exit 2

case "$IDLE_RATIO" in
    0.[1-9]|0.[1-9][0-9]) ;;
    *) echo "IDLE_RATIO 는 0.1~0.9 여야 한다: '$IDLE_RATIO'"; exit 2 ;;
esac
saved=$($COMPOSE exec -T redis redis-cli GET gw:tunables 2>/dev/null)
cleanup() {
  if [ -n "$saved" ]; then $COMPOSE exec -T redis redis-cli SET gw:tunables "$saved" >/dev/null 2>&1
  else $COMPOSE exec -T redis redis-cli DEL gw:tunables >/dev/null 2>&1; fi
  # **죽은 주소를 걷는다.** 안 걷으면 다음 회차가 그 대를 물려받아, 재려던 것
  # 대신 이 회차가 남긴 고장을 잰다.
  $COMPOSE exec -T redis redis-cli HDEL capacity:coupon-svc:v1 stub-dead >/dev/null 2>&1
}
trap 'reap_children; cleanup; rm -rf "$work"' EXIT
$COMPOSE exec -T redis redis-cli SET gw:tunables \
  "{\"idleCreditRatio\":$IDLE_RATIO,\"inFlightSeconds\":3}" >/dev/null

wait_for_ramp || exit 2
sleep "$WARMUP_SEC"
wait_for_idle_queue || exit 2

gauge() {
    $COMPOSE exec -T gateway sh -c \
        'wget -qO- http://localhost:8081/actuator/prometheus' 2>/dev/null \
        | awk -v k="$1{" 'index($0, k) == 1 {print $2; exit}'
}

# 죽은 주소를 매 초 되살린다. 보고는 3초면 낡아 걸러지므로 한 번만 쓰면
# 부하가 시작되기도 전에 사라진다.
feed_dead() {
    while :; do
        $COMPOSE exec -T redis redis-cli HSET capacity:coupon-svc:v1 stub-dead \
            "{\"addr\":\"$DEAD_ADDR\",\"credits\":$DEAD_CAP,\"ts\":$(date +%s)}" >/dev/null 2>&1
        sleep 1
    done
}
feed_dead &
feeder=$!

# **보고에 실린 것을 보고 시작한다.** 지표는 트래픽이 있어야 오르므로 여기서는
# 못 쓴다 — 부하 중에 인스턴스 수가 넷인지로 목록 진입을 따로 확인한다.
fed=0
for _ in $(seq 1 20); do
    raw=$($COMPOSE exec -T redis redis-cli --raw HGET capacity:coupon-svc:v1 stub-dead 2>/dev/null)
    case "$raw" in *"$DEAD_ADDR"*) fed=1; break ;; esac
    sleep 1
done
[ "$fed" -eq 1 ] || { echo "판정 불가 — 죽은 주소가 보고에 안 실렸다"; exit 2; }
echo "죽은 주소를 보고에 실었다: $DEAD_ADDR"

for name in "${NAMES[@]}"; do arrived "$name" || exit 2; done > "$work/before"

# 배제와 인스턴스 수를 같이 본다. 수가 넷이어야 목록에 들어온 것이고, 배제가
# 오르면 실제로 그리로 갔다가 실패한 것이다. 둘 다 있어야 잰 것이 된다.
watch_ejected() {
    while :; do
        local v n
        v=$(gauge waiting_routing_ejected)
        n=$(gauge waiting_routing_instances)
        case "$v" in ''|*[!0-9.eE+-]*) sleep 1; continue ;; esac
        case "$n" in ''|*[!0-9.eE+-]*) n=0 ;; esac
        printf '%.0f\n' "$v" >> "$work/ejected"
        printf '%.0f\n' "$n" >> "$work/instances"
        sleep 1
    done
}
watch_ejected &
watcher=$!

vus=$(( RATE * STUB_LATENCY_MS / 1000 * 3 / 2 + 50 ))
BASE_URL="$GATEWAY" COUPON="$COUPON" RATE="$RATE" DURATION="${DURATION_SEC}s" \
  VUS="${VUS:-$vus}" \
  k6 run --quiet --summary-export="$work/summary.json" \
  test/load/routing-ratio-rate.js > "$work/k6.log" 2>&1
k6_rc=$?

kill "$watcher" "$feeder" 2>/dev/null
wait "$watcher" "$feeder" 2>/dev/null
sleep "$(awk -v ms="$STUB_LATENCY_MS" 'BEGIN{printf "%.1f", ms/1000 + 1.5}')"
for name in "${NAMES[@]}"; do arrived "$name" || exit 2; done > "$work/after"

if [ "$k6_rc" -ne 0 ]; then
  echo "판정 불가 — k6 가 임계를 못 지켰다 (도착을 떨어뜨렸다)"
  tail -5 "$work/k6.log" | sed 's/^/  /'; exit 2
fi

python3 - "$work/summary.json" > "$work/codes" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))["metrics"]
def n(k):
    v = m.get(k, {})
    return int(v["count"]) if "count" in v else int(v.get("values", {}).get("count", 0))
print("200", n("issue_200")); print("202", n("issue_202")); print("other", n("issue_other"))
print("total", n("http_reqs"))
PY
sent=$(awk '$1=="total"{print $2}' "$work/codes")
ok=$(awk '$1=="200"{print $2}' "$work/codes")
bad=$(awk '$1=="202"||$1=="other"{s+=$2} END{print s+0}' "$work/codes")
case "$sent$ok" in ''|*[!0-9]*) echo "판정 불가 — k6 요약을 못 읽었다"; exit 2 ;; esac
[ "$sent" -gt 0 ] || { echo "판정 불가 — 보낸 것이 0 이다"; exit 2; }

live=0
for idx in 0 1 2; do
  b=$(sed -n "$((idx + 1))p" "$work/before"); a=$(sed -n "$((idx + 1))p" "$work/after")
  live=$(( live + a - b ))
done
peak_eject=$(sort -n "$work/ejected" 2>/dev/null | tail -1)
case "$peak_eject" in ''|*[!0-9]*) peak_eject=0 ;; esac
peak_inst=$(sort -n "$work/instances" 2>/dev/null | tail -1)
case "$peak_inst" in ''|*[!0-9]*) peak_inst=0 ;; esac

echo
echo "응답 코드: $(awk '$1!="total"{printf "%s×%s ", $2, $1}' "$work/codes")"
echo "산 대 도착 합계: $live (보낸 것 $sent) · 인스턴스 최고 $peak_inst 대 · 배제 최고 $peak_eject 대"
echo

test/load/evaluate-dead-addr.sh "$sent" "$ok" "$bad" "$live" "$peak_inst" "$peak_eject"

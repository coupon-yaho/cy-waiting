#!/usr/bin/env bash
# 9.4 실측 — **부하가 끝나면 물린 표가 제자리로 돌아온다** (G9.3 · R-8).
#
# 완료·에러·취소·타임아웃 네 경로는 단위 시험이 덮는다. 여기서 재는 것은 그
# 경로들이 **실제 부하에서 다 도는가** 다 — 시험이 덮은 것과 배선이 부르는
# 것은 다르고, 이 페이즈에서 이미 한 번 갈렸다.
#
# **"언젠가 0" 은 근거가 아니다.** 표를 놓는 줄을 통째로 지워도 수명이 지나면
# 0 이 된다. 수명보다 훨씬 빨리 0 이 되는 것으로만 둘이 갈린다.
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
SAMPLE_MS="${SAMPLE_MS:-500}"
# 물린 표의 수명. **제품에도 같은 값을 내려 준다** — 여기만 바꾸면 판정과 제품이
# 다른 수명으로 돌고, 그러면 수명이 걷어 준 0 을 "제자리로 돌아왔다" 로 적는다.
TTL_SEC="${TTL_SEC:-30}"
export ROUTING_IN_FLIGHT_TTL="${TTL_SEC}s"
# 놓는 자리가 돌면 마지막 응답 뒤 이 안에 0 이다. 뒷단 지연 하나가 여유다.
ZERO_BUDGET_SEC="${ZERO_BUDGET_SEC:-5}"

require_positive_int RATE DURATION_SEC STUB_LATENCY_MS SAMPLE_MS TTL_SEC ZERO_BUDGET_SEC || exit 2
# 예산이 수명 이상이면 둘을 못 가른다 — 판정이 무의미해진다. 돌기 전에 끊는다.
[ "$TTL_SEC" -gt "$ZERO_BUDGET_SEC" ] || {
    echo "예산 ${ZERO_BUDGET_SEC}초가 수명 ${TTL_SEC}초 이상이면 둘을 못 가른다"; exit 2; }
require_non_negative_int WARMUP_SEC || exit 2
command -v k6 >/dev/null || { echo "k6 가 없다 — 이 하네스는 고정 유입 실행기가 필요하다"; exit 2; }

export BIG_INFLIGHT="${BIG_INFLIGHT:-500}" SMALL_INFLIGHT="${SMALL_INFLIGHT:-500}" \
       MID_INFLIGHT="${MID_INFLIGHT:-500}" ROUTING_PER_INSTANCE_CAP="${ROUTING_PER_INSTANCE_CAP:-500}"

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

echo "$(banner) · 유입 ${RATE}/s 고정 · ${DURATION_SEC}초 · 수명 ${TTL_SEC}초"

$COMPOSE rm -sf gateway >> "$work/up.log" 2>&1
bring_up "$work/up.log" || exit 2

case "$IDLE_RATIO" in
    0.[1-9]|0.[1-9][0-9]) ;;
    *) echo "IDLE_RATIO 는 0.1~0.9 여야 한다: '$IDLE_RATIO'"; exit 2 ;;
esac
saved=$($COMPOSE exec -T redis redis-cli GET gw:tunables 2>/dev/null)
restore() {
  if [ -n "$saved" ]; then $COMPOSE exec -T redis redis-cli SET gw:tunables "$saved" >/dev/null 2>&1
  else $COMPOSE exec -T redis redis-cli DEL gw:tunables >/dev/null 2>&1; fi
}
trap 'reap_children; restore; rm -rf "$work"' EXIT
$COMPOSE exec -T redis redis-cli SET gw:tunables \
  "{\"idleCreditRatio\":$IDLE_RATIO,\"inFlightSeconds\":3}" >/dev/null
applied=$($COMPOSE exec -T redis redis-cli GET gw:tunables 2>/dev/null)
case "$applied" in
    *"\"idleCreditRatio\":$IDLE_RATIO"*) ;;
    *) echo "튜너블을 못 세웠다 — 읽은 값: ${applied:-없음}"; exit 2 ;;
esac

wait_for_ramp || exit 2
sleep "$WARMUP_SEC"
wait_for_idle_queue || exit 2

# 게이지를 읽는다. **관리 포트는 밖에 안 열려 있으므로** 안에서 읽는다.
gauge() {
    $COMPOSE exec -T gateway sh -c \
        'wget -qO- http://localhost:8081/actuator/prometheus' 2>/dev/null \
        | awk '/^waiting_routing_inflight\{/ {print $2; exit}'
}

probe=$(gauge)
case "$probe" in
    ''|*[!0-9.eE+-]*) echo "판정 불가 — 게이지를 못 읽었다: '${probe:-없음}'"; exit 2 ;;
esac

# 부하 중에는 경과를 음수로 적는다. 판정기가 그것으로 두 구간을 가른다.
sample_loop() {
    local sleep_sec mark=$1
    sleep_sec=$(awk -v ms="$SAMPLE_MS" 'BEGIN{printf "%.3f", ms/1000}')
    while :; do
        local v now
        v=$(gauge)
        case "$v" in ''|*[!0-9.eE+-]*) sleep "$sleep_sec"; continue ;; esac
        now=$(date +%s%3N)
        printf '%d %d\n' "$(( now - mark ))" "$(printf '%.0f' "$v")" >> "$work/samples"
        sleep "$sleep_sec"
    done
}

# **표본은 절대 시각으로 남긴다.** 끝나는 시각을 미리 잡으면 k6 가 뜨는 동안
# 원점이 앞으로 밀려, 0 이 되기까지 걸린 시간이 그만큼 부풀려 적힌다.
sample_loop 0 &
sampler=$!

vus=$(( RATE * STUB_LATENCY_MS / 1000 * 3 / 2 + 50 ))
BASE_URL="$GATEWAY" COUPON="$COUPON" RATE="$RATE" DURATION="${DURATION_SEC}s" \
  VUS="${VUS:-$vus}" \
  k6 run --quiet --summary-export="$work/summary.json" \
  test/load/routing-ratio-rate.js > "$work/k6.log" 2>&1
k6_rc=$?
end_at=$(date +%s%3N)

# 회수를 지켜보는 구간. 수명보다 길게 봐야 "수명이 걷어 줬다" 도 표본에 남는다.
sleep "$(( TTL_SEC + 3 ))"
kill "$sampler" 2>/dev/null
wait "$sampler" 2>/dev/null

if [ "$k6_rc" -ne 0 ]; then
  echo "판정 불가 — k6 가 임계를 못 지켰다 (도착을 떨어뜨렸다)"
  tail -5 "$work/k6.log" | sed 's/^/  /'
  exit 2
fi

# 절대 시각을 원점 기준 경과로 옮긴다. 부하 중은 음수가 된다.
awk -v mark="$end_at" '{printf "%d %d\n", $1 - mark, $2}' "$work/samples" > "$work/relative"

echo "표본 $(wc -l < "$work/relative") 개"
echo
# 바닥을 조건에서 유도한다. 기대 깊이는 유입 × 지연이고, 8 할이면 예열과
# 끝맺음의 흔들림을 덮으면서 "아무것도 안 지났다" 는 확실히 거른다.
min_peak=$(( RATE * STUB_LATENCY_MS / 1000 * 8 / 10 ))
TTL_SEC="$TTL_SEC" ZERO_BUDGET_SEC="$ZERO_BUDGET_SEC" MIN_PEAK="$min_peak" \
  test/load/evaluate-leak.sh "$work/relative"

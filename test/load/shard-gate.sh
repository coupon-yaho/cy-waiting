#!/usr/bin/env bash
# Phase 10 착수 판정 회차 (10.0.5).
#
# **판정을 못 믿게 만든 것은 판정기가 아니라 회차였다.** 같은 유입으로 세 번
# 돌려 피크가 13,230 · 265 · 292 ops/s 로 갈렸는데, 원인은 앞 회차가 남긴
# 대기열이었다. 줄이 차 있으면 첫 요청부터 QUEUE_FULL 이라 등록 경로를 한 번도
# 안 밟고, 그때 찍히는 세 자릿수 ops 는 제어 평면 몫이다 — 그것을 "여유 있다"
# 로 읽으면 샤딩이 필요한 상황에서도 게이트가 안 열린다.
#
# 그래서 회차는 반드시 **빈 줄에서** 시작한다.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1

COMPOSE="docker compose -f test/load/compose.yml"
COUPON="${COUPON:-c2}"
OUT_OPS="${OUT_OPS:-redis-ops.txt}"
OUT_SUMMARY="${OUT_SUMMARY:-k6-summary.json}"

command -v k6 >/dev/null || { echo "k6 가 없다"; exit 2; }

echo "착수 판정 회차 · 쿠폰 ${COUPON}"
# **게이트웨이도 새로 만든다.** 앞 회차가 깎아 둔 회복 램프를 그대로 들고
# 있으면 크레딧이 1 에서 안 오르고, 예열이 3 분을 기다리다 죽는다. 예열
# 컨테이너는 unhealthy 로 남으면 `--wait` 가 기다리지 않고 그대로 실패로 읽는다.
#
# **겹침이 남긴 스텁도 걷는다.** 이 회차는 `compose.yml` 하나로만 돌아야 CI 와
# 같은 모양이다 — 라우팅 겹침의 스텁이 살아 있으면 뒷단 구성이 달라진다.
$COMPOSE rm -sf gateway warmup >/dev/null 2>&1
$COMPOSE up -d --wait --wait-timeout 240 --remove-orphans \
    || { echo "스택을 못 세웠다"; exit 2; }

# **줄 키만 지우면 안 된다.** 입장 커서와 최대 순번이 남으면 리더가 줄을
# 비었다고 안 보고 쿠폰을 QUEUEING 으로 되돌리며, 판정은 IDLE 이 아니면 무조건
# 줄에 세운다(추월 금지) — 첫 요청부터 202 이거나 QUEUE_FULL 이다.
$COMPOSE exec -T redis redis-cli DEL \
    "queue:{$COUPON}" "admitted:{$COUPON}" "maxscore:{$COUPON}" >/dev/null 2>&1

state=""
for _ in $(seq 1 30); do
    state=$($COMPOSE exec -T redis redis-cli HGET gw:snapshot "$COUPON" 2>/dev/null)
    case "$state" in *:IDLE:*) break ;; *) state=""; sleep 1 ;; esac
done
if [ -z "$state" ]; then
    echo "::error title=착수 판정::줄 모드가 안 꺼진다 — 이 상태로는 못 잰다"
    exit 2
fi

test/load/redis-probe.sh "$OUT_OPS" &
probe=$!
trap 'kill "$probe" 2>/dev/null' EXIT

rc=0
k6 run --summary-export="$OUT_SUMMARY" test/load/open-spike.js || rc=$?
kill "$probe" 2>/dev/null
trap - EXIT

echo "k6=$rc"
test/load/evaluate-shard-gate.sh "$OUT_OPS" "$OUT_SUMMARY"

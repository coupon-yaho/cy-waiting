#!/usr/bin/env bash
# G7.5 실측 — **스위퍼가 손 쓸 시간이 있었던 이탈자를 다 걷는가.**
#
# 크레딧 낭비는 **차례를 줬는데 아무도 안 받아 간 몫**이다. 판이 끝나고 남은
# 사람들이 다 받아 간 뒤에도 입장 임계 **아래**에 남아 있는 줄 항목이 정확히
# 그것이다 — 그 사람은 차례를 받았고, 오지 않았다.
#
# **모델을 안 쓴다.** 이전 판은 "걷을 수 있었던 인원" 을 시나리오 상수로
# 계산해 실측과 견줬는데, 그 상수 하나(250 이냐 310 이냐)가 같은 측정에서
# 통과와 미달을 뒤집었다. 여기서는 네 값이 전부 실측이다.
#
#   줄 score        = 줄 선 시각 (레디스 μs). `enqueue.lua` 가 TIME 으로 만든다
#   입장 임계 표본  = 판 도중 1초마다 뜬 `admitted:{cid}`
#   남은 유령       = 판이 끝난 뒤 임계 아래에 남은 줄 항목
#   차례를 준 인원  = `waiting_allocation_admitted_total`
#
# 유령 하나의 기다린 시간 = (임계가 그를 지난 시각) − (그가 줄 선 시각). 둘 다
# 위 표본과 score 에서 나온다. 그 시간이 생존 신호 수명을 넘었으면 스위퍼가
# 걷었어야 한다 — 그것이 기구의 결함이고, 못 넘었으면 어떤 구현으로도 못 막는다.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1

COMPOSE="docker compose -f test/load/compose.yml -f test/load/compose.waste.yml"
COUPON="${COUPON:-c2}"

# **코드에서 끌어온다.** 손으로 적으면 정책이 움직일 때 판정만 옛 값을 쓴다.
ALIVE_TTL_SEC="${ALIVE_TTL_SEC:-$(./gradlew -q printAliveTtlSeconds 2>/dev/null | tail -1)}"

WASTE_MAX="${WASTE_MAX:-0.05}"
SETTLE_SEC="${SETTLE_SEC:-90}"

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"; $COMPOSE down -v >/dev/null 2>&1' EXIT

redis() { $COMPOSE exec -T redis redis-cli "$@" 2>/dev/null | tr -d '\r'; }

# **관리 포트는 안 열려 있다.** 밖에서 닿으면 안 되는 자리라 안에서 읽는다.
scrape() {
  $COMPOSE exec -T gateway wget -qO- http://localhost:8081/actuator/prometheus 2>/dev/null
}

# 이름 하나를 숫자로. 여러 시계열이면 **합한다** — tail 로 하나만 집으면
# 라벨이 하나 늘 때 값이 조용히 줄어든다.
metric() {
  awk -v pat="$1" '$0 ~ pat && $0 !~ /^#/ { sum += $NF; found = 1 }
                   END { if (!found) exit 1; print sum }' <<<"$2"
}

if ! [[ "${ALIVE_TTL_SEC:-}" =~ ^[0-9]+$ ]]; then
  echo "::error title=G7.5 미판정::생존 신호 수명을 코드에서 못 읽었다: ${ALIVE_TTL_SEC:-없음}"
  exit 1
fi

echo "== 하네스를 띄운다 (생존 신호 수명 ${ALIVE_TTL_SEC}초) =="
# **매번 다시 짓는다.** 이미지가 남아 있으면 compose 가 그것을 그대로 쓰고,
# 지표를 새로 넣은 판에서도 옛 바이너리를 재게 된다.
$COMPOSE up -d --build --wait || { echo "::error::하네스가 안 떴다"; exit 1; }

echo "== 부하를 시작한다 =="
k6 run --quiet --summary-export="$work/summary.json" \
    test/load/abandonment-waste.js >"$work/k6.log" 2>&1 &
k6_pid=$!

# **임계를 1초마다 뜬다.** 유령이 언제 차례를 받았는지는 이 표본으로만 안다.
# 레디스 시계로 찍는다 — 줄 score 와 같은 시계여야 뺄 수 있다.
( while kill -0 "$k6_pid" 2>/dev/null; do
    t=$(redis TIME | head -1)
    a=$(redis GET "admitted:{$COUPON}")
    [ -n "$t" ] && [ -n "$a" ] && echo "$t $a" >> "$work/threshold.log"
    sleep 1
  done ) &
sampler=$!

wait "$k6_pid"
k6_rc=$?
kill "$sampler" 2>/dev/null

echo "== 마지막 사람들이 받아 갈 때까지 기다린다 =="
sleep "$SETTLE_SEC"

final=$(scrape)
admitted_total=$(metric 'waiting_allocation_admitted_total' "$final")
swept=$(metric 'waiting_sweep_total.*kind="swept"' "$final")
threshold=$(redis GET "admitted:{$COUPON}")

# **판이 끝난 뒤 임계 아래에 남은 줄 항목이 곧 낭비다.** 차례가 왔는데 안 왔다.
redis ZRANGEBYSCORE "queue:{$COUPON}" -inf "$threshold" WITHSCORES \
    | paste - - > "$work/ghosts.tsv"

joined=$(jq -r '.metrics.joined.count // .metrics.joined.values.count // empty' \
    "$work/summary.json" 2>/dev/null)
abandoned=$(jq -r '.metrics.abandoned.count // .metrics.abandoned.values.count // empty' \
    "$work/summary.json" 2>/dev/null)

for v in admitted_total swept threshold joined abandoned; do
  if [ -z "${!v:-}" ]; then
    echo "::error title=G7.5 미판정::$v 를 못 읽었다 — 배선이 끊겼거나 이름이 바뀌었다"
    exit 1
  fi
done

printf '  %-26s %s\n' "줄 선 수 / 이탈한 수" "$joined / $abandoned"
printf '  %-26s %s\n' "차례를 준 인원" "$admitted_total"
printf '  %-26s %s\n' "걷은 수" "$swept"

# **판정은 갈라 둔다.** 드라이버 안에 있으면 그것을 검증하려고 매번 도커와
# k6 를 15분씩 돌려야 하고, 그러면 아무도 안 고친다.
./test/load/evaluate-waste.sh "$work/threshold.log" "$work/ghosts.tsv" \
    "$admitted_total" "$swept" "$abandoned" "$ALIVE_TTL_SEC"
failed=$?

[ "$k6_rc" -eq 0 ] || { echo "::error::k6 가 실패했다"; tail -20 "$work/k6.log"; failed=1; }
exit "$failed"

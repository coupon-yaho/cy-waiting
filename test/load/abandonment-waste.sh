#!/usr/bin/env bash
# G7.5 실측 — **이탈 30% 에서 크레딧 낭비가 5% 미만인가.**
#
# 낭비 = 차례를 준 인원 − 실제로 받아 간 인원. 앞엣것은 배분기가 세고
# (`waiting_allocation_admitted_total`), 뒤엣것은 판정이 센다
# (`waiting_admission_decision_total{decision="PASS_TOKEN"}`).
#
# **정상 구간에서만 잰다.** 판이 시작되고 생존 신호 수명(250초) 동안은 아직
# 아무도 안 걷혔으므로 그 구간의 이탈자는 전부 낭비다. 판 전체로 재면 낭비율이
# `이탈률 × 수명 / 판_길이` 로 나오는데, 그건 기구의 성능이 아니라 판을 얼마나
# 길게 잡았는지를 재는 값이다. 그래서 창을 뒤로 밀어 놓고 그 안의 증분만 본다.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1

COMPOSE="docker compose -f test/load/compose.yml -f test/load/compose.waste.yml"
BASE_URL="${BASE_URL:-http://localhost:18080}"

# **창을 여는 시각.** 줄 세우기가 끝나고, 이탈자의 생존 신호가 만료되고, 청소가
# 재개할 때까지 기다린 뒤다. 그래야 이 뒤의 모든 입장이 "기구가 손 쓸 시간이
# 있었던" 구간에 든다 — 판이 시작되자마자 차례가 온 사람은 어떤 구현으로도
# 못 걷는다. 계획서 6절이 기준을 이 구간으로 한정한 이유다.
WINDOW_OPEN_SEC="${WINDOW_OPEN_SEC:-380}"

# 차례가 온 것을 아는 데 걸리는 시간. 서버가 말할 수 있는 가장 먼 간격이다.
# 받아 가는 쪽 창만 이만큼 늦게 연다 — 창이 열리기 직전에 차례를 받은 사람이
# 그 뒤에 오는 것을 우리 몫으로 세면 안 된다.
CLAIM_LAG_SEC="${CLAIM_LAG_SEC:-60}"

# 판이 끝나고 마지막 사람들이 받아 갈 때까지 기다리는 시간.
SETTLE_SEC="${SETTLE_SEC:-90}"

WASTE_MAX="${WASTE_MAX:-0.05}"

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"; $COMPOSE down -v >/dev/null 2>&1' EXIT

# **관리 포트는 안 열려 있다.** 밖에서 닿으면 안 되는 자리라, 안에서 읽는다.
scrape() {
  $COMPOSE exec -T gateway wget -qO- http://localhost:8081/actuator/prometheus 2>/dev/null
}

# 이름 하나를 숫자로. 없으면 빈 값을 낸다 — 0 으로 뭉개면 배선이 끊긴 판이
# "낭비 0" 으로 통과한다.
metric() {
  awk -v pat="$1" '$0 ~ pat && $0 !~ /^#/ { print $NF; found = 1 }
                   END { if (!found) exit 1 }' <<<"$2" | tail -1
}

# **매번 다시 짓는다.** 이미지가 남아 있으면 compose 가 그것을 그대로 쓰고,
# 그러면 지표를 새로 넣은 판에서도 옛 바이너리를 재게 된다 — 실제로 그랬다.
echo "== 하네스를 띄운다 =="
CREDITS="${CREDITS:-2}" $COMPOSE up -d --build --wait || { echo "::error::하네스가 안 떴다"; exit 1; }

echo "== 부하를 시작한다 =="
k6 run --quiet --summary-export="$work/summary.json" \
    test/load/abandonment-waste.js >"$work/k6.log" 2>&1 &
k6_pid=$!

# **닫는 쪽은 판이 다 끝난 뒤다.** 창을 둘 다 중간에 두면 차례를 받은 것과
# 받아 간 것 사이의 지연이 그대로 낭비로 잡힌다 — 그 지연은 폴링 간격만큼이라
# 창을 어디에 두든 몇십 %가 얹힌다.
sleep "$WINDOW_OPEN_SEC"
open_admitted=$(metric 'waiting_allocation_admitted_total' "$(scrape)")

sleep "$CLAIM_LAG_SEC"
open_claimed=$(metric 'waiting_admission_total.*outcome="PASS_TOKEN"' "$(scrape)")

wait "$k6_pid"
k6_rc=$?

echo "== 마지막 사람들이 받아 갈 때까지 기다린다 =="
sleep "$SETTLE_SEC"

final=$(scrape)
close_admitted=$(metric 'waiting_allocation_admitted_total' "$final")
close_claimed=$(metric 'waiting_admission_total.*outcome="PASS_TOKEN"' "$final")

# **못 잰 판에서도 무엇을 봤는지 남긴다.** 값 없이 미판정만 나오면 원인이
# 배선인지 판 설계인지 안 갈리고, 그때 다음 사람은 판을 처음부터 다시 짠다.
printf '  %-24s %s\n' "차례를 준 인원(열림/닫힘)" "${open_admitted:-?} / ${close_admitted:-?}"
printf '  %-24s %s\n' "받아 간 인원(열림/닫힘)" "${open_claimed:-?} / ${close_claimed:-?}"
for name in joined abandoned admittedHere; do
  printf '  %-24s %s\n' "$name" \
      "$(jq -r ".metrics.${name}.count // .metrics.${name}.values.count // \"없음\"" \
          "$work/summary.json" 2>/dev/null)"
done
# **스위퍼가 돌았는지 먼저 본다.** 낭비율만 보면 "기구가 약한 것" 과 "기구가
# 아예 안 돈 것" 이 같은 숫자로 나온다.
for kind in swept expired-signal failed; do
  printf '  %-24s %s\n' "sweep($kind)" \
      "$(metric "waiting_sweep_total.*kind=\"$kind\"" "$final" || echo 없음)"
done
tail -5 "$work/k6.log"

if [ -z "${open_admitted:-}" ] || [ -z "${close_admitted:-}" ]; then
  echo "::error title=G7.5 미판정::배분 지표를 못 읽었다 — 배선이 끊겼거나 이름이 바뀌었다"
  exit 1
fi
if [ -z "${open_claimed:-}" ] || [ -z "${close_claimed:-}" ]; then
  echo "::error title=G7.5 미판정::판정 지표를 못 읽었다 — 아무도 차례를 안 받아 갔다"
  exit 1
fi

read -r admitted claimed waste < <(awk -v oa="$open_admitted" -v ca="$close_admitted" \
    -v oc="$open_claimed" -v cc="$close_claimed" \
    'BEGIN { a = ca - oa; c = cc - oc; printf "%.0f %.0f %.6f", a, c, (a > 0 ? (a - c) / a : -1) }')

printf '  %-24s %s\n' "차례를 준 인원(합)" "$admitted"
printf '  %-24s %s\n' "받아 간 인원(합)" "$claimed"
printf '  %-24s %s\n' "크레딧 낭비율" "$waste"

failed=0
# **차례가 충분히 지나가야 잰 것이다.** 몇 명뿐이면 한 사람이 낭비율을 통째로
# 흔들어, 통과든 미달이든 그 수에 뜻이 없다.
if awk -v a="$admitted" 'BEGIN { exit (a >= 100) ? 0 : 1 }'; then :; else
  echo "::error title=G7.5 미판정::차례가 $admitted 명뿐이다 — 판이 짧거나 크레딧이 안 나갔다"
  failed=1
fi
if awk -v w="$waste" 'BEGIN { exit (w >= 0) ? 0 : 1 }'; then :; else
  echo "::error title=G7.5 미판정::차례를 준 인원이 0 이라 비율을 못 낸다"
  failed=1
fi
if awk -v w="$waste" -v m="$WASTE_MAX" 'BEGIN { exit (w <= m) ? 0 : 1 }'; then
  echo "G7.5 통과 — 대기 시간이 수명을 넘은 구간의 크레딧 낭비 $waste <= $WASTE_MAX"
else
  echo "::error title=G7.5 미달::크레딧 낭비 $waste > $WASTE_MAX"
  failed=1
fi

[ "$k6_rc" -eq 0 ] || { echo "::error::k6 가 실패했다"; tail -20 "$work/k6.log"; failed=1; }
exit "$failed"

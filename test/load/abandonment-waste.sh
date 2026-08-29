#!/usr/bin/env bash
# G7.5 실측 — **스위퍼가 걷을 수 있는 이탈자를 다 걷는가.**
#
# 크레딧 낭비는 차례를 준 인원에서 실제로 받아 간 인원을 뺀 값이다. 그런데 그
# 절대값은 기구의 성능이 아니라 **판을 얼마나 길게 잡았는지**를 잰다.
#
#   낭비율 ≈ 이탈률 × (생존 신호 수명 + 청소 재개 유예) / 판 길이
#
# 이탈자는 생존 신호가 만료되어야 걷힌다. 그 전에 차례가 오면 크레딧이 나가고,
# 그것은 어떤 구현으로도 못 막는다 — 그 사람이 떠났다는 것을 알 방법이 아직
# 없기 때문이다. 판을 두 배로 늘리면 아무것도 안 고쳐도 절반이 된다.
#
# **그래서 기구가 책임지는 구간만 잰다.** 대기 시간이 수명+유예를 넘은 사람은
# 스위퍼가 손 쓸 시간이 있었고, 거기서 놓치면 그것은 기구의 결함이다. 그
# 인원을 시나리오 값에서 구해 실제로 걷은 수와 견준다.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1

COMPOSE="docker compose -f test/load/compose.yml -f test/load/compose.waste.yml"
BASE_URL="${BASE_URL:-http://localhost:18080}"

# **줄 세우는 시간과 크레딧은 시나리오가 정한다.** 실제로 몇 명이 줄을 섰고
# 몇 명이 이탈했는지는 재서 쓴다 — 설정값을 그대로 믿으면 거절당한 사람까지
# 센 판을 잰다.
JOIN_SEC="${JOIN_SEC:-40}"
CREDITS="${CREDITS:-2}"

# 이탈자가 걷힐 수 있게 되는 시각 = **자기 생존 신호가 만료되는 때**.
# `PollIntervalPolicy.aliveTtl()` 하나가 정한다.
#
# **청소 재개 유예를 더하지 않는다.** 그 유예는 `SweepGate` 가 낡음이나 매진을
# 본 쿠폰에만 거는 것이고 정상 구간에서는 0 이다. 게다가 기준점이 리더의 틱
# 카운터라 부하 시작 시각과 무관하다. 더하면 창이 60초만큼 뒤로 밀려, 그
# 사이에 놓친 사람이 판정에서 통째로 빠진다 — 통과와 미달이 그 상수 하나로
# 갈린다.
SWEEPABLE_AFTER_SEC="${SWEEPABLE_AFTER_SEC:-250}"

# 걷을 수 있었던 인원 중 몇 %를 놓쳐도 되는가. 임계 전진과 틱 경계에서
# 몇 명은 어긋난다.
MISS_MAX="${MISS_MAX:-0.05}"

SETTLE_SEC="${SETTLE_SEC:-90}"

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

# **워밍업 구간의 낭비도 참고로 남긴다.** 판정은 안 하지만, 판을 길게 잡으면
# 이 값이 줄어든다는 것을 다음 사람이 눈으로 봐야 한다.
wait "$k6_pid"
k6_rc=$?

echo "== 마지막 사람들이 받아 갈 때까지 기다린다 =="
sleep "$SETTLE_SEC"

final=$(scrape)
close_admitted=$(metric 'waiting_allocation_admitted_total' "$final")
close_claimed=$(metric 'waiting_admission_total.*outcome="PASS_TOKEN"' "$final")

# **못 잰 판에서도 무엇을 봤는지 남긴다.** 값 없이 미판정만 나오면 원인이
# 배선인지 판 설계인지 안 갈리고, 그때 다음 사람은 판을 처음부터 다시 짠다.
printf '  %-24s %s\n' "차례를 준 인원" "${close_admitted:-못 읽음}"
printf '  %-24s %s\n' "받아 간 인원" "${close_claimed:-못 읽음}"
for name in joined abandoned admittedHere; do
  printf '  %-24s %s\n' "$name" \
      "$(jq -r ".metrics.${name}.count // .metrics.${name}.values.count // \"없음\"" \
          "$work/summary.json" 2>/dev/null)"
done
# **스위퍼가 돌았는지 먼저 본다.** 낭비율만 보면 "기구가 약한 것" 과 "기구가
# 아예 안 돈 것" 이 같은 숫자로 나온다.
swept=$(metric 'waiting_sweep_total.*kind="swept"' "$final")
for kind in swept expired-signal failed; do
  printf '  %-24s %s\n' "sweep($kind)" \
      "$(metric "waiting_sweep_total.*kind=\"$kind\"" "$final" || echo 없음)"
done
tail -5 "$work/k6.log"

if [ -z "${close_admitted:-}" ]; then
  echo "::error title=G7.5 미판정::배분 지표를 못 읽었다 — 배선이 끊겼거나 이름이 바뀌었다"
  exit 1
fi
if [ -z "${swept:-}" ]; then
  echo "::error title=G7.5 미판정::청소 지표를 못 읽었다"
  exit 1
fi

joined=$(jq -r '.metrics.joined.count // .metrics.joined.values.count // empty' \
    "$work/summary.json" 2>/dev/null)
abandoned=$(jq -r '.metrics.abandoned.count // .metrics.abandoned.values.count // empty' \
    "$work/summary.json" 2>/dev/null)
if [ -z "${joined:-}" ] || [ -z "${abandoned:-}" ]; then
  echo "::error title=G7.5 미판정::줄 선 수나 이탈한 수를 못 읽었다"
  exit 1
fi

read -r catchable missed ratio < <(awk \
    -v pop="$joined" -v join="$JOIN_SEC" -v gone="$abandoned" \
    -v credit="$CREDITS" -v after="$SWEEPABLE_AFTER_SEC" -v swept="${swept:-0}" '
    BEGIN {
        perPos = 1 / credit - join / pop
        from = (perPos > 0) ? after / perPos : pop
        frac = (pop - from) / pop
        if (frac < 0) frac = 0
        c = gone * frac
        m = c - swept
        printf "%.0f %.0f %.6f", c, m, (c > 0 ? m / c : -1)
    }')

printf '  %-24s %s\n' "걷을 수 있었던 이탈자" "$catchable"
printf '  %-24s %s\n' "실제로 걷은 수" "${swept:-못 읽음}"
printf '  %-24s %s\n' "놓친 비율" "$ratio"

failed=0
if awk -v c="$catchable" 'BEGIN { exit (c >= 100) ? 0 : 1 }'; then :; else
  echo "::error title=G7.5 미판정::걷을 수 있었던 인원이 $catchable 명뿐이다 — 판이 짧거나 줄이 얕다"
  failed=1
fi
# **양쪽으로 본다.** 0 으로 접으면 살아 있는 사람을 걷은 판이 "완벽" 으로
# 읽힌다 — 그건 순번 역행이라 놓치는 것보다 나쁘다 (불변식 4).
if awk -v r="$ratio" -v m="$MISS_MAX" 'BEGIN { exit (r < -m) ? 0 : 1 }'; then
  echo "::error title=G7.5 미판정::셈보다 $((0 - missed)) 명을 더 걷었다 — 살아 있는 사람을 걷었거나 셈이 틀렸다"
  failed=1
elif awk -v r="$ratio" -v m="$MISS_MAX" 'BEGIN { exit (r <= m) ? 0 : 1 }'; then
  echo "G7.5 통과 — 걷을 수 있었던 이탈자를 다 걷었다 (놓친 비율 $ratio <= $MISS_MAX)"
else
  echo "::error title=G7.5 미달::걷을 수 있었던 $catchable 명 중 $missed 명을 놓쳤다"
  failed=1
fi

# **워밍업 구간의 손실은 따로 적는다.** 없어지는 것이 아니라 수명과 유예에서
# 직접 나오는 값이라, 줄이려면 그 둘을 줄여야 한다 — 그리고 둘 다 줄이면
# 성실히 줄 선 사람이 걷힐 위험이 커진다. 맞바꿈이지 결함이 아니다.
printf '  %-24s %s\n' "워밍업 구간 낭비(참고)" \
    "$(awk -v a="${close_admitted:-0}" -v c="${close_claimed:-0}" \
        'BEGIN { printf "%.4f", (a > 0 ? (a - c) / a : -1) }')"

[ "$k6_rc" -eq 0 ] || { echo "::error::k6 가 실패했다"; tail -20 "$work/k6.log"; failed=1; }
exit "$failed"

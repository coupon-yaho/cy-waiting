#!/usr/bin/env bash
# LB 기준선과 LB 경유 회차를 같은 사다리로 재고, 감당과 오버헤드를 낸다.
#
# **두 표가 같은 사다리여야 뺄 수 있다.** 칸이 다르면 부하가 달라, 뺀 값이 게이트웨이 몫이 아니다. 그래서 두
# 러너를 따로 돌리지 않고 여기서 같은 `RATES` 로 부른다.
#
#   1. 기준선 — LB 뒤에 정적 응답 origin 만 두고 사다리를 돈다
#   2. 경유 회차 — 같은 LB 뒤에 게이트웨이를 두고 같은 사다리를 돈다
#   3. 감당 — 기준선이 회차의 유입을 받는가. 회차가 LB 에서 막혔으면 판정 불가다
#   4. 오버헤드 — 같은 유입 칸끼리 p99 를 뺀다. 기준은 안 짓고 기록만 한다
#
#   사용: RATES="2000 4000 8000" DURATION=30s test/load/lb-compare.sh
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1

RATES=${RATES:-"2000 4000 8000 12000 16000"}
DURATION=${DURATION:-30s}
out=${OUT_DIR:-lb-compare-out}

rm -rf "$out"; mkdir -p "$out"

echo "── 기준선 사다리"
RATES="$RATES" DURATION="$DURATION" OUT_DIR="$out/baseline" OUT_TABLE="$out/baseline.tsv" \
    test/load/lb-baseline.sh > "$out/baseline.log" 2>&1
base_rc=$?
sed 's/^/    /' "$out/baseline.log" | tail -12
if [ "$base_rc" != 0 ]; then
    echo "::error title=LB 비교::기준선 회차가 안 돌았다 (종료 $base_rc) — 뺄 기준이 없다"
    exit 2
fi

echo "── LB 경유 최대치 사다리"
VIA_LB=1 RATES="$RATES" DURATION="$DURATION" OUT_DIR="$out/peak" OUT_TABLE="$out/peak.tsv" \
    test/load/peak.sh > "$out/peak.log" 2>&1
peak_rc=$?
sed 's/^/    /' "$out/peak.log" | tail -12
# **0 이 아니면 멈춘다.** 2 만 보면 가장 낮은 회차부터 무너진 표(1)나 중간에 끊긴 표(130)가 판정기로 넘어가,
# 부분 표로 감당과 오버헤드가 나온다.
if [ "$peak_rc" != 0 ]; then
    echo "::error title=LB 비교::경유 회차가 안 섰다 (종료 $peak_rc) — 그 표는 평가하지 않는다"
    exit 2
fi

echo "── 기준선이 회차 유입을 감당하는가"
test/load/evaluate-lb-cover.sh "$out/baseline.tsv" "$out/peak.tsv" \
    "$out/peak/ceiling-cause.txt"
cover_rc=$?

echo "── 기준선을 뺀 오버헤드"
test/load/evaluate-lb-overhead.sh "$out/peak.tsv" "$out/baseline.tsv"
overhead_rc=$?

# **감당을 못 하면 오버헤드도 못 읽는다.** 그래도 표는 남긴다 — 무엇이 얼마나 모자랐는지가 다음 회차의 사다리를 정한다.
if [ "$cover_rc" != 0 ]; then
    exit "$cover_rc"
fi
exit "$overhead_rc"

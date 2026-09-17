#!/usr/bin/env bash
# LB 기준선을 뺀 게이트웨이 오버헤드 산출의 자기검증.
#
# **같은 요청 유입끼리만 뺀다.** 칸이 다르면 부하가 달라, 뺀 값이 게이트웨이 몫이 아니다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

. test/load/selftest-lib.sh || exit 2
SELFTEST_JUDGE=${SELFTEST_JUDGE:-$PWD/test/load/evaluate-lb-overhead.sh}

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

steps() {
    local name=$1
    shift
    fixture "$name" "$(printf '# 요청유입\t실측유입\t판정\t응답p99ms\n'; printf '%s\n' "$@")"
}

peak=$(steps peak.tsv "$(printf '4000\t3990\tok\t30.0')" "$(printf '8000\t7950\tok\t160.5')")
lb=$(steps lb.tsv "$(printf '4000\t3999\tok\t2.0')" "$(printf '8000\t7999\tok\t3.5')")

echo "LB 오버헤드 산출 자기검증"

run_case "같은 유입 칸끼리 뺀다" 0 "요청 4000/초 · 게이트웨이 p99 30.0 ms · 기준선 p99 2.0 ms · 오버헤드 p99 28.0 ms" \
    -- "$peak" "$lb"
run_case "칸마다 낸다" 0 "요청 8000/초 · 게이트웨이 p99 160.5 ms · 기준선 p99 3.5 ms · 오버헤드 p99 157.0 ms" \
    -- "$peak" "$lb"
# 분위수끼리 뺀 값은 어느 표본의 값도 아니다. 그 한계를 수와 같이 적는다.
run_case "뺀 값의 한계를 적는다" 0 "한 표본의 값이 아니다" -- "$peak" "$lb"

run_case "기준선에 같은 유입이 없으면 짝 없음" 0 "요청 8000/초 · 짝 없음 — 기준선에 같은 유입이 없다" \
    -- "$peak" "$(steps lb_short.tsv "$(printf '4000\t3999\tok\t2.0')")"
run_case "회차 칸이 안 섰으면 빼지 않는다" 0 "요청 8000/초 · 짝 없음 — 회차 칸이 안 섰다" \
    -- "$(steps peak_under.tsv "$(printf '4000\t3990\tok\t30.0')" "$(printf '8000\t7950\tunder\t160.5')")" "$lb"
run_case "기준선 칸의 실측이 모자라면 빼지 않는다" 0 "요청 8000/초 · 짝 없음 — 기준선 칸이 안 섰다" \
    -- "$peak" "$(steps lb_drop.tsv "$(printf '4000\t3999\tok\t2.0')" "$(printf '8000\t7000\tok\t3.5')")"
# 허용 오차 경계: 7600 은 8000 의 95% 정확히라 선다.
run_case "실측이 허용 오차 정확히면 선 칸이다" 0 "오버헤드 p99 157.0 ms" \
    -- "$peak" "$(steps lb_edge.tsv "$(printf '8000\t7600\tok\t3.5')")"

run_case "짝이 하나도 없으면 판정 불가" 2 "판정 불가" \
    -- "$peak" "$(steps lb_other.tsv "$(printf '2000\t1999\tok\t2.0')")"
run_case "표가 없으면 판정 불가" 2 "판정 불가" -- "$peak" "$work/없는표.tsv"
run_case "응답 p99 가 숫자가 아니면 판정 불가" 2 "판정 불가" \
    -- "$(steps peak_nan.tsv "$(printf '4000\t3990\tok\t느림')")" "$lb"
PEAK_ARRIVAL_TOLERANCE=abc run_case "허용 오차가 수가 아니면 판정 불가" 2 "판정 불가" -- "$peak" "$lb"
run_case "인자가 모자라면 판정 불가" 2 "판정 불가" -- "$peak"

[ "$selftest_failed" = 0 ] && echo "LB 오버헤드 산출 자기검증 통과"
exit "$selftest_failed"

#!/usr/bin/env bash
# 증설 효율 판정의 자기검증 (TS-9).
#
# **두 천장을 나눈 값이 계획서로 간다.** 한쪽 천장이 하네스의 것이면 그 나눗셈은 게이트웨이가 아니라 k6 를 잰다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

. test/load/selftest-lib.sh || exit 2
SELFTEST_JUDGE=${SELFTEST_JUDGE:-$PWD/test/load/evaluate-scale.sh}

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

# 현재 최대치 러너의 표 형식 그대로 — `요청유입<TAB>실측유입<TAB>판정<TAB>응답p99ms`.
steps() {
    local name=$1
    shift
    fixture "$name" "$(printf '# 요청유입\t실측유입\t판정\t응답p99ms\n'; printf '%s\n' "$@")"
}
cause() { fixture "$1" "$2"; }

gw="원인: 게이트웨이 — 1 대 모두 코어 한도에 붙었다"
one=$(steps one.tsv "$(printf '1000\t998\tok\t12')" "$(printf '2000\t1990\tok\t30')" \
    "$(printf '3000\t2400\tunder\t900')")
one_cause=$(cause one-cause.txt "$gw")

echo "증설 효율 자기검증"

# 천장은 선 회차 중 가장 높은 실측 유입이다 — 요청 유입이 아니다.
run_case "두 배에 가까우면 충족" 0 "충족" \
    -- "$one" "$one_cause" \
    "$(steps two.tsv "$(printf '2000\t1995\tok\t15')" "$(printf '4000\t3582\tok\t40')" \
        "$(printf '5000\t3900\tunder\t800')")" "$(cause two-cause.txt "$gw")"
# 1990 × 2 × 0.7 = 2786 — 이 값이 기준 정확히다.
run_case "기준 정확히는 충족" 0 "충족" \
    -- "$one" "$one_cause" \
    "$(steps edge.tsv "$(printf '3000\t2786\tok\t40')")" "$(cause edge-cause.txt "$gw")"
run_case "기준 바로 아래는 미달" 1 "미달" \
    -- "$one" "$one_cause" \
    "$(steps below.tsv "$(printf '3000\t2785\tok\t40')")" "$(cause below-cause.txt "$gw")"
run_case "효율 수치를 낸다" 0 "90.0%" \
    -- "$one" "$one_cause" \
    "$(steps pct.tsv "$(printf '4000\t3582\tok\t40')")" "$(cause pct-cause.txt "$gw")"

# **하네스 천장은 나누지 않는다.**
run_case "N 대 천장이 호스트면 판정 불가" 2 "판정 불가" \
    -- "$one" "$(cause host1.txt '원인: 호스트 — 하네스와 코어를 다퉈')" \
    "$(steps h2.tsv "$(printf '4000\t3582\tok\t40')")" "$(cause h2-cause.txt "$gw")"
run_case "2N 대 천장을 못 가렸으면 판정 불가" 2 "판정 불가" \
    -- "$one" "$one_cause" \
    "$(steps u2.tsv "$(printf '4000\t3582\tok\t40')")" "$(cause u2-cause.txt '원인: 가르지 못함 — 붙은 자리가 없다')"
run_case "원인 줄이 없으면 판정 불가" 2 "판정 불가" \
    -- "$one" "$one_cause" \
    "$(steps n2.tsv "$(printf '4000\t3582\tok\t40')")" "$(cause n2-cause.txt '표본이 모자라다')"
run_case "선 회차가 없으면 판정 불가" 2 "판정 불가" \
    -- "$(steps none.tsv "$(printf '1000\t500\tunder\t900')")" "$one_cause" \
    "$(steps x2.tsv "$(printf '4000\t3582\tok\t40')")" "$(cause x2-cause.txt "$gw")"
run_case "인자가 모자라면 판정 불가" 2 "판정 불가" -- "$one" "$one_cause"
SCALE_TARGET_PCT=abc run_case "기준이 백분율이 아니면 판정 불가" 2 "판정 불가" \
    -- "$one" "$one_cause" \
    "$(steps t2.tsv "$(printf '4000\t3582\tok\t40')")" "$(cause t2-cause.txt "$gw")"

[ "$selftest_failed" = 0 ] && echo "증설 효율 자기검증 통과"
exit "$selftest_failed"

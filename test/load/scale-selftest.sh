#!/usr/bin/env bash
# 증설 효율 판정의 자기검증.
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

# **원인 줄은 천장 원인 판정기가 내는 문구 그대로다.** 줄여 쓰면 앞머리만 보는 뮤턴트가 산다.
gw1="원인: 게이트웨이 — 1 대 모두 코어 한도에 붙었다"
gw2="원인: 게이트웨이 — 2 대 모두 코어 한도에 붙었다"
host="원인: 호스트 — 하네스와 코어를 다퉈 이 천장은 게이트웨이의 것이 아니다"
undecided="원인: 가르지 못함 — 붙은 자리가 없다 (게이트웨이 0/2 대)"

# 멈춤 칸은 유입은 만들었고 판정 비율이 못 선 칸이다 — 최대치 판정기가 제품 천장으로 본다.
one=$(steps one.tsv "$(printf '1000\t998\tok\t12')" "$(printf '2000\t1990\tok\t30')" \
    "$(printf '3000\t2950\tunder\t900')")
one_cause=$(cause one-cause.txt "$gw1")
stop=$(printf '6000\t5900\tunder\t900')

echo "증설 효율 자기검증"

run_case "두 배에 가까우면 충족" 0 "충족" \
    -- "$one" "$one_cause" \
    "$(steps two.tsv "$(printf '2000\t1995\tok\t15')" "$(printf '3600\t3582\tok\t40')" "$stop")" \
    "$(cause two-cause.txt "$gw2")"
# 1990 × 2 × 0.7 = 2786 — 이 값이 기준 정확히다.
run_case "기준 정확히는 충족" 0 "충족" \
    -- "$one" "$one_cause" \
    "$(steps edge.tsv "$(printf '2800\t2786\tok\t40')" "$stop")" "$(cause edge-cause.txt "$gw2")"
run_case "기준 바로 아래는 미달" 1 "미달" \
    -- "$one" "$one_cause" \
    "$(steps below.tsv "$(printf '2800\t2785\tok\t40')" "$stop")" "$(cause below-cause.txt "$gw2")"
run_case "효율 수치를 낸다" 0 "90.0%" \
    -- "$one" "$one_cause" \
    "$(steps pct.tsv "$(printf '3600\t3582\tok\t40')" "$stop")" "$(cause pct-cause.txt "$gw2")"

# **판정 비율만 선 칸은 천장이 아니다.** 러너는 실측이 모자란 칸도 판정 비율이 서면 ok 로 적는다. 그 실측을
# 천장으로 쓰면 N 대 천장이 부풀어 효율이 내려간다. 2500 을 쓰면 71.6% 가 나온다.
short=$(steps short.tsv "$(printf '1000\t998\tok\t12')" "$(printf '2000\t1990\tok\t30')" \
    "$(printf '3000\t2500\tok\t4000')")
run_case "실측이 허용 오차 밖인 ok 칸은 천장이 아니다" 0 "90.0%" \
    -- "$short" "$one_cause" \
    "$(steps short2.tsv "$(printf '3600\t3582\tok\t40')" "$stop")" "$(cause short2-cause.txt "$gw2")"
# **코어가 붙으면 도착도 모자라게 된다.** 최대치 판정기는 그 칸을 하네스 멈춤으로 적지만, 어디서 막혔는지는
# 원인 판정이 가른다. 원인이 게이트웨이면 나눈다.
run_case "하네스 멈춤이라도 원인이 게이트웨이면 나눈다" 0 "충족" \
    -- "$short" "$one_cause" \
    "$(steps harness2.tsv "$(printf '3600\t3582\tok\t40')" "$(printf '6000\t4000\tok\t900')")" \
    "$(cause harness2-cause.txt "$gw2")"

# **못 잰 칸에서 멈춘 표는 나누지 않는다.** 요약을 못 읽은 칸이 멈춤이면 그 앞 칸이 천장이라는 보장이 없다.
run_case "멈춘 칸이 판정 불가면 판정 불가" 2 "판정 불가" \
    -- "$one" "$one_cause" \
    "$(steps unm2.tsv "$(printf '3600\t3582\tok\t40')" "$(printf '6000\t0\tunmeasurable\t0')")" \
    "$(cause unm2-cause.txt "$gw2")"

# **천장을 못 본 표는 나누지 않는다.** 멈춘 칸이 없으면 그 수는 아래 경계라, 사다리를 줄이는 것으로 효율을 바꿀 수 있다.
run_case "2N 대 사다리가 천장을 못 봤으면 판정 불가" 2 "천장을 못 봤다" \
    -- "$one" "$one_cause" \
    "$(steps open2.tsv "$(printf '3600\t3582\tok\t40')")" "$(cause open2-cause.txt "$gw2")"

# **대수를 대조한다.** 같은 N 대 결과를 두 번 넘긴 배선 실수가 미달로 나가면 제품 결함으로 읽힌다.
run_case "2N 대 원인이 N 대의 두 배 대수가 아니면 판정 불가" 2 "판정 불가" \
    -- "$one" "$one_cause" \
    "$(steps same.tsv "$(printf '3600\t3582\tok\t40')" "$stop")" "$(cause same-cause.txt "$gw1")"

# **두 배를 넘는 증설은 없다.** 110% 를 넘으면 두 표가 다른 일을 잰 것이다 — 결과 섞임이 전체 유입을 따라
# 바뀌면 그렇게 된다. 3980 × 1.1 = 4378.
run_case "효율 110% 정확히는 충족" 0 "110.0%" \
    -- "$one" "$one_cause" \
    "$(steps cap.tsv "$(printf '4400\t4378\tok\t40')" "$(printf '8000\t7900\tunder\t900')")" \
    "$(cause cap-cause.txt "$gw2")"
run_case "효율 110% 를 넘으면 판정 불가" 2 "판정 불가" \
    -- "$one" "$one_cause" \
    "$(steps over.tsv "$(printf '4400\t4379\tok\t40')" "$(printf '8000\t7900\tunder\t900')")" \
    "$(cause over-cause.txt "$gw2")"

# **하네스 천장은 나누지 않는다.**
run_case "N 대 천장이 호스트면 판정 불가" 2 "판정 불가" \
    -- "$one" "$(cause host1.txt "$host")" \
    "$(steps h2.tsv "$(printf '3600\t3582\tok\t40')" "$stop")" "$(cause h2-cause.txt "$gw2")"
run_case "2N 대 천장을 못 가렸으면 판정 불가" 2 "판정 불가" \
    -- "$one" "$one_cause" \
    "$(steps u2.tsv "$(printf '3600\t3582\tok\t40')" "$stop")" "$(cause u2-cause.txt "$undecided")"
run_case "원인 줄이 없으면 판정 불가" 2 "판정 불가" \
    -- "$one" "$one_cause" \
    "$(steps n2.tsv "$(printf '3600\t3582\tok\t40')" "$stop")" "$(cause n2-cause.txt '표본이 모자라다')"
# 최대치 판정기가 든 이유를 그대로 넘긴다. 뭉뚱그리면 무엇을 고칠지 모른다.
run_case "선 칸이 없으면 최대치 판정기의 이유를 낸다" 2 "가장 낮은 회차부터" \
    -- "$(steps none.tsv "$(printf '1000\t998\tunder\t900')")" "$one_cause" \
    "$(steps x2.tsv "$(printf '3600\t3582\tok\t40')" "$stop")" "$(cause x2-cause.txt "$gw2")"
run_case "인자가 모자라면 판정 불가" 2 "판정 불가" -- "$one" "$one_cause"
SCALE_TARGET_PCT=abc run_case "기준이 백분율이 아니면 판정 불가" 2 "판정 불가" \
    -- "$one" "$one_cause" \
    "$(steps t2.tsv "$(printf '3600\t3582\tok\t40')" "$stop")" "$(cause t2-cause.txt "$gw2")"

[ "$selftest_failed" = 0 ] && echo "증설 효율 자기검증 통과"
exit "$selftest_failed"

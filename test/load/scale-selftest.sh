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

# 현재 최대치 러너의 표 형식 그대로 — `요청유입<TAB>실측유입<TAB>판정<TAB>응답p99ms<TAB>끊긴몫%`.
# 끊긴 몫을 안 적은 줄에는 1.0 을 붙인다 — 섞임이 같은 두 표가 기본이다.
steps() {
    local name=$1 row
    shift
    fixture "$name" "$(printf '# 요청유입\t실측유입\t판정\t응답p99ms\t끊긴몫%%\n'
        for row in "$@"; do
            if [ "$(printf '%s' "$row" | awk -F '\t' '{ print NF }')" -lt 5 ]; then row+=$'\t1.0'; fi
            printf '%s\n' "$row"
        done)"
}
# 옛 러너의 네 칸짜리 표. 끊긴 몫이 없어 섞임을 못 견준다.
old_steps() {
    local name=$1
    shift
    fixture "$name" "$(printf '# 요청유입\t실측유입\t판정\t응답p99ms\n'; printf '%s\n' "$@")"
}
# 러너가 남기는 원인 파일 — 천장 원인 판정기의 출력 뒤에 멈춘 칸을 적는다.
cause() { fixture "$1" "$(printf '%s\n멈춘 칸: %s\n' "$2" "$3")"; }

# **원인 줄은 천장 원인 판정기가 내는 문구 그대로다.** 줄여 쓰면 앞머리만 보는 뮤턴트가 산다.
gw1="원인: 게이트웨이 — 1 대 모두 코어 한도에 붙었다"
gw2="원인: 게이트웨이 — 2 대 모두 코어 한도에 붙었다"
host="원인: 호스트 — 하네스와 코어를 다퉈 이 천장은 게이트웨이의 것이 아니다"
undecided="원인: 가르지 못함 — 붙은 자리가 없다 (게이트웨이 0/2 대)"

# **천장은 한 점이 아니라 구간이다.** 선 칸의 실측과 멈춘 칸의 요청 사이 어딘가에 있다. N 대 천장은 1990~2100.
one=$(steps one.tsv "$(printf '1000\t998\tok\t12')" "$(printf '2000\t1990\tok\t30')" \
    "$(printf '2100\t2080\tunder\t900')")
one_cause=$(cause one-cause.txt "$gw1" 2100)
# 2N 대 천장 3582~3700 이면 효율은 3582/4200 = 85.3% 에서 3700/3980 = 93.0% 사이다.
two=$(steps two.tsv "$(printf '2000\t1995\tok\t15')" "$(printf '3600\t3582\tok\t40')" \
    "$(printf '3700\t3650\tunder\t800')")
two_cause=$(cause two-cause.txt "$gw2" 3700)

echo "증설 효율 자기검증"

run_case "구간 전체가 기준 위면 충족" 0 "충족" -- "$one" "$one_cause" "$two" "$two_cause"
run_case "효율을 구간으로 낸다" 0 "85.3%~93.0%" -- "$one" "$one_cause" "$two" "$two_cause"
# 아래 끝 = 2N 선 칸 ÷ (2 × N 멈춘 칸). 2940 / 4200 = 70% 정확히.
run_case "아래 끝이 기준 정확히면 충족" 0 "충족" \
    -- "$one" "$one_cause" \
    "$(steps edge.tsv "$(printf '3000\t2940\tok\t40')" "$(printf '3050\t3000\tunder\t900')")" \
    "$(cause edge-cause.txt "$gw2" 3050)"
# **사다리 간격이 기준을 걸치면 못 가른다.** 선 칸끼리만 나누면 간격 오차가 아래로 나 제품 결함으로 나간다.
run_case "구간이 기준을 걸치면 판정 불가" 2 "사다리" \
    -- "$one" "$one_cause" \
    "$(steps span.tsv "$(printf '3000\t2939\tok\t40')" "$(printf '3050\t3000\tunder\t900')")" \
    "$(cause span-cause.txt "$gw2" 3050)"
# 위 끝 = 2N 멈춘 칸 ÷ (2 × N 선 칸). 2700 / 3980 = 67.8%.
run_case "위 끝까지 기준 아래면 미달" 1 "미달" \
    -- "$one" "$one_cause" \
    "$(steps below.tsv "$(printf '2400\t2398\tok\t40')" "$(printf '2700\t2600\tunder\t900')")" \
    "$(cause below-cause.txt "$gw2" 2700)"
# 2786 / 3980 = 70% 정확히 — 미달로 부르려면 위 끝이 기준 아래여야 한다.
run_case "위 끝이 기준 정확히면 미달이 아니다" 2 "판정 불가" \
    -- "$one" "$one_cause" \
    "$(steps top.tsv "$(printf '2400\t2398\tok\t40')" "$(printf '2786\t2700\tunder\t900')")" \
    "$(cause top-cause.txt "$gw2" 2786)"

# **판정 비율만 선 칸은 천장이 아니다.** 러너는 실측이 모자란 칸도 판정 비율이 서면 ok 로 적는다. 그 실측(2050)을
# 선 칸으로 쓰면 위 끝이 3700/4100 = 90.2% 로 내려간다.
short=$(steps short.tsv "$(printf '1000\t998\tok\t12')" "$(printf '2000\t1990\tok\t30')" \
    "$(printf '2200\t2050\tok\t4000')")
run_case "실측이 허용 오차 밖인 ok 칸은 천장이 아니다" 0 "81.4%~93.0%" \
    -- "$short" "$(cause short-cause.txt "$gw1" 2200)" "$two" "$two_cause"
# **코어가 붙으면 도착도 모자라게 된다.** 최대치 판정기는 그 칸을 하네스 멈춤으로 적지만, 어디서 막혔는지는
# 원인 판정이 가른다. 원인이 게이트웨이면 나눈다.
run_case "하네스 멈춤이라도 원인이 게이트웨이면 나눈다" 0 "충족" \
    -- "$one" "$one_cause" \
    "$(steps harness2.tsv "$(printf '3600\t3582\tok\t40')" "$(printf '3700\t3000\tok\t900')")" \
    "$(cause harness2-cause.txt "$gw2" 3700)"

# **원인 파일은 멈춘 칸의 것이어야 한다.** 응답 기준처럼 판정기에만 있는 조건이 멈춘 칸을 앞당기면, 러너가 남긴
# 원인은 더 높은 칸의 것이 된다.
run_case "원인 파일의 칸이 멈춘 칸과 다르면 판정 불가" 2 "판정 불가" \
    -- "$one" "$one_cause" "$two" "$(cause two-late.txt "$gw2" 4000)"
run_case "원인 파일에 칸이 없으면 판정 불가" 2 "판정 불가" \
    -- "$one" "$(fixture nostep.txt "$gw1")" "$two" "$two_cause"

# **못 잰 칸에서 멈춘 표는 나누지 않는다.** 요약을 못 읽은 칸이 멈춤이면 그 앞 칸이 천장이라는 보장이 없다.
run_case "멈춘 칸이 판정 불가면 판정 불가" 2 "판정 불가" \
    -- "$one" "$one_cause" \
    "$(steps unm2.tsv "$(printf '3600\t3582\tok\t40')" "$(printf '3700\t0\tunmeasurable\t0')")" \
    "$(cause unm2-cause.txt "$gw2" 3700)"
# **천장을 못 본 표는 나누지 않는다.** 멈춘 칸이 없으면 그 수는 아래 경계라, 사다리를 줄이는 것으로 효율을 바꿀 수 있다.
run_case "2N 대 사다리가 천장을 못 봤으면 판정 불가" 2 "천장을 못 봤다" \
    -- "$one" "$one_cause" \
    "$(steps open2.tsv "$(printf '3600\t3582\tok\t40')")" "$(cause open2-cause.txt "$gw2" 3600)"

# **대수를 대조한다.** 같은 N 대 결과를 두 번 넘긴 배선 실수가 미달로 나가면 제품 결함으로 읽힌다.
run_case "2N 대 원인이 N 대의 두 배 대수가 아니면 판정 불가" 2 "판정 불가" \
    -- "$one" "$one_cause" "$two" "$(cause same-cause.txt "$gw1" 3700)"

# **두 배를 넘는 증설은 없다.** 아래 끝이 110% 를 넘으면 두 표가 다른 일을 잰 것이다 — 결과 섞임이 전체 유입을
# 따라 바뀌면 그렇게 된다. 4620 / 4200 = 110% 정확히.
run_case "아래 끝 110% 정확히는 충족" 0 "충족" \
    -- "$one" "$one_cause" \
    "$(steps cap.tsv "$(printf '4700\t4620\tok\t40')" "$(printf '4800\t4700\tunder\t900')")" \
    "$(cause cap-cause.txt "$gw2" 4800)"
run_case "아래 끝이 110% 를 넘으면 판정 불가" 2 "섞임" \
    -- "$one" "$one_cause" \
    "$(steps over.tsv "$(printf '4700\t4621\tok\t40')" "$(printf '4800\t4700\tunder\t900')")" \
    "$(cause over-cause.txt "$gw2" 4800)"

# **하네스 천장은 나누지 않는다.**
run_case "N 대 천장이 호스트면 판정 불가" 2 "판정 불가" \
    -- "$one" "$(cause host1.txt "$host" 2100)" "$two" "$two_cause"
run_case "2N 대 천장을 못 가렸으면 판정 불가" 2 "판정 불가" \
    -- "$one" "$one_cause" "$two" "$(cause u2-cause.txt "$undecided" 3700)"
# 원인 줄은 줄 머리에서만 읽는다. 표본 줄이나 오류 문구 속의 같은 말을 원인으로 읽으면 안 된다.
run_case "원인 문구가 줄 머리에 없으면 판정 불가" 2 "판정 불가" \
    -- "$one" "$one_cause" "$two" "$(cause mid2-cause.txt "  이전 칸 $gw2" 3700)"
run_case "원인 줄이 없으면 판정 불가" 2 "판정 불가" \
    -- "$one" "$one_cause" "$two" "$(cause n2-cause.txt '표본이 모자라다' 3700)"
# 최대치 판정기가 든 이유를 그대로 넘긴다. 뭉뚱그리면 무엇을 고칠지 모른다.
run_case "선 칸이 없으면 최대치 판정기의 이유를 낸다" 2 "가장 낮은 회차부터" \
    -- "$(steps none.tsv "$(printf '1000\t998\tunder\t900')")" "$(cause none-cause.txt "$gw1" 1000)" \
    "$two" "$two_cause"
run_case "대수가 0 이면 판정 불가" 2 "판정 불가" \
    -- "$one" "$(cause zero1.txt "원인: 게이트웨이 — 0 대 모두 코어 한도에 붙었다" 2100)" \
    "$two" "$(cause zero2.txt "원인: 게이트웨이 — 0 대 모두 코어 한도에 붙었다" 3700)"
run_case "인자가 모자라면 판정 불가" 2 "판정 불가" -- "$one" "$one_cause"
SCALE_TARGET_PCT=abc run_case "기준이 백분율이 아니면 판정 불가" 2 "판정 불가" \
    -- "$one" "$one_cause" "$two" "$two_cause"

# **두 표의 결과 섞임이 같아야 나눈다** (CY-990). 줄 상한에 닿아 싼 거절이 늘면 대당 처리량이 부풀어, 그 두
# 천장은 다른 일을 잰 것이다. 멈춘 칸의 끊긴 몫을 견준다.
run_case "멈춘 칸의 끊긴 몫이 크게 다르면 판정 불가" 2 "섞임이 다르다" \
    -- "$one" "$one_cause" \
    "$(steps mix.tsv "$(printf '2000\t1995\tok\t15\t0.8')" "$(printf '3600\t3582\tok\t40\t20.0')" \
        "$(printf '3700\t3650\tunder\t800\t35.0')")" "$two_cause"
run_case "허용 차 안이면 섞임이 같다" 0 "충족" \
    -- "$one" "$one_cause" \
    "$(steps mix_ok.tsv "$(printf '2000\t1995\tok\t15\t1.0')" "$(printf '3600\t3582\tok\t40\t2.0')" \
        "$(printf '3700\t3650\tunder\t800\t3.9')")" "$two_cause"
run_case "섞임을 판정문에 싣는다" 0 "끊긴 몫 1.0%·3.9%" \
    -- "$one" "$one_cause" \
    "$(steps mix_show.tsv "$(printf '2000\t1995\tok\t15\t1.0')" "$(printf '3600\t3582\tok\t40\t2.0')" \
        "$(printf '3700\t3650\tunder\t800\t3.9')")" "$two_cause"
run_case "끊긴 몫이 없는 옛 표면 판정 불가" 2 "끊긴 몫이 없어" \
    -- "$one" "$one_cause" \
    "$(old_steps old.tsv "$(printf '2000\t1995\tok\t15')" "$(printf '3600\t3582\tok\t40')" \
        "$(printf '3700\t3650\tunder\t800')")" "$two_cause"
# 허용 차 정확히는 같은 섞임이다. 뺄셈을 부동소수로 견주면 3.3·8.3 이 넘는 쪽으로 떨어진다.
run_case "허용 차 정확히는 같은 섞임이다" 0 "충족" \
    -- "$(steps mix_e1.tsv "$(printf '1000\t998\tok\t12\t3.3')" "$(printf '2000\t1990\tok\t30\t3.3')" \
        "$(printf '2100\t2080\tunder\t900\t3.3')")" "$one_cause" \
    "$(steps mix_e2.tsv "$(printf '2000\t1995\tok\t15\t8.3')" "$(printf '3600\t3582\tok\t40\t8.3')" \
        "$(printf '3700\t3650\tunder\t800\t8.3')")" "$two_cause"
# 멈춘 칸은 마지막 줄이 아닐 수 있다. 응답 기준이 앞당기면 가운데 줄이다.
PEAK_LATENCY_P99_MS=100 run_case "가운데 줄에서 멈추면 그 칸의 끊긴 몫을 본다" 2 "섞임이 다르다" \
    -- "$(steps mid1.tsv "$(printf '1000\t998\tok\t12\t1.0')" "$(printf '2000\t1990\tok\t30\t1.0')" \
        "$(printf '2100\t2080\tok\t900\t1.0')" "$(printf '2200\t2100\tunder\t950\t1.0')")" \
    "$(cause mid1-cause.txt "$gw1" 2100)" \
    "$(steps mid2.tsv "$(printf '2000\t1995\tok\t15\t1.0')" "$(printf '3600\t3582\tok\t40\t1.0')" \
        "$(printf '3700\t3650\tok\t800\t20.0')" "$(printf '3800\t3700\tunder\t900\t1.0')")" \
    "$(cause mid2-cause.txt "$gw2" 3700)"
run_case "멈춘 칸의 끊긴 몫이 - 면 판정 불가" 2 "끊긴 몫이 없어" \
    -- "$one" "$one_cause" \
    "$(steps dash.tsv "$(printf '2000\t1995\tok\t15\t1.0')" "$(printf '3600\t3582\tok\t40\t1.0')" \
        "$(printf '3700\t3650\tunder\t800\t-')")" "$two_cause"
MIX_TOLERANCE_PP=abc run_case "섞임 허용 차가 수가 아니면 판정 불가" 2 "판정 불가" \
    -- "$one" "$one_cause" "$two" "$two_cause"

[ "$selftest_failed" = 0 ] && echo "증설 효율 자기검증 통과"
exit "$selftest_failed"

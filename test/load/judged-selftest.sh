#!/usr/bin/env bash
# 판정 비율 판정의 자기검증 (TS-9).
#
# **이 기준이 `5xx < 0.1%` 를 대신한다.** 여기가 조용히 틀리면 게이트웨이가
# 제 일을 못 한 회차가 충족으로 적힌다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

. test/load/selftest-lib.sh || exit 2
# 돌연변이를 넣은 사본을 검증 대상으로 지목할 수 있어야 한다. 무조건 덮어쓰면
# 사본을 겨눈 회차가 조용히 원본을 돌리고 전부 "안 잡힘" 으로 나온다.
SELFTEST_JUDGE=${SELFTEST_JUDGE:-$PWD/test/load/evaluate-judged.sh}

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

# 프로메테우스 노출 형식 그대로 쓴다. 모양이 바뀌면 파싱이 먼저 깨져야 한다.
metrics() {
    local path=$work/$1
    shift
    printf '%s\n' "$@" > "$path"
    printf '%s' "$path"
}
fresh() { printf 'waiting_judgement_total{application="waiting",quality="fresh"} %s' "$1"; }
degraded() { printf 'waiting_judgement_total{application="waiting",quality="degraded"} %s' "$1"; }

# 회차가 시작하기 전의 표본. 대부분의 사례는 여기서 0 으로 출발한다.
zero=$(metrics zero.txt "$(fresh 0.0)" "$(degraded 0.0)")

echo "판정 비율 자기검증"

run_case "전부 판정했으면 충족" 0 "충족" \
    -- "$zero" "$(metrics all.txt "$(fresh 20000.0)")"
# 2 만 중 열아홉이면 99.905% 로 기준 바로 위다.
run_case "기준 바로 위는 충족" 0 "충족" \
    -- "$zero" "$(metrics edge_ok.txt "$(fresh 19981.0)" "$(degraded 19.0)")"
# 스물이면 99.9% 정확히 — 기준 이상이라 충족이다.
run_case "기준 정확히는 충족" 0 "충족" \
    -- "$zero" "$(metrics edge.txt "$(fresh 19980.0)" "$(degraded 20.0)")"
run_case "기준 바로 아래는 미달" 1 "미달" \
    -- "$zero" "$(metrics edge_bad.txt "$(fresh 19979.0)" "$(degraded 21.0)")"

# **기준을 실제로 쓰는지 본다.** 안 쓰면 어떤 값을 줘도 같은 답이 나온다.
JUDGED_TARGET_PCT=99.99 run_case "기준을 올리면 미달이 된다" 1 "미달" \
    -- "$zero" "$(metrics edge_ok.txt "$(fresh 19981.0)" "$(degraded 19.0)")"

# **기준도 확인해야 한다.** 음수를 주면 어떤 회차도 안 걸리고, 오타는 "미달" 로
# 나가 계기 문제가 제품 문제로 읽힌다.
JUDGED_TARGET_PCT=-1 run_case "음수 기준은 막는다" 2 "백분율이 아니다" \
    -- "$zero" "$(metrics edge_bad.txt "$(fresh 19979.0)" "$(degraded 21.0)")"
JUDGED_TARGET_PCT=101 run_case "100 을 넘는 기준은 막는다" 2 "백분율이 아니다" \
    -- "$zero" "$(metrics all.txt "$(fresh 20000.0)")"
JUDGED_TARGET_PCT=구십구 run_case "기준이 숫자가 아니면 막는다" 2 "백분율이 아니다" \
    -- "$zero" "$(metrics all.txt "$(fresh 20000.0)")"

# **반올림한 표시값으로 판정하면 안 된다.** 1,000 만 중 9,989,998 은 99.89998%
# 라 미달인데, 넷째 자리에서 자르면 "99.9000" 이 되어 기준과 같아진다. 표시값을
# 비교하던 동안 이 회차가 충족으로 나갔다.
run_case "반올림으로 기준을 넘기지 않는다" 1 "미달" \
    -- "$zero" "$(metrics round.txt "$(fresh 9989998.0)" "$(degraded 10002.0)")"

# **앞 회차와 예열이 분모에 섞이면 미달이 충족으로 뒤집힌다.** 이전 표본에
# 신선 120 만이 쌓여 있고 이 회차가 99.0% 면, 차분을 안 하면 99.92% 가 된다.
warm=$(metrics warm.txt "$(fresh 1200000.0)" "$(degraded 0.0)")
run_case "예열을 뺀 뒤에 판정한다" 1 "미달" \
    -- "$warm" "$(metrics warm_after.txt "$(fresh 1299000.0)" "$(degraded 1000.0)")"
# 같은 이후 표본이라도 예열이 없었으면 실제로 충족이다 — 차분이 방향을 가른다.
run_case "차분이 회차의 값을 낸다" 0 "충족" \
    -- "$(metrics warm2.txt "$(fresh 1200000.0)" "$(degraded 990.0)")" \
       "$(metrics warm_after2.txt "$(fresh 1299000.0)" "$(degraded 1000.0)")"

# **계수는 줄 수 없다.** 줄었으면 회차 중에 프로세스가 다시 뜬 것이라, 차분이
# 그 앞의 트래픽을 통째로 빼먹는다.
run_case "계수가 줄면 판정 불가" 2 "다시 떴다" \
    -- "$(metrics hi.txt "$(fresh 50000.0)")" "$(metrics lo.txt "$(fresh 20000.0)")"

# **지수 표기를 읽는다.** 프로메테우스가 1e7 부터 그렇게 낸다 — 목표 규모에서만
# 판정기가 죽는 자리다.
run_case "지수 표기를 읽는다" 0 "충족" \
    -- "$zero" "$(metrics exp.txt "$(fresh 1.2E7)" "$(degraded 100.0)")"

# **라벨이 갈려 시계열이 둘이면 못 읽는다.** 마지막 값만 쓰는 것도 이어 붙이는
# 것도 틀린다. 무엇이 갈렸는지를 먼저 봐야 한다.
run_case "시계열이 둘이면 막는다" 2 "둘 이상이다" \
    -- "$zero" "$(metrics multi.txt \
        'waiting_judgement_total{node="a",quality="fresh"} 10000.0' \
        'waiting_judgement_total{node="b",quality="fresh"} 10000.0')"

# **없는 것과 0 을 가른다.** 지표를 못 긁은 회차를 열화 0 으로 읽으면
# 아무것도 안 잰 회차가 100% 로 나온다.
run_case "신선 계수가 없으면 판정 불가" 2 "계수가 없다" \
    -- "$zero" "$(metrics nofresh.txt "$(degraded 20.0)")"
# 열화 계수는 없을 수 있다 — 한 번도 안 났으면 그 시계열이 안 생긴다.
run_case "열화 계수가 없으면 0 으로 본다" 0 "충족" \
    -- "$zero" "$(metrics nodeg.txt "$(fresh 20000.0)")"

run_case "판정이 적으면 판정 불가" 2 "부하가 안 닿았다" \
    -- "$zero" "$(metrics few.txt "$(fresh 300.0)" "$(degraded 1.0)")"
# **기대 건수를 알면 하한이 그것을 따라간다.** 절대값 하나로는 목표가 커질수록
# 아무것도 안 막는다 — 600 만을 기대하는 회차에서 1,000 건은 0.017% 다.
EXPECT_TOTAL=6000000 run_case "기대치의 절반에 못 미치면 판정 불가" 2 "부하가 안 닿았다" \
    -- "$zero" "$(metrics all.txt "$(fresh 20000.0)")"

run_case "계수가 숫자가 아니면 막는다" 2 "숫자가 아니다" \
    -- "$zero" "$(metrics bad.txt 'waiting_judgement_total{quality="fresh"} oops')"

: > "$work/empty.txt"
run_case "표본이 비면 막는다" 2 "표본이 비었다" -- "$zero" "$work/empty.txt"
run_case "표본 파일이 없으면 막는다" 2 "표본이 비었다" -- "$zero" "$work/없는파일.txt"
# 회차 이전 표본을 안 주면 절대값으로 판정하게 된다. 그 자리를 아예 막는다.
# 배선 실수라 판정 불가(2)다 — 1 로 내면 러너의 오타가 제품 미달로 읽힌다.
run_case "이후 표본만 주면 막는다" 2 "표본 파일 둘이 필요하다" -- "$zero"
# 하한이 비숫자면 가드가 조용히 사라진다. 그 자리를 막는지 본다.
EXPECT_TOTAL=많이 run_case "기대 건수가 숫자가 아니면 막는다" 2 "숫자가 아니다" \
    -- "$zero" "$(metrics all.txt "$(fresh 20000.0)")"

[ "$selftest_failed" -eq 0 ] && echo "판정 비율 자기검증 통과" || echo "판정 비율 자기검증 실패"
exit "$selftest_failed"

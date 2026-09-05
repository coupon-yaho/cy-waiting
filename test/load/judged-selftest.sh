#!/usr/bin/env bash
# 판정 비율 판정의 자기검증 (TS-9).
#
# **이 기준이 `5xx < 0.1%` 를 대신한다.** 여기가 조용히 틀리면 게이트웨이가
# 제 일을 못 한 회차가 충족으로 적힌다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

. test/load/selftest-lib.sh || exit 2
SELFTEST_JUDGE=$PWD/test/load/evaluate-judged.sh

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

echo "판정 비율 자기검증"

run_case "전부 판정했으면 충족" 0 "충족" \
    -- "$(metrics all.txt "$(fresh 20000.0)")"
# 2 만 중 열아홉이면 99.905% 로 기준 바로 위다.
run_case "기준 바로 위는 충족" 0 "충족" \
    -- "$(metrics edge_ok.txt "$(fresh 19981.0)" "$(degraded 19.0)")"
# 스물이면 99.9% 정확히 — 기준 이상이라 충족이다.
run_case "기준 정확히는 충족" 0 "충족" \
    -- "$(metrics edge.txt "$(fresh 19980.0)" "$(degraded 20.0)")"
run_case "기준 바로 아래는 미달" 1 "미달" \
    -- "$(metrics edge_bad.txt "$(fresh 19979.0)" "$(degraded 21.0)")"

# **기준을 실제로 쓰는지 본다.** 안 쓰면 어떤 값을 줘도 같은 답이 나온다.
JUDGED_TARGET_PCT=99.99 run_case "기준을 올리면 미달이 된다" 1 "미달" \
    -- "$(metrics edge_ok.txt "$(fresh 19981.0)" "$(degraded 19.0)")"

# **없는 것과 0 을 가른다.** 지표를 못 긁은 회차를 열화 0 으로 읽으면
# 아무것도 안 잰 판이 100% 로 나온다.
run_case "신선 계수가 없으면 판정 불가" 1 "계수가 없다" \
    -- "$(metrics nofresh.txt "$(degraded 20.0)")"
# 열화 계수는 없을 수 있다 — 한 번도 안 났으면 그 시계열이 안 생긴다.
run_case "열화 계수가 없으면 0 으로 본다" 0 "충족" \
    -- "$(metrics nodeg.txt "$(fresh 20000.0)")"

run_case "판정이 적으면 판정 불가" 1 "부하가 안 닿았다" \
    -- "$(metrics few.txt "$(fresh 300.0)" "$(degraded 1.0)")"
run_case "계수가 숫자가 아니면 막는다" 1 "숫자가 아니다" \
    -- "$(metrics bad.txt 'waiting_judgement_total{quality="fresh"} oops')"

: > "$work/empty.txt"
run_case "표본이 비면 막는다" 1 "표본이 비었다" -- "$work/empty.txt"
run_case "표본 파일이 없으면 막는다" 1 "표본이 비었다" -- "$work/없는파일.txt"

[ "$selftest_failed" -eq 0 ] && echo "판정 비율 자기검증 통과" || echo "판정 비율 자기검증 실패"
exit "$selftest_failed"

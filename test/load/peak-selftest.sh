#!/usr/bin/env bash
# 최대치 판정의 자기검증 (TS-9).
#
# **이 판정이 Phase 10 착수 표에 적힐 수를 만든다.** 여기가 조용히 틀리면
# 하네스가 못 만든 부하를 게이트웨이가 견딘 것으로 적게 된다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

. test/load/selftest-lib.sh || exit 2
SELFTEST_JUDGE=${SELFTEST_JUDGE:-$PWD/test/load/evaluate-peak.sh}

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

# 회차 표를 만든다. 한 줄이 한 회차다 — 요청 유입·실측 유입·판정·응답 p99.
table() {
    local path=$work/$1
    shift
    printf '%s\n' "$@" > "$path"
    printf '%s' "$path"
}

echo "최대치 판정 자기검증"

# 하네스가 만든 회차 하나. 실측이 요청의 95% 이상이라 선다.
one=$(table one.tsv $'4000\t3950\tok\t2.1')
run_case "한 회차가 서면 그 실측이 최대치다" 0 "3950" -- "$one"

# **가장 낮은 회차조차 못 만들었으면 잰 것이 없다.** 0 을 최대치로 적으면
# 게이트웨이가 아무것도 못 한 것처럼 읽힌다 — 계기 문제를 제품 문제로 옮긴다.
run_case "가장 낮은 회차를 못 만들면 판정 불가" 2 "잰 것이 없다" \
    -- "$(table nofloor.tsv $'4000\t1200\tok\t2.1')"

# 실측이 모자란 회차에서 멈춘다. 그 위는 게이트웨이 얘기가 아니다.
ladder=$(table ladder.tsv $'2000\t1990\tok\t1.8' $'4000\t3950\tok\t2.4' $'8000\t5900\tok\t3.1')
run_case "도착이 모자라면 거기서 멈춘다" 0 "3950" -- "$ladder"
run_case "그 천장은 하네스 쪽이다" 0 "하네스" -- "$ladder"

# 판정 비율이 미달인 회차는 게이트웨이가 못 버틴 것이다 — 천장의 종류가 다르다.
product=$(table product.tsv $'2000\t1990\tok\t1.8' $'4000\t3950\tunder\t2.4')
run_case "판정이 미달이면 제품 천장이다" 0 "제품" -- "$product"
run_case "제품 천장 아래가 최대치다" 0 "1990" -- "$product"

# **응답 기준은 줄 때만 건다.** 끝에서 끝까지의 값이라 G10.10 의 오버헤드가
# 아니고, 근거 없는 수를 기본값으로 두면 그 수가 계획서로 새어 나간다.
slow=$(table slow.tsv $'2000\t1990\tok\t1.8' $'4000\t3950\tok\t7.2')
run_case "기준을 안 주면 느려도 선다" 0 "3950" -- "$slow"
PEAK_LATENCY_P99_MS=5 run_case "기준을 주면 느린 회차가 제품 천장이다" 0 "제품" -- "$slow"

# **천장을 못 본 회차를 최대치로 적지 않는다.** 사다리가 짧았을 뿐이다.
run_case "전 회차가 다 서면 천장을 못 본 것이다" 0 "천장을 아직 못 봤다" \
    -- "$(table all.tsv $'2000\t1990\tok\t1.8' $'4000\t3950\tok\t2.4')"

# **기준을 실제로 쓰는지 본다.** 안 쓰면 어떤 값을 줘도 같은 답이 나온다.
PEAK_ARRIVAL_TOLERANCE=0.99 run_case "허용 오차를 조이면 최대치가 내려간다" 0 "1990" \
    -- "$ladder"
PEAK_LATENCY_P99_MS=2.0 run_case "응답 기준을 조이면 최대치가 내려간다" 0 "1990" \
    -- "$ladder"

# 바닥을 주면 되돌아가는 것을 막는다. 안 주면 기록만 한다.
PEAK_FLOOR=5000 run_case "바닥 아래는 미달" 1 "미달" -- "$ladder"
PEAK_FLOOR=3000 run_case "바닥 위는 충족" 0 "충족" -- "$ladder"
PEAK_FLOOR=오천 run_case "바닥이 숫자가 아니면 판정 불가" 2 "바닥" -- "$ladder"

# 계기가 틀어진 모양들. 전부 판정 불가여야 한다 — 미달로 내면 제품 탓이 된다.
run_case "인자가 없으면 판정 불가" 2 "회차 표" --
run_case "회차가 한 줄도 없으면 판정 불가" 2 "한 줄도 없다" -- "$(table empty.tsv '')"
run_case "칸이 모자라면 판정 불가" 2 "칸" -- "$(table short.tsv $'4000\t3950\tok')"
run_case "요청 유입이 숫자가 아니면 판정 불가" 2 "숫자" \
    -- "$(table nan.tsv $'사천\t3950\tok\t2.1')"
run_case "판정 칸이 셋 밖이면 판정 불가" 2 "판정 칸" \
    -- "$(table verdict.tsv $'4000\t3950\tmaybe\t2.1')"
run_case "요청 유입이 오름차순이 아니면 판정 불가" 2 "오름차순" \
    -- "$(table order.tsv $'4000\t3950\tok\t2.1' $'2000\t1990\tok\t1.8')"
# **실측이 요청을 크게 넘으면 계기가 틀어진 것이다.** 그 수를 최대치로 적으면
# 만든 적 없는 부하가 표에 남는다.
run_case "실측이 요청을 크게 넘으면 판정 불가" 2 "계기" \
    -- "$(table over.tsv $'4000\t9000\tok\t2.1')"
# 판정을 못 한 회차는 미달과 다르다. 그 위를 안 쓰되 제품 탓으로 적지 않는다.
run_case "판정 불가 회차에서 멈추면 그렇게 적는다" 0 "판정 불가" \
    -- "$(table unmeas.tsv $'2000\t1990\tok\t1.8' $'4000\t3950\tunmeasurable\t2.4')"
# **실패 뒤에 선 회차는 안 쓴다.** 사다리는 아래에서부터 이어져야 한다.
gap=$(table gap.tsv $'2000\t1990\tok\t1.8' $'4000\t2100\tok\t2.4' $'8000\t7900\tok\t3.1')
run_case "실패 뒤에 선 회차가 있으면 경고한다" 0 "이어지지 않았다" -- "$gap"
# **경고만 보면 안 된다.** 문구는 그대로 두고 값만 위 회차로 바꾸는 판이 있다.
run_case "이어지지 않은 위 회차는 최대치가 아니다" 0 "최대치(실측 유입) 1990" -- "$gap"

[ "$selftest_failed" -eq 0 ] && echo "최대치 자기검증 통과" || echo "최대치 자기검증 실패"
exit "$selftest_failed"

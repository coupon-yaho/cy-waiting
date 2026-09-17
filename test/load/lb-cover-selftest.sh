#!/usr/bin/env bash
# LB 기준선 감당 판정의 자기검증.
#
# **이 판정이 틀리면 LB 가 못 받은 부하가 게이트웨이 수치로 적힌다.**
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

. test/load/selftest-lib.sh || exit 2
SELFTEST_JUDGE=${SELFTEST_JUDGE:-$PWD/test/load/evaluate-lb-cover.sh}

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

# 최대치 러너와 기준선 러너가 같이 쓰는 표 형식 — `요청유입<TAB>실측유입<TAB>판정<TAB>응답p99ms`.
steps() {
    local name=$1
    shift
    fixture "$name" "$(printf '# 요청유입\t실측유입\t판정\t응답p99ms\n'; printf '%s\n' "$@")"
}

peak=$(steps peak.tsv "$(printf '4000\t3990\tok\t30')" "$(printf '8000\t7950\tok\t160')" \
    "$(printf '16000\t14600\tok\t900')")

echo "LB 기준선 감당 자기검증"

# **멈춘 칸까지 센다.** 회차가 그 유입을 LB 에 실었으니, LB 가 거기까지 받아야 그 칸의 멈춤을 게이트웨이 탓으로 읽는다.
run_case "기준선이 회차의 가장 높은 요청 유입 위에서 서면 감당" 0 "감당" \
    -- "$(steps lb_high.tsv "$(printf '16000\t15990\tok\t2')" "$(printf '32000\t25000\tok\t9')")" "$peak"
run_case "같은 요청 유입에서 서면 감당" 0 "감당" \
    -- "$(steps lb_same.tsv "$(printf '16000\t15990\tok\t2')" "$(printf '32000\t25000\tok\t9')")" \
    "$(steps peak_same.tsv "$(printf '16000\t14600\tok\t900')")"
run_case "기준선이 낮은 칸에서 멈추면 판정 불가" 2 "못 감당" \
    -- "$(steps lb_low.tsv "$(printf '8000\t7990\tok\t2')" "$(printf '16000\t12000\tok\t9')")" "$peak"
# 회차가 선 칸은 8000 이지만 16000 을 실었다.
run_case "회차의 선 칸이 아니라 실은 칸과 견준다" 2 "못 감당" \
    -- "$(steps lb_mid.tsv "$(printf '8000\t7990\tok\t2')" "$(printf '16000\t12000\tok\t9')")" \
    "$(steps peak_stop.tsv "$(printf '8000\t7950\tok\t160')" "$(printf '16000\t9000\tok\t900')")"
# **아래 경계로 충분하다.** 기준선이 천장을 못 봤어도 거기까지는 받았다.
run_case "기준선이 천장을 못 봤어도 그 칸까지 섰으면 감당" 0 "감당" \
    -- "$(steps lb_open.tsv "$(printf '8000\t7990\tok\t2')" "$(printf '16000\t15990\tok\t3')")" "$peak"

# 못 읽는 것은 감당했다고 적지 않는다.
run_case "기준선에 선 칸이 없으면 판정 불가" 2 "판정 불가" \
    -- "$(steps lb_none.tsv "$(printf '4000\t2000\tok\t2')")" "$peak"
run_case "기준선 표가 없으면 판정 불가" 2 "판정 불가" -- "$work/없는표.tsv" "$peak"
run_case "회차 표가 없으면 판정 불가" 2 "판정 불가" \
    -- "$(steps lb_ok.tsv "$(printf '16000\t15990\tok\t2')")" "$work/없는표.tsv"
run_case "회차 표에 칸이 없으면 판정 불가" 2 "판정 불가" \
    -- "$(steps lb_ok2.tsv "$(printf '16000\t15990\tok\t2')")" "$(steps empty.tsv)"
run_case "회차 요청 유입이 숫자가 아니면 판정 불가" 2 "판정 불가" \
    -- "$(steps lb_ok3.tsv "$(printf '16000\t15990\tok\t2')")" "$(steps nan.tsv "$(printf '만\t7950\tok\t160')")"
run_case "인자가 모자라면 판정 불가" 2 "판정 불가" -- "$peak"

[ "$selftest_failed" = 0 ] && echo "LB 기준선 감당 자기검증 통과"
exit "$selftest_failed"

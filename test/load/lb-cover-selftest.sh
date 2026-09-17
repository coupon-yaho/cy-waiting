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
    -- "$(steps lb_high.tsv "$(printf '16000\t15990\tok\t2')" "$(printf '32000\t31000\tok\t9')")" "$peak"
run_case "같은 요청 유입에서 서면 감당" 0 "감당" \
    -- "$(steps lb_same.tsv "$(printf '16000\t15990\tok\t2')" "$(printf '32000\t25000\tok\t9')")" \
    "$(steps peak_same.tsv "$(printf '16000\t14600\tok\t900')")"
run_case "기준선이 낮은 칸에서 멈추면 판정 불가" 2 "못 감당" \
    -- "$(steps lb_low.tsv "$(printf '8000\t7990\tok\t2')" "$(printf '16000\t12000\tok\t9')")" "$peak"
# 회차가 선 칸은 8000 이지만 16000 을 실었다.
run_case "회차의 선 칸이 아니라 실은 칸과 견준다" 2 "못 감당" \
    -- "$(steps lb_mid.tsv "$(printf '8000\t7990\tok\t2')" "$(printf '16000\t12000\tok\t9')")" \
    "$(steps peak_stop.tsv "$(printf '8000\t7950\tok\t160')" "$(printf '16000\t7000\tok\t900')")"
# 요약을 못 읽은 칸도 실었던 칸이다. 실측이 0 이라고 건너뛰면 필요한 유입이 낮아져 거짓 감당이 난다.
run_case "판정 불가로 멈춘 칸까지 센다" 2 "못 감당" \
    -- "$(steps lb_unm.tsv "$(printf '8000\t7990\tok\t2')" "$(printf '16000\t12000\tok\t9')")" \
    "$(steps peak_unm.tsv "$(printf '8000\t7950\tok\t160')" "$(printf '16000\t0\tunmeasurable\t0')")"
# **응답 기준을 물려받지 않는다.** 최대치 판정기의 응답 기준이 새어 들어오면 멀쩡한 기준선 칸이 안 선 칸이 된다.
PEAK_LATENCY_P99_MS=1 run_case "응답 기준 환경변수를 물려받지 않는다" 0 "감당" \
    -- "$(steps lb_lat.tsv "$(printf '16000\t15990\tok\t2')")" "$peak"

# **아래 경계로 충분하다.** 기준선이 천장을 못 봤어도 거기까지는 받았다.
run_case "기준선이 천장을 못 봤어도 그 칸까지 섰으면 감당" 0 "감당" \
    -- "$(steps lb_open.tsv "$(printf '8000\t7990\tok\t2')" "$(printf '16000\t15990\tok\t3')")" "$peak"

# **멈춤의 원인이 LB 면 감당한 것이 아니다.** 기준선은 CPU 로만 본 것이라, 경유 회차가 LB 에서 막힌 칸을 못 가른다.
lb_ok=$(steps lb_cause_ok.tsv "$(printf '16000\t15990\tok\t2')")
run_case "경유 회차의 멈춤이 LB 면 판정 불가" 2 "판정 불가" \
    -- "$lb_ok" "$peak" "$(fixture cause_lb.txt '원인: LB — 연결 한도에서 막혔다 (오류 3 건)')"
run_case "멈춤이 게이트웨이면 감당" 0 "감당" \
    -- "$lb_ok" "$peak" "$(fixture cause_gw.txt '원인: 게이트웨이 — 1 대 모두 코어 한도에 붙었다')"
run_case "원인 파일이 없어도 유입만으로 판정한다" 0 "감당" -- "$lb_ok" "$peak"

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

#!/usr/bin/env bash
# 서킷 회복 판정의 자기검증 (TS-9).
#
# **이 판정이 서킷 해제 램프의 유일한 실측 근거다.** 램프는 게이트웨이 한 대
# 짜리 단위 시험으로만 잡혀 있었다. 여기가 조용히 틀리면, 회복이 계단인 회차가
# 충족으로 적힌다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

. test/load/selftest-lib.sh || exit 2
# 돌연변이를 넣은 사본을 겨눌 수 있어야 한다. 무조건 덮어쓰면 사본을 지목한
# 회차가 조용히 원본을 돌리고 전부 "안 잡힘" 으로 나온다.
SELFTEST_JUDGE=${SELFTEST_JUDGE:-$PWD/test/load/evaluate-circuit-recovery.sh}

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

# 표본 한 줄: <시각ms> <발행 크레딧> <뒷단 누적 도착> <노드 수>
# 구간은 `#` 줄로 가른다. 러너가 자극을 준 시각에 그 줄을 쓴다.
NODES=2
# 노드 둘이면 한산 통과가 성립하는 최소가 4 다 (노드 수 × 유휴 나눗값).
# **정상 구간을 만든다.** 크레딧 300, 초당 100 건 도착. 이 둘이 기준선이다.
normal() {
    local t=$1 served=$2 n=${3:-8}
    for i in $(seq 0 $((n - 1))); do
        printf '%s 300 %s %s\n' $((t + i * 200)) $((served + i * 20)) "$NODES"
    done
}

# 조인 구간. 크레딧이 노드 수 이하로 내려가 있고 도착도 멎는다.
gated() {
    local t=$1 served=$2 credit=${3:-2} n=${4:-5}
    for i in $(seq 0 $((n - 1))); do
        printf '%s %s %s %s\n' $((t + i * 200)) "$credit" "$served" "$NODES"
    done
}

# 회복 구간. 하한 4 에서 두 배씩 올라 300 에 닿는다. 도착은 크레딧을 따라간다.
recovering() {
    local t=$1 served=$2
    local i=0
    for credit in 4 8 16 32 64 128 256 300 300; do
        printf '%s %s %s %s\n' $((t + i * 200)) "$credit" "$((served + i * 20))" "$NODES"
        i=$((i + 1))
    done
}

healthy_run() {
    {
        echo '# 정상'; normal 0 0
        echo '# 진입'; gated 1600 160 2 3
        echo '# 유지'; gated 2200 160 2 5
        echo '# 회복'; recovering 3200 160
        echo '# 승계'; printf '%s 300 %s %s\n' 5200 340 "$NODES"
    } > "$work/$1"
    printf '%s' "$work/$1"
}

echo "서킷 회복 자기검증"

run_case "정상 회차는 충족" 0 "충족" -- "$(healthy_run ok.txt)"

# **진입이 늦으면 조임이 안 걸린 것이다.** 뒷단이 못 받는데 몫이 그대로 나간다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 300 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'; recovering 3200 160
    echo '# 승계'; printf '%s 300 %s %s\n' 5200 340 "$NODES"
} > "$work/late.txt"
run_case "진입이 늦으면 미달" 1 "조이지 않았다" -- "$work/late.txt"

# 유지 구간에 한 번이라도 상한 위로 튀면 조임이 안 붙어 있는 것이다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 2
    printf '%s 300 160 %s\n' 2600 "$NODES"
    echo '# 회복'; recovering 3200 160
    echo '# 승계'; printf '%s 300 %s %s\n' 5200 340 "$NODES"
} > "$work/leak.txt"
run_case "유지가 새면 미달" 1 "조임이 유지되지 않았다" -- "$work/leak.txt"

# **회복이 늦으면 램프가 게이트를 깬다.** 30초 안에 못 돌아오면 미달이다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'
    for i in $(seq 0 40); do
        printf '%s %s %s %s\n' $((3200 + i * 1000)) $((4 + i)) $((160 + i * 4)) "$NODES"
    done
    echo '# 승계'; printf '%s 300 %s %s\n' 60000 400 "$NODES"
} > "$work/slow.txt"
run_case "회복이 느리면 미달" 1 "회복이" -- "$work/slow.txt"

# **봉우리가 기준선의 1.2 배를 넘으면 회복이 곧 2차 장애다** (RC4).
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'
    printf '%s 4 160 %s\n' 3200 "$NODES"
    printf '%s 300 300 %s\n' 3400 "$NODES"
    printf '%s 300 320 %s\n' 3600 "$NODES"
    echo '# 승계'; printf '%s 300 340 %s\n' 3800 "$NODES"
} > "$work/burst.txt"
run_case "회복 봉우리가 크면 미달" 1 "봉우리" -- "$work/burst.txt"

# **회복 구간에 한산 통과 상한이 0 이 되면 안 된다** (R1). 노드 둘이면 4 미만이다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'
    printf '%s 2 160 %s\n' 3200 "$NODES"
    printf '%s 300 180 %s\n' 3400 "$NODES"
    echo '# 승계'; printf '%s 300 200 %s\n' 3600 "$NODES"
} > "$work/idle.txt"
run_case "회복 중 한산 통과가 막히면 미달" 1 "한산 통과" -- "$work/idle.txt"

# **승계가 계단을 되살리면 안 된다.** 이어받은 노드는 조인 적이 없어 램프가
# 안 걸린다 — 게이트웨이가 둘 이상일 때만 열리는 구멍이라 여기서만 잡힌다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'
    printf '%s 4 160 %s\n' 3200 "$NODES"
    printf '%s 8 168 %s\n' 3400 "$NODES"
    echo '# 승계'; printf '%s 300 188 %s\n' 3600 "$NODES"
} > "$work/handover.txt"
run_case "승계가 계단을 되살리면 미달" 1 "승계" -- "$work/handover.txt"

# **리더를 죽이면 노드 수가 준다.** 그건 우리가 만든 자극이라 판정을 막지
# 않는다. 승계 뒤 최소도 그 줄어든 수를 따라가야 한다 — 시작 값을 박아 두면
# 문턱이 실제보다 두 배라 지킨 회차가 미달로 적힌다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'
    printf '%s 4 160 %s\n' 3200 "$NODES"
    printf '%s 8 168 %s\n' 3400 "$NODES"
    echo '# 승계'
    printf '%s 16 176 1\n' 3600
    printf '%s 32 184 1\n' 3800
    printf '%s 300 200 1\n' 4000
} > "$work/shrink.txt"
run_case "승계로 노드가 줄어도 충족" 0 "충족" -- "$work/shrink.txt"

# **서킷이 완전히 열렸다 돌아오는 회차.** 조인 동안의 몫이 0 이라, 승계가
# 회복의 첫 표본에 걸리면 앞 값이 0 이다 — 배수만으로는 허용이 0 이 되어, 한산
# 통과 최소를 지킨 회차가 미달로 적힌다. 그 자리를 하한이 받친다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 0 3
    echo '# 유지'; gated 2200 160 0 5
    echo '# 회복'
    echo '# 승계'
    printf '%s 4 160 %s\n' 3200 "$NODES"
    printf '%s 8 168 %s\n' 3400 "$NODES"
    printf '%s 300 188 %s\n' 3600 "$NODES"
} > "$work/fullopen.txt"
run_case "전면 정지에서 돌아와도 충족" 0 "충족" -- "$work/fullopen.txt"

# ── 판정 불가 ────────────────────────────────────────────────────────────────

# **느는 것은 자극이 아니라 오염이다.** 앞 회차의 등록이 살아나거나 다른 스택이
# 붙은 것이고, 그러면 한산 통과 문턱이 회차 중에 올라간다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'
    printf '%s 4 160 %s\n' 3200 "$NODES"
    printf '%s 300 180 3\n' 3400
    echo '# 승계'; printf '%s 300 200 3\n' 3600
} > "$work/grow.txt"
run_case "노드가 늘면 판정 불가" 2 "늘었다" -- "$work/grow.txt"

: > "$work/empty.txt"
run_case "표본이 비면 판정 불가" 2 "표본이 비었다" -- "$work/empty.txt"
run_case "표본 파일이 없으면 판정 불가" 2 "표본이 비었다" -- "$work/없는파일.txt"

{
    normal 0 0
    recovering 3200 160
} > "$work/nomark.txt"
run_case "구간 표시가 없으면 판정 불가" 2 "구간 표시" -- "$work/nomark.txt"

# **한 대짜리 회차는 이 시나리오가 아니다.** 승계도 쏠림도 원리적으로 안 생긴다.
{
    echo '# 정상'; printf '%s 300 0 1\n' 0; printf '%s 300 20 1\n' 200
    echo '# 진입'; printf '%s 1 20 1\n' 1600
    echo '# 유지'; printf '%s 1 20 1\n' 2200
    echo '# 회복'; printf '%s 2 20 1\n' 3200; printf '%s 300 40 1\n' 3400
    echo '# 승계'; printf '%s 300 60 1\n' 3600
} > "$work/single.txt"
run_case "노드가 하나면 판정 불가" 2 "게이트웨이가" -- "$work/single.txt"

{
    echo '# 정상'; normal 0 0
    echo '# 진입'; printf '%s 오류 160 %s\n' 1600 "$NODES"
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'; recovering 3200 160
    echo '# 승계'; printf '%s 300 340 %s\n' 5200 "$NODES"
} > "$work/bad.txt"
run_case "표본이 숫자가 아니면 판정 불가" 2 "표본이 숫자가 아니다" -- "$work/bad.txt"

# **기준선을 못 만들면 판정할 게 없다.** 정상 구간이 짧으면 그 회차의 목표를 모른다.
#
# 표본을 셋 준다. 하나만 주면 유입도 0 이 되어, 기준선 가드를 지워도 유입 가드가
# 대신 막는다 — 어느 가드가 일했는지 안 갈린다.
{
    echo '# 정상'; normal 0 0 3
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'; recovering 3200 160
    echo '# 승계'; printf '%s 300 340 %s\n' 5200 "$NODES"
} > "$work/short.txt"
run_case "정상 구간이 짧으면 판정 불가" 2 "기준선 표본이" -- "$work/short.txt"

# **누적은 줄 수 없다.** 줄었으면 회차 중에 뒷단이 다시 떴다는 뜻이라, 도착
# 셈이 그 앞의 트래픽을 통째로 빼먹는다.
{
    echo '# 정상'; normal 0 500
    echo '# 진입'; gated 1600 100 2 3
    echo '# 유지'; gated 2200 100 2 5
    echo '# 회복'; recovering 3200 100
    echo '# 승계'; printf '%s 300 280 %s\n' 5200 "$NODES"
} > "$work/reset.txt"
run_case "도착이 줄면 판정 불가" 2 "다시 떴다" -- "$work/reset.txt"

# 기준선 유입이 0 이면 회복 봉우리를 어디에 견줄지가 없다.
{
    echo '# 정상'; gated 0 0 300 8
    echo '# 진입'; gated 1600 0 2 3
    echo '# 유지'; gated 2200 0 2 5
    echo '# 회복'; recovering 3200 0
    echo '# 승계'; printf '%s 300 180 %s\n' 5200 "$NODES"
} > "$work/noload.txt"
run_case "기준선 유입이 0 이면 판정 불가" 2 "부하가 안 닿았다" -- "$work/noload.txt"

[ "$selftest_failed" -eq 0 ] && echo "서킷 회복 자기검증 통과" || echo "서킷 회복 자기검증 실패"
exit "$selftest_failed"

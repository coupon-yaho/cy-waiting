#!/usr/bin/env bash
# 천장 원인 판정의 자기검증 (TS-9).
#
# **이 판정이 증설 효율의 전제다.** 여기가 조용히 틀리면 하네스가 못 만든 천장이 게이트웨이의
# 천장으로 적히고, 그 두 수를 나눈 효율이 계획서로 간다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

. test/load/selftest-lib.sh || exit 2
SELFTEST_JUDGE=${SELFTEST_JUDGE:-$PWD/test/load/evaluate-bottleneck.sh}

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

# 표집기가 쓰는 형식 그대로다 — `종류<TAB>이름<TAB>값`. 게이트웨이·레디스·스텁은 CPU(한 코어가 100),
# 호스트는 유휴 백분율이다. 한 초에 한 벌씩 쌓인다.
samples() {
    local name=$1 gateway=$2 redis=$3 idle=$4 seconds=${5:-5}
    local body=""
    for _ in $(seq 1 "$seconds"); do
        body+=$(printf 'cpu\tload-gateway-1\t%s\ncpu\tload-redis-1\t%s\ncpu\tload-backend-1\t20.0\nidle\thost\t%s\n' \
            "$gateway" "$redis" "$idle")
        body+=$'\n'
    done
    fixture "$name" "$body"
}

echo "천장 원인 자기검증"

# 코어 둘이면 200 이 한도다. 그 90% 는 180.
GATEWAY_CPUS=2 run_case "게이트웨이가 한도에 붙었으면 게이트웨이" 0 "원인: 게이트웨이" \
    -- "$(samples gw.tsv 190.0 30.0 40.0)"
GATEWAY_CPUS=2 run_case "한도 90% 정확히는 게이트웨이" 0 "원인: 게이트웨이" \
    -- "$(samples gw_edge.tsv 180.0 30.0 40.0)"
GATEWAY_CPUS=2 run_case "한도 바로 아래는 게이트웨이가 아니다" 0 "원인: 가르지 못함" \
    -- "$(samples gw_below.tsv 179.0 30.0 40.0)"

# **호스트가 먼저다.** 호스트가 말랐으면 게이트웨이가 한도에 붙은 것도 k6 와 코어를 다툰 결과일 수 있다.
GATEWAY_CPUS=2 run_case "호스트가 마르면 게이트웨이가 붙어도 호스트" 0 "원인: 호스트" \
    -- "$(samples host.tsv 190.0 30.0 5.0)"
GATEWAY_CPUS=2 run_case "호스트 유휴 10% 정확히는 호스트가 아니다" 0 "원인: 게이트웨이" \
    -- "$(samples host_edge.tsv 190.0 30.0 10.0)"

# 레디스는 명령을 한 스레드로 처리하므로 한 코어가 한도다.
GATEWAY_CPUS=2 run_case "레디스가 한 코어에 붙었으면 레디스" 0 "원인: 레디스" \
    -- "$(samples redis.tsv 120.0 95.0 40.0)"

GATEWAY_CPUS=2 run_case "아무도 안 붙었으면 가르지 못함" 0 "원인: 가르지 못함" \
    -- "$(samples none.tsv 100.0 30.0 40.0)"

# 대마다 한도를 준다. 게이트웨이가 여럿이면 **전 대가** 붙어야 게이트웨이 천장이다 — 한 대만 붙은 것은
# 고르게 안 나뉜 하네스 쪽 문제다.
two=$(fixture two.tsv "$(for _ in 1 2 3 4 5; do
    printf 'cpu\tload-gateway-1\t95.0\ncpu\tload-gateway-2\t96.0\ncpu\tload-redis-1\t30.0\nidle\thost\t40.0\n'
done)")
GATEWAY_CPUS=1 run_case "두 대 모두 한도면 게이트웨이" 0 "원인: 게이트웨이" -- "$two"
lopsided=$(fixture lopsided.tsv "$(for _ in 1 2 3 4 5; do
    printf 'cpu\tload-gateway-1\t99.0\ncpu\tload-gateway-2\t40.0\ncpu\tload-redis-1\t30.0\nidle\thost\t40.0\n'
done)")
GATEWAY_CPUS=1 run_case "한 대만 붙었으면 게이트웨이가 아니다" 0 "원인: 가르지 못함" -- "$lopsided"

# **못 잰 것을 판정하지 않는다.** 표본이 모자라면 회차 중간의 한 순간을 천장 원인으로 적게 된다.
GATEWAY_CPUS=2 run_case "표본이 셋 미만이면 판정 불가" 2 "판정 불가" \
    -- "$(samples short.tsv 190.0 30.0 40.0 2)"
GATEWAY_CPUS=2 run_case "게이트웨이 표본이 없으면 판정 불가" 2 "판정 불가" \
    -- "$(fixture nogw.tsv "$(printf 'idle\thost\t40.0\nidle\thost\t40.0\nidle\thost\t40.0\n')")"
GATEWAY_CPUS=2 run_case "숫자가 아니면 판정 불가" 2 "판정 불가" \
    -- "$(fixture nan.tsv "$(for _ in 1 2 3; do printf 'cpu\tload-gateway-1\t--\nidle\thost\t40.0\n'; done)")"
GATEWAY_CPUS=2 run_case "파일이 없으면 판정 불가" 2 "판정 불가" -- "$work/없는파일.tsv"
GATEWAY_CPUS=2 run_case "인자가 없으면 판정 불가" 2 "판정 불가" --
GATEWAY_CPUS=0 run_case "코어 한도가 양수가 아니면 판정 불가" 2 "판정 불가" \
    -- "$(samples badcpu.tsv 190.0 30.0 40.0)"

[ "$selftest_failed" = 0 ] && echo "천장 원인 자기검증 통과"
exit "$selftest_failed"

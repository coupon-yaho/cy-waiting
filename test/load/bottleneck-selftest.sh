#!/usr/bin/env bash
# 천장 원인 판정의 자기검증.
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
GATEWAYS=2 GATEWAY_CPUS=1 run_case "두 대 모두 한도면 게이트웨이" 0 "원인: 게이트웨이" -- "$two"
lopsided=$(fixture lopsided.tsv "$(for _ in 1 2 3 4 5; do
    printf 'cpu\tload-gateway-1\t99.0\ncpu\tload-gateway-2\t40.0\ncpu\tload-redis-1\t30.0\nidle\thost\t40.0\n'
done)")
GATEWAYS=2 GATEWAY_CPUS=1 run_case "한 대만 붙었으면 게이트웨이가 아니다" 0 "원인: 가르지 못함" -- "$lopsided"
# **뜬 대수를 표본에서 세지 않는다.** 한 대가 죽었거나 표집에 안 잡히면 남은 한 대만 붙어도 "모두" 가 된다.
GATEWAYS=2 GATEWAY_CPUS=2 run_case "기대한 대수보다 표본의 대가 적으면 판정 불가" 2 "판정 불가" \
    -- "$(samples missing.tsv 190.0 30.0 40.0)"

# **공유 자원이 먼저다.** 레디스가 붙었으면 게이트웨이를 늘려도 안 풀린다 — 둘 다 붙은 칸을 게이트웨이로 적으면
# 증설 효율이 그 칸을 나눈다.
GATEWAY_CPUS=2 run_case "게이트웨이와 레디스가 둘 다 붙었으면 레디스" 0 "원인: 레디스" \
    -- "$(samples both.tsv 190.0 95.0 40.0)"
GATEWAY_CPUS=2 run_case "레디스 90% 정확히는 레디스" 0 "원인: 레디스" \
    -- "$(samples redis_edge.tsv 100.0 90.0 40.0)"
GATEWAY_CPUS=2 run_case "레디스 90% 바로 아래는 레디스가 아니다" 0 "원인: 가르지 못함" \
    -- "$(samples redis_below.tsv 100.0 89.9 40.0)"

# **표집은 회차 앞뒤에 걸친다.** k6 가 VU 를 띄우기 전과 끝난 뒤의 표본이 섞이므로 평균으로 가르면 몇 개가
# 판정을 뒤집는다. 가운데 값으로 가른다.
edges=$(fixture edges.tsv "$(printf 'cpu\tload-gateway-1\t5.0\ncpu\tload-redis-1\t5.0\nidle\thost\t90.0\n'
    for _ in $(seq 1 14); do
        printf 'cpu\tload-gateway-1\t190.0\ncpu\tload-redis-1\t30.0\nidle\thost\t8.0\n'
    done
    printf 'cpu\tload-gateway-1\t5.0\ncpu\tload-redis-1\t5.0\nidle\thost\t90.0\n')")
GATEWAY_CPUS=2 run_case "앞뒤 한가한 표본이 호스트 마름을 가리지 않는다" 0 "원인: 호스트" -- "$edges"
tail_idle=$(fixture tail.tsv "$(for _ in $(seq 1 12); do
        printf 'cpu\tload-gateway-1\t190.0\ncpu\tload-redis-1\t30.0\nidle\thost\t40.0\n'
    done
    for _ in 1 2 3; do printf 'cpu\tload-gateway-1\t5.0\nidle\thost\t95.0\n'; done)")
GATEWAY_CPUS=2 run_case "끝난 뒤 표본이 붙음을 가리지 않는다" 0 "원인: 게이트웨이" -- "$tail_idle"

GATEWAY_CPUS=1.5 run_case "코어 한도를 소수 그대로 적는다" 0 "1.5 코어" \
    -- "$(samples frac.tsv 140.0 30.0 40.0)"

# **못 잰 것을 판정하지 않는다.** 표본이 모자라면 회차 중간의 한 순간을 천장 원인으로 적게 된다.
GATEWAY_CPUS=2 run_case "표본이 셋 미만이면 판정 불가" 2 "판정 불가" \
    -- "$(samples short.tsv 190.0 30.0 40.0 2)"
# 표본 수는 대마다 따로 본다. 호스트 표본이 충분해도 한 대가 모자라면 못 잰다.
GATEWAY_CPUS=2 run_case "게이트웨이 표본만 셋 미만이어도 판정 불가" 2 "판정 불가" \
    -- "$(fixture gw_short.tsv "$(printf 'cpu\tload-gateway-1\t190.0\nidle\thost\t40.0\n'
        printf 'cpu\tload-gateway-1\t190.0\nidle\thost\t40.0\n'
        for _ in 1 2 3; do printf 'idle\thost\t40.0\n'; done)")"
# 숫자가 아닌 표본 하나를 빼고 평균하면 남은 표본이 조용히 판정한다.
GATEWAY_CPUS=2 run_case "숫자가 아닌 표본이 하나만 섞여도 판정 불가" 2 "판정 불가" \
    -- "$(fixture one_nan.tsv "$(for _ in 1 2 3 4; do printf 'cpu\tload-gateway-1\t190.0\nidle\thost\t40.0\n'; done
        printf 'cpu\tload-gateway-1\t--\nidle\thost\t40.0\n')")"
GATEWAY_CPUS=2 run_case "게이트웨이 표본이 없으면 판정 불가" 2 "판정 불가" \
    -- "$(fixture nogw.tsv "$(printf 'idle\thost\t40.0\nidle\thost\t40.0\nidle\thost\t40.0\n')")"
GATEWAY_CPUS=2 run_case "숫자가 아니면 판정 불가" 2 "판정 불가" \
    -- "$(fixture nan.tsv "$(for _ in 1 2 3; do printf 'cpu\tload-gateway-1\t--\nidle\thost\t40.0\n'; done)")"
GATEWAY_CPUS=2 run_case "파일이 없으면 판정 불가" 2 "판정 불가" -- "$work/없는파일.tsv"
GATEWAY_CPUS=2 run_case "인자가 없으면 판정 불가" 2 "판정 불가" --
# **기준도 확인한다.** 오타 하나가 awk 에서 0 이 되면 모든 대가 붙은 것으로 나와 게이트웨이 천장이 된다.
SATURATION_PCT=abc GATEWAY_CPUS=2 run_case "붙음 선이 수가 아니면 판정 불가" 2 "판정 불가" \
    -- "$(samples bad_sat.tsv 100.0 30.0 40.0)"
HOST_IDLE_FLOOR_PCT=abc GATEWAY_CPUS=2 run_case "마름 선이 수가 아니면 판정 불가" 2 "판정 불가" \
    -- "$(samples bad_floor.tsv 100.0 30.0 40.0)"
MIN_SAMPLES=0 GATEWAY_CPUS=2 run_case "최소 표본이 양의 정수가 아니면 판정 불가" 2 "판정 불가" \
    -- "$(samples bad_min.tsv 100.0 30.0 40.0)"
GATEWAYS=0 GATEWAY_CPUS=2 run_case "기대 대수가 양의 정수가 아니면 판정 불가" 2 "판정 불가" \
    -- "$(samples bad_gw.tsv 190.0 30.0 40.0)"
GATEWAY_CPUS=0 run_case "코어 한도가 양수가 아니면 판정 불가" 2 "판정 불가" \
    -- "$(samples badcpu.tsv 190.0 30.0 40.0)"

[ "$selftest_failed" = 0 ] && echo "천장 원인 자기검증 통과"
exit "$selftest_failed"

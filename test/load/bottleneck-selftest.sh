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

# **계기를 못 읽은 회차는 호스트 탓이 아니다.** 표집기가 유휴를 못 읽으면 NA 를 내는데,
# 그것이 0 으로 읽히면 첫 규칙(유휴 바닥)에 걸려 모든 회차가 호스트 탓으로 기록된다.
# **손으로 적은 NA 는 생산자와 소비자를 안 잇는다.** 진짜 표집기를 태워 그 값이 판정
# 불가로 읽히는지 본다 — 표집기가 예전처럼 0.0 을 내면 이 사례가 "호스트" 로 갈린다.
. test/load/peak-lib.sh || exit 2
na_work=$(mktemp -d) || exit 1
printf 'cpu  100 0 100 800 0 0 0 0\n' > "$na_work/stat"
# 같은 파일을 두 번 읽으면 차가 0 이라 델타 경로가 NA 를 낸다 — 표집기 전체를 태운다.
produced=$(PEAK_STAT=$na_work/stat PEAK_IDLE_WAIT=0 peak_host_idle_pct)
rm -rf "$na_work"
GATEWAY_CPUS=2 run_case "표집기가 못 읽은 유휴는 판정 불가" 2 "판정 불가" \
    -- "$(samples na 95.0 30.0 "$produced")"

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

# **가운데 값만 찍으면 뒤늦게 붙은 자리가 안 보인다.** 유입이 천장에 닿는 칸은 회차 뒤쪽에서만 붙는다.
late=$(fixture late.tsv "$(for _ in 1 2 3; do printf 'cpu\tload-gateway-1\t100.0\ncpu\tload-redis-1\t20.0\nidle\thost\t60.0\n'; done
    for _ in 1 2; do printf 'cpu\tload-gateway-1\t200.0\ncpu\tload-redis-1\t20.0\nidle\thost\t12.0\n'; done)")
GATEWAY_CPUS=2 run_case "붙음은 가운데 값으로 가르되 최댓값을 같이 적는다" 0 "가운데 100.0% (최대 200.0%" -- "$late"
GATEWAY_CPUS=2 run_case "호스트는 가장 낮은 유휴를 같이 적는다" 0 "유휴 가운데 60.0% (최저 12.0%" -- "$late"

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

# **LB 를 지나는 회차는 LB 도 본다.** 앞단이 코어에 붙은 칸을 게이트웨이 천장으로 적으면 증설 효율이 LB 를 나눈다.
lb_samples() {
    local name=$1 gateway=$2 lb=$3
    fixture "$name" "$(for _ in 1 2 3 4 5; do
        printf 'cpu\tload-gateway-1\t%s\ncpu\tload-redis-1\t30.0\ncpu\tload-lb-1\t%s\nidle\thost\t40.0\n' "$gateway" "$lb"
    done)"
}
LB_CPUS=2 GATEWAY_CPUS=2 run_case "LB 가 한도에 붙었으면 LB" 0 "원인: LB" -- "$(lb_samples lb_sat.tsv 190.0 190.0)"
LB_CPUS=2 GATEWAY_CPUS=2 run_case "LB 90% 바로 아래면 LB 가 아니다" 0 "원인: 게이트웨이" -- "$(lb_samples lb_below.tsv 190.0 179.0)"
LB_CPUS=2 GATEWAY_CPUS=2 run_case "LB 를 지나는데 LB 표본이 없으면 판정 불가" 2 "판정 불가" \
    -- "$(samples lb_missing.tsv 190.0 30.0 40.0)"
LB_CPUS=abc GATEWAY_CPUS=2 run_case "LB 코어 한도가 양수가 아니면 판정 불가" 2 "판정 불가" -- "$(lb_samples lb_bad.tsv 190.0 50.0)"
# LB 선은 LB 코어로 잡는다. 게이트웨이 코어로 잡으면 게이트웨이가 4 코어일 때 LB 가 붙어도 못 본다.
LB_CPUS=2 GATEWAY_CPUS=4 run_case "LB 선은 게이트웨이 코어가 아니라 LB 코어로 잡는다" 0 "원인: LB" \
    -- "$(lb_samples lb_gw4.tsv 300.0 190.0)"
LB_CPUS=2 GATEWAY_CPUS=2 run_case "LB 90% 정확히는 LB" 0 "원인: LB" -- "$(lb_samples lb_edge.tsv 190.0 180.0)"
LB_CPUS=0 GATEWAY_CPUS=2 run_case "LB 코어 한도 0 은 판정 불가" 2 "판정 불가" -- "$(lb_samples lb_zero.tsv 190.0 50.0)"
LB_CPUS=2 GATEWAY_CPUS=2 run_case "LB 표본이 셋 미만이면 판정 불가" 2 "판정 불가" \
    -- "$(fixture lb_short.tsv "$(for i in 1 2 3 4 5; do
        printf 'cpu\tload-gateway-1\t190.0\ncpu\tload-redis-1\t30.0\nidle\thost\t40.0\n'
        [ "$i" -le 2 ] && printf 'cpu\tload-lb-1\t50.0\n'
    done)")"
# **이름은 서비스 자리로만 가른다.** 프로젝트 이름에 lb 나 gateway 가 들어가면 뒷단이 LB 나 게이트웨이로 세어진다.
LB_CPUS=2 GATEWAY_CPUS=2 run_case "프로젝트 이름의 lb 는 LB 표본이 아니다" 0 "원인: 게이트웨이" \
    -- "$(fixture lb_name.tsv "$(for _ in 1 2 3 4 5; do
        printf 'cpu\tscale-lb-x-gateway-1\t190.0\ncpu\tscale-lb-x-redis-1\t30.0\ncpu\tscale-lb-x-lb-1\t50.0\n'
        printf 'cpu\tscale-lb-x-backend-1\t195.0\ncpu\tscale-lb-x-seeder-1\t195.0\nidle\thost\t40.0\n'
    done)")"
GATEWAY_CPUS=2 run_case "프로젝트 이름의 gateway 는 게이트웨이 표본이 아니다" 0 "원인: 게이트웨이" \
    -- "$(fixture gw_name.tsv "$(for _ in 1 2 3 4 5; do
        printf 'cpu\tgateway-test-gateway-1\t190.0\ncpu\tgateway-test-redis-1\t30.0\ncpu\tgateway-test-backend-1\t10.0\nidle\thost\t40.0\n'
    done)")"

# **LB 는 CPU 말고 연결 한도에서도 막힌다.** 러너가 회차 중 nginx 가 낸 연결 한도·accept 실패 줄 수를 넘긴다.
LB_CONN_ERRORS=3 LB_CPUS=2 GATEWAY_CPUS=2 run_case "LB 연결 오류가 있으면 CPU 가 한가해도 LB" 0 "원인: LB" \
    -- "$(lb_samples lb_conn.tsv 190.0 60.0)"
LB_CONN_ERRORS=0 LB_CPUS=2 GATEWAY_CPUS=2 run_case "LB 연결 오류가 0 이면 LB 가 아니다" 0 "원인: 게이트웨이" \
    -- "$(lb_samples lb_conn0.tsv 190.0 60.0)"
LB_CONN_ERRORS=abc LB_CPUS=2 GATEWAY_CPUS=2 run_case "LB 연결 오류 수가 정수가 아니면 판정 불가" 2 "판정 불가" \
    -- "$(lb_samples lb_conn_bad.tsv 190.0 60.0)"
GATEWAY_CPUS=2 run_case "프로젝트 이름의 redis 는 레디스 표본이 아니다" 0 "원인: 게이트웨이" \
    -- "$(fixture redis_name.tsv "$(for _ in 1 2 3 4 5; do
        printf 'cpu\tredis-lab-gateway-1\t190.0\ncpu\tredis-lab-redis-1\t30.0\ncpu\tredis-lab-backend-1\t99.0\nidle\thost\t40.0\n'
    done)")"

# 호스트가 먼저다. 호스트가 말랐으면 LB 가 붙은 것도 k6 와 코어를 다툰 결과일 수 있다.
host_lb=$(fixture host_lb.tsv "$(for _ in 1 2 3 4 5; do
    printf 'cpu\tload-gateway-1\t190.0\ncpu\tload-redis-1\t30.0\ncpu\tload-lb-1\t195.0\nidle\thost\t5.0\n'
done)")
LB_CPUS=2 GATEWAY_CPUS=2 run_case "호스트가 마르면 LB 가 붙어도 호스트" 0 "원인: 호스트" -- "$host_lb"
# LB 가 공유 자원인 레디스보다 앞이다 — 앞단이 막히면 레디스까지 부하가 안 간다.
LB_CPUS=2 GATEWAY_CPUS=2 run_case "LB 와 레디스가 둘 다 붙었으면 LB" 0 "원인: LB" \
    -- "$(fixture lb_redis.tsv "$(for _ in 1 2 3 4 5; do
        printf 'cpu\tload-gateway-1\t100.0\ncpu\tload-redis-1\t95.0\ncpu\tload-lb-1\t190.0\nidle\thost\t40.0\n'
    done)")"

# **못 잰 것을 판정하지 않는다.** 표본이 모자라면 회차 중간의 한 순간을 천장 원인으로 적게 된다.
GATEWAY_CPUS=2 run_case "표본이 셋 미만이면 판정 불가" 2 "판정 불가" \
    -- "$(samples short.tsv 190.0 30.0 40.0 2)"
GATEWAY_CPUS=2 run_case "표본이 정확히 셋이면 판정한다" 0 "원인: 게이트웨이" \
    -- "$(samples three.tsv 190.0 30.0 40.0 3)"
GATEWAY_CPUS=2 run_case "호스트 표본만 셋 미만이어도 판정 불가" 2 "판정 불가" \
    -- "$(fixture idle_short.tsv "$(for _ in 1 2 3 4 5; do printf 'cpu\tload-gateway-1\t190.0\ncpu\tload-redis-1\t30.0\n'; done
        printf 'idle\thost\t40.0\nidle\thost\t40.0\n')")"
# 표본 수는 대마다 따로 본다. 호스트 표본이 충분해도 한 대가 모자라면 못 잰다.
GATEWAY_CPUS=2 run_case "게이트웨이 표본만 셋 미만이어도 판정 불가" 2 "판정 불가" \
    -- "$(fixture gw_short.tsv "$(printf 'cpu\tload-gateway-1\t190.0\nidle\thost\t40.0\n'
        printf 'cpu\tload-gateway-1\t190.0\nidle\thost\t40.0\n'
        for _ in 1 2 3; do printf 'idle\thost\t40.0\ncpu\tload-redis-1\t30.0\n'; done)")"
# 숫자가 아닌 표본 하나를 빼고 평균하면 남은 표본이 조용히 판정한다.
GATEWAY_CPUS=2 run_case "숫자가 아닌 표본이 하나만 섞여도 판정 불가" 2 "판정 불가" \
    -- "$(fixture one_nan.tsv "$(for _ in 1 2 3 4; do printf 'cpu\tload-gateway-1\t190.0\ncpu\tload-redis-1\t30.0\nidle\thost\t40.0\n'; done
        printf 'cpu\tload-gateway-1\t--\nidle\thost\t40.0\n')")"
# **레디스도 표본을 요구한다.** 레디스를 먼저 보는데 그 표집이 빠지면 순서가 조용히 사라진다.
GATEWAY_CPUS=2 run_case "레디스 표본이 없으면 판정 불가" 2 "판정 불가" \
    -- "$(fixture noredis.tsv "$(for _ in 1 2 3 4 5; do printf 'cpu\tload-gateway-1\t190.0\nidle\thost\t40.0\n'; done)")"
GATEWAY_CPUS=2 run_case "레디스 표본이 셋 미만이면 판정 불가" 2 "판정 불가" \
    -- "$(fixture redis_short.tsv "$(for _ in 1 2 3 4 5; do printf 'cpu\tload-gateway-1\t100.0\nidle\thost\t40.0\n'; done
        printf 'cpu\tload-redis-1\t95.0\n')")"
GATEWAY_CPUS=2 run_case "게이트웨이 표본이 없으면 판정 불가" 2 "판정 불가" \
    -- "$(fixture nogw.tsv "$(printf 'idle\thost\t40.0\nidle\thost\t40.0\nidle\thost\t40.0\n')")"
GATEWAY_CPUS=2 run_case "숫자가 아니면 판정 불가" 2 "판정 불가" \
    -- "$(fixture nan.tsv "$(for _ in 1 2 3; do printf 'cpu\tload-gateway-1\t--\nidle\thost\t40.0\n'; done)")"
# **값의 범위도 본다.** 유휴 -1 이 호스트 마름으로, 150 이 여유로 읽히면 계기 고장이 원인으로 적힌다.
GATEWAY_CPUS=2 run_case "호스트 유휴가 음수면 판정 불가" 2 "판정 불가" -- "$(samples neg_idle.tsv 190.0 30.0 -1.0)"
GATEWAY_CPUS=2 run_case "호스트 유휴가 100 을 넘으면 판정 불가" 2 "판정 불가" -- "$(samples big_idle.tsv 190.0 30.0 150.0)"
GATEWAY_CPUS=2 run_case "CPU 가 음수면 판정 불가" 2 "판정 불가" -- "$(samples neg_cpu.tsv 190.0 -5.0 40.0)"
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

# **CPU 만으로는 레디스의 천장을 다 못 본다** (CY-990). 명령이 짧으면 한 스레드가 코어를 안 채운 채 초당 명령 수에서
# 먼저 붙고, 네트워크는 CPU 가 한가한 채로 막힌다. 두 축은 천장을 적었을 때만 판정에 쓴다 — 천장은 호스트마다 다르다.
with_axes() {
    local name=$1 ops=$2 gw_net=$3 redis_net=$4
    fixture "$name" "$(for _ in 1 2 3 4 5; do
        printf 'cpu\tload-gateway-1\t120.0\ncpu\tload-redis-1\t40.0\nidle\thost\t40.0\n'
        printf 'ops\tload-redis-1\t%s\nnet\tload-gateway-1\t%s\nnet\tload-redis-1\t%s\n' "$ops" "$gw_net" "$redis_net"
    done)"
}
REDIS_OPS_CEILING=100000 GATEWAY_CPUS=2 run_case "명령 수가 천장에 붙었으면 레디스" 0 "원인: 레디스 — 명령 수" \
    -- "$(with_axes ops.tsv 95000 100 100)"
REDIS_OPS_CEILING=100000 GATEWAY_CPUS=2 run_case "명령 수가 천장 90% 아래면 레디스가 아니다" 0 "원인: 가르지 못함" \
    -- "$(with_axes ops_below.tsv 89000 100 100)"
NET_CEILING_MBPS=1000 GATEWAY_CPUS=2 run_case "게이트웨이 망이 천장에 붙었으면 네트워크" 0 "원인: 네트워크 — load-gateway-1" \
    -- "$(with_axes net_gw.tsv 1000 950 100)"
NET_CEILING_MBPS=1000 GATEWAY_CPUS=2 run_case "레디스 망이 천장에 붙었으면 네트워크" 0 "원인: 네트워크 — load-redis-1" \
    -- "$(with_axes net_redis.tsv 1000 100 950)"
# 공유 자원을 먼저 본다. 명령 수가 붙었으면 망이 같이 찼어도 레디스를 늘려야 풀린다.
REDIS_OPS_CEILING=100000 NET_CEILING_MBPS=1000 GATEWAY_CPUS=2 run_case "명령 수가 망보다 먼저다" 0 \
    "원인: 레디스 — 명령 수" -- "$(with_axes both.tsv 95000 950 950)"
REDIS_OPS_CEILING=100000 GATEWAY_CPUS=2 run_case "호스트가 마르면 명령 수가 붙어도 호스트" 0 \
    "원인: 호스트" -- "$(fixture host_ops.tsv "$(for _ in 1 2 3 4 5; do
        printf 'cpu\tload-gateway-1\t120.0\ncpu\tload-redis-1\t40.0\nidle\thost\t5.0\nops\tload-redis-1\t95000\n'
    done)")"
# 천장을 안 적으면 그 축은 판정에 안 쓴다. 표본은 보여 준다.
GATEWAY_CPUS=2 run_case "천장을 안 적으면 명령 수로 안 가른다" 0 "명령 수 가운데 95000" \
    -- "$(with_axes no_ceiling.tsv 95000 950 950)"
GATEWAY_CPUS=2 run_case "천장을 안 적으면 원인이 바뀌지 않는다" 0 "원인: 가르지 못함" \
    -- "$(with_axes no_ceiling2.tsv 95000 950 950)"
REDIS_OPS_CEILING=100000 GATEWAY_CPUS=2 run_case "천장을 적었는데 명령 수 표본이 없으면 판정 불가" 2 "판정 불가" \
    -- "$(samples no_ops.tsv 120.0 40.0 40.0)"
NET_CEILING_MBPS=1000 GATEWAY_CPUS=2 run_case "천장을 적었는데 망 표본이 없으면 판정 불가" 2 "판정 불가" \
    -- "$(samples no_net.tsv 120.0 40.0 40.0)"
REDIS_OPS_CEILING=abc GATEWAY_CPUS=2 run_case "명령 수 천장이 수가 아니면 판정 불가" 2 "판정 불가" \
    -- "$(with_axes bad_ops.tsv 95000 100 100)"
NET_CEILING_MBPS=0 GATEWAY_CPUS=2 run_case "망 천장이 양수가 아니면 판정 불가" 2 "판정 불가" \
    -- "$(with_axes bad_net.tsv 1000 100 100)"
GATEWAY_CPUS=2 run_case "명령 수가 음수면 판정 불가" 2 "판정 불가" \
    -- "$(with_axes neg_ops.tsv -1 100 100)"

# 경계와 순서. 90% 정확히는 붙은 것이다 — CPU 쪽과 같다.
REDIS_OPS_CEILING=100000 GATEWAY_CPUS=2 run_case "명령 수 90% 정확히는 레디스" 0 "원인: 레디스 — 명령 수" \
    -- "$(with_axes ops_edge.tsv 90000 100 100)"
NET_CEILING_MBPS=1000 GATEWAY_CPUS=2 run_case "망 90% 정확히는 네트워크" 0 "원인: 네트워크" \
    -- "$(with_axes net_edge.tsv 1000 900 100)"
# 망이 게이트웨이보다 먼저다. 게이트웨이가 붙은 채 망도 붙었으면 코어를 늘려도 망에서 막힌다.
NET_CEILING_MBPS=1000 GATEWAY_CPUS=2 run_case "게이트웨이가 붙어도 망이 붙었으면 네트워크" 0 "원인: 네트워크" \
    -- "$(fixture gw_net.tsv "$(for _ in 1 2 3 4 5; do
        printf 'cpu\tload-gateway-1\t190.0\ncpu\tload-redis-1\t40.0\nidle\thost\t40.0\nnet\tload-gateway-1\t950\n'
    done)")"
GATEWAY_CPUS=2 run_case "망 표본이 음수면 판정 불가" 2 "판정 불가" \
    -- "$(with_axes neg_net.tsv 1000 -1 100)"
# **천장을 안 적었으면 망 표본이 모자라도 판정을 안 막는다.** 표집기는 첫 바퀴의 망을 버려 늘 하나 적다.
short_net=$(fixture short_net.tsv "$(for _ in 1 2 3 4 5; do
    printf 'cpu\tload-gateway-1\t190.0\ncpu\tload-redis-1\t40.0\nidle\thost\t40.0\n'
done; printf 'net\tload-lb-1\t5.0\n')")
GATEWAY_CPUS=2 run_case "천장 없이 망 표본이 모자라면 그 망만 뺀다" 0 "원인: 게이트웨이" -- "$short_net"
NET_CEILING_MBPS=1000 GATEWAY_CPUS=2 run_case "천장을 적었는데 망 표본이 모자라면 판정 불가" 2 "판정 불가" \
    -- "$short_net"

[ "$selftest_failed" = 0 ] && echo "천장 원인 자기검증 통과"
exit "$selftest_failed"

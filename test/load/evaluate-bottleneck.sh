#!/usr/bin/env bash
# 천장 원인 (10.7.4).
#
# **천장이 났다는 것과 어디서 났는지는 다르다.** 현재 최대치 러너는 회차가 안 서면 멈추는데, 하네스가
# 못 만든 것과 게이트웨이가 못 버틴 것이 같은 멈춤으로 보인다. 그 둘을 안 가르면 증설 효율(10.7.3)이
# 하네스의 한계를 나눈 수가 된다.
#
# 표본은 회차 동안 초마다 쌓인 `종류<TAB>이름<TAB>값` 이다. CPU 는 한 코어가 100, 호스트는 유휴 백분율.
#
# **원인은 이 순서로 가른다.** 호스트가 말랐으면 게이트웨이가 한도에 붙은 것도 k6 와 코어를 다툰 결과일 수
# 있어 먼저 본다. 그다음 게이트웨이 — 대가 여럿이면 전 대가 붙어야 한다. 한 대만 붙은 것은 고르게 안 나뉜
# 하네스 쪽이다. 그다음 레디스(한 스레드라 한 코어가 한도). 아무도 안 붙었으면 가르지 못한다고 적는다.
set -uo pipefail

# 2 는 계기를 고치라는 뜻이다. 원인을 가른 회차는 원인과 상관없이 0 이다 — 원인이 제품이라는 것이 이
# 판정기의 실패가 아니다.
UNMEASURABLE=2

if [ $# -lt 1 ]; then
    echo "::error title=천장 원인::표본 파일이 필요하다 — 판정 불가"
    exit "$UNMEASURABLE"
fi
samples=$1
if [ ! -s "$samples" ]; then
    echo "::error title=천장 원인::표본 파일이 없거나 비었다: $samples — 판정 불가"
    exit "$UNMEASURABLE"
fi

cpus=${GATEWAY_CPUS:-2}
if ! printf '%s' "$cpus" | grep -Eq '^[0-9]+(\.[0-9]+)?$' \
        || awk -v c="$cpus" 'BEGIN{ exit (c > 0) ? 1 : 0 }'; then
    echo "::error title=천장 원인::게이트웨이 코어 한도가 양수가 아니다: '$cpus' — 판정 불가"
    exit "$UNMEASURABLE"
fi

# **붙었다고 보는 선.** 한도를 정확히 채우는 일은 드물다 — 스케줄러 몫과 표집 간격이 있어 90% 에서 끊는다.
saturation=${SATURATION_PCT:-90}
# **호스트 유휴가 이보다 적으면 말랐다.** k6 가 같은 호스트에서 돌아 이 선 아래서는 유입 자체가 흔들린다.
host_floor=${HOST_IDLE_FLOOR_PCT:-10}
# 회차 중간의 한 순간을 천장 원인으로 적지 않는다.
min_samples=${MIN_SAMPLES:-3}

awk -F '\t' -v cpus="$cpus" -v sat="$saturation" -v floor="$host_floor" -v need="$min_samples" '
    function num(v) { return v ~ /^-?[0-9]+(\.[0-9]+)?$/ }
    $0 ~ /^#/ || NF < 3 { next }
    !num($3) { bad = 1; next }
    $1 == "cpu" && $2 ~ /gateway/ { gw_sum[$2] += $3; gw_n[$2]++; next }
    $1 == "cpu" && $2 ~ /redis/   { redis_sum += $3; redis_n++; next }
    $1 == "idle"                  { idle_sum += $3; idle_n++; next }
    END {
        if (bad) { print "::error title=천장 원인::숫자가 아닌 표본이 있다 — 판정 불가"; exit 2 }
        gateways = 0; saturated = 0; limit = cpus * 100 * sat / 100
        for (g in gw_n) {
            if (gw_n[g] < need) { printf "::error title=천장 원인::%s 표본이 %d 개다 — 판정 불가\n", g, gw_n[g]; exit 2 }
            gateways++
            mean = gw_sum[g] / gw_n[g]
            printf "  %s CPU 평균 %.1f%% (한도 %d 코어, 붙음 선 %.1f%%)\n", g, mean, cpus, limit
            if (mean >= limit) saturated++
        }
        if (gateways == 0) { print "::error title=천장 원인::게이트웨이 표본이 없다 — 판정 불가"; exit 2 }
        if (idle_n < need) { printf "::error title=천장 원인::호스트 표본이 %d 개다 — 판정 불가\n", idle_n; exit 2 }
        idle = idle_sum / idle_n
        printf "  호스트 유휴 평균 %.1f%% (마름 선 %.1f%%)\n", idle, floor
        redis = redis_n > 0 ? redis_sum / redis_n : 0
        if (redis_n > 0) printf "  레디스 CPU 평균 %.1f%%\n", redis

        if (idle < floor)           { print "원인: 호스트 — 하네스와 코어를 다퉈 이 천장은 게이트웨이의 것이 아니다"; exit 0 }
        if (saturated == gateways)  { printf "원인: 게이트웨이 — %d 대 모두 코어 한도에 붙었다\n", gateways; exit 0 }
        if (redis_n > 0 && redis >= sat) { print "원인: 레디스 — 한 스레드가 한 코어에 붙었다"; exit 0 }
        printf "원인: 가르지 못함 — 붙은 자리가 없다 (게이트웨이 %d/%d 대)\n", saturated, gateways
        exit 0
    }
' "$samples"

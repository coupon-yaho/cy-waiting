#!/usr/bin/env bash
# 게이트웨이 증설 효율 (10.7.3 · G10.11).
#
# N 대와 2N 대의 현재 최대치를 견준다. 효율 = 2N 대 천장 ÷ (2 × N 대 천장). 기준은 70% 이상이다.
#
# **두 천장이 모두 게이트웨이의 것일 때만 나눈다.** 한쪽이 하네스 천장(호스트가 말랐거나 원인을 못 가림)이면 그
# 나눗셈은 게이트웨이가 아니라 k6 를 잰다. 원인은 러너가 멈춘 회차의 천장 원인 판정(10.7.4)이 낸다.
#
# **천장은 최대치 판정기(10.7.2)가 낸 현재 최대치다.** 요청 유입으로 재면 하네스가 못 만든 몫까지 천장에 들어간다.
set -uo pipefail

UNMEASURABLE=2
UNDER=1

if [ $# -lt 4 ]; then
    echo "::error title=증설 효율::인자 넷이 필요하다 — N 대 표·원인, 2N 대 표·원인 (판정 불가)"
    exit "$UNMEASURABLE"
fi
one_steps=$1 one_cause=$2 two_steps=$3 two_cause=$4

target=${SCALE_TARGET_PCT:-70}
if ! printf '%s' "$target" | grep -Eq '^[0-9]+(\.[0-9]+)?$' \
        || awk -v t="$target" 'BEGIN{ exit (t > 0 && t <= 100) ? 1 : 0 }'; then
    echo "::error title=증설 효율::기준이 0 초과 100 이하의 백분율이 아니다: '$target' (판정 불가)"
    exit "$UNMEASURABLE"
fi

# 그 표의 천장. **선 칸을 여기서 다시 고르지 않는다** — 최대치 판정기와 갈리면 계획서의 두 수가 다른 천장을
# 말한다. 천장을 못 봤거나 선 칸이 없으면 빈 값이다.
ceiling() {
    PEAK_FLOOR='' PEAK_REQUIRE_CEILING=1 "$(dirname "$0")/evaluate-peak.sh" "$1" 2>/dev/null \
        | awk '/현재 최대치\(실측 유입\)/ { printf "%s", $NF; exit }'
}

# 천장이 게이트웨이의 것인가. 원인 줄을 못 찾으면 모르는 것이다.
gateway_bound() {
    grep -q '^원인: 게이트웨이' "$1" 2>/dev/null
}

for pair in "N:$one_cause" "2N:$two_cause"; do
    label=${pair%%:*}
    file=${pair#*:}
    if ! gateway_bound "$file"; then
        found=$(grep -m1 '^원인' "$file" 2>/dev/null || echo '원인 줄 없음')
        echo "::error title=증설 효율::$label 대 천장이 게이트웨이의 것이 아니다 — $found (판정 불가)"
        exit "$UNMEASURABLE"
    fi
done

one=$(ceiling "$one_steps")
two=$(ceiling "$two_steps")
if [ -z "$one" ] || [ -z "$two" ]; then
    echo "::error title=증설 효율::선 회차가 없는 표가 있다 — N 대 '${one:-없음}', 2N 대 '${two:-없음}' (판정 불가)"
    exit "$UNMEASURABLE"
fi

# **곱으로 견준다.** 나눈 비율을 기준과 견주면 기준 정확히가 부동소수 오차로 한쪽에 떨어진다.
if awk -v one="$one" -v two="$two" -v t="$target" 'BEGIN{
    printf "N 대 천장 %.0f/초 · 2N 대 천장 %.0f/초 · 효율 %.1f%% (기준 %s%%)\n", one, two, 100 * two / (2 * one), t
    exit (two * 100 >= t * 2 * one) ? 0 : 1
}'; then
    echo "판정: 충족"
    exit 0
fi
echo "판정: 미달"
exit "$UNDER"

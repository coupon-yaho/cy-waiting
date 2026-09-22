#!/usr/bin/env bash
# 게이트웨이 증설 효율.
#
# N 대와 2N 대의 현재 최대치를 견준다. 효율 = 2N 대 천장 ÷ (2 × N 대 천장). 기준은 70% 이상이다.
#
# **천장은 구간이다.** 선 칸의 실측과 멈춘 칸의 요청 사이 어딘가에 있어, 효율도 구간으로 낸다. 구간 전체가 기준
# 위면 충족, 전체가 아래면 미달, 걸치면 사다리 간격이 넓어 판정 불가다.
#
# **두 천장이 모두 게이트웨이의 것일 때만 나눈다.** 한쪽이 하네스 천장(호스트가 말랐거나 원인을 못 가림)이면 그
# 나눗셈은 게이트웨이가 아니라 k6 를 잰다. 원인은 러너가 멈춘 칸의 천장 원인 판정이 낸다.
#
# **천장은 최대치 판정기가 낸 현재 최대치다.** 선 칸을 여기서 다시 고르면 두 판정기가 다른 천장을 말한다.
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
# **두 표의 결과 섞임이 같아야 나눈다** (CY-990). 멈춘 칸의 끊긴 몫(%p)이 이만큼 넘게 다르면 두 천장은 다른 일을 잰
# 것이다. 줄 상한에 닿아 싼 거절이 늘면 대당 처리량이 부푼다. 섞임을 같게 두는 방법은 계획서 10.7.3 에 적는다.
mix_tolerance=${MIX_TOLERANCE_PP:-5}
if ! printf '%s' "$mix_tolerance" | grep -Eq '^[0-9]+(\.[0-9]+)?$'; then
    echo "::error title=증설 효율::섞임 허용 차가 수가 아니다: '$mix_tolerance' (판정 불가)"
    exit "$UNMEASURABLE"
fi
# **두 배를 넘는 증설은 없다.** 이 위면 두 표가 다른 일을 잰 것이다 — 결과 섞임이 전체 유입을 따라 바뀌면
# 싼 거절 몫이 늘어 대당 처리량이 부푼다.
ceiling_pct=110

# 원인 줄에서 게이트웨이 천장의 대수를 뽑는다. 게이트웨이 천장이 아니면 빈 값이다.
gateway_count() {
    sed -n 's/^원인: 게이트웨이 — \([0-9][0-9]*\) 대 모두.*/\1/p' "$1" 2>/dev/null | head -n 1
}

for pair in "N:$one_cause" "2N:$two_cause"; do
    label=${pair%%:*}
    file=${pair#*:}
    if [ -z "$(gateway_count "$file")" ]; then
        found=$(grep -m1 '^원인' "$file" 2>/dev/null || echo '원인 줄 없음')
        echo "::error title=증설 효율::$label 대 천장이 게이트웨이의 것이 아니다 — $found (판정 불가)"
        exit "$UNMEASURABLE"
    fi
done
one_n=$(gateway_count "$one_cause")
two_n=$(gateway_count "$two_cause")
if [ "$one_n" -le 0 ] || [ "$two_n" -ne $((one_n * 2)) ]; then
    echo "::error title=증설 효율::대수가 N 과 2N 이 아니다 — ${one_n} 대와 ${two_n} 대 (판정 불가)"
    exit "$UNMEASURABLE"
fi

# 그 표의 천장 구간 `<선 칸 실측> <멈춘 칸 요청>`. 못 읽으면 최대치 판정기의 이유를 넘기고 판정 불가로 끝낸다.
#
# **멈춤의 종류는 판정 불가만 거른다.** 코어가 붙으면 도착도 모자라 최대치 판정기가 하네스 멈춤으로 적는데,
# 어디서 막혔는지는 위의 원인 판정이 이미 갈랐다.
ceiling() {
    local label=$1 table=$2 report value kind stop
    report=$(PEAK_FLOOR='' PEAK_REQUIRE_CEILING=1 "$(dirname "$0")/evaluate-peak.sh" "$table" 2>&1)
    value=$(printf '%s\n' "$report" | awk '/현재 최대치\(실측 유입\)/ { print $NF; exit }')
    kind=$(printf '%s\n' "$report" | sed -n 's/^ *천장의 종류 *//p' | head -n 1)
    stop=$(printf '%s\n' "$kind" | sed -n 's/.*(요청 \([0-9][0-9.]*\)).*/\1/p')
    if [ -z "$value" ] || [ -z "$stop" ]; then
        echo "::error title=증설 효율::$label 대 표에서 천장을 못 읽었다 (판정 불가)" >&2
        printf '%s\n' "$report" | grep -E '::error|^판정|천장의 종류|가장 낮은' | sed 's/^/  /' >&2
        return 1
    fi
    case "$kind" in
        '판정 불가'*)
            echo "::error title=증설 효율::$label 대 표가 못 잰 칸에서 멈췄다 — $kind (판정 불가)" >&2
            return 1 ;;
    esac
    printf '%s %s' "$value" "$stop"
}

# **원인 파일은 멈춘 칸의 것이어야 한다.** 판정기에만 있는 조건(응답 기준)이 멈춘 칸을 앞당기면 러너가 남긴 원인은
# 더 높은 칸의 것이다.
same_step() {
    local label=$1 file=$2 stop=$3 step
    step=$(sed -n 's/^멈춘 칸: *\([0-9][0-9.]*\).*/\1/p' "$file" 2>/dev/null | head -n 1)
    if [ -z "$step" ] || awk -v a="$step" -v b="$stop" 'BEGIN{ exit (a == b) ? 1 : 0 }'; then
        echo "::error title=증설 효율::$label 대 원인이 멈춘 칸(요청 $stop)의 것이 아니다 — '${step:-칸 없음}' (판정 불가)"
        return 1
    fi
}

one=$(ceiling N "$one_steps" 2>&1) || { printf '%s\n' "$one"; exit "$UNMEASURABLE"; }
two=$(ceiling 2N "$two_steps" 2>&1) || { printf '%s\n' "$two"; exit "$UNMEASURABLE"; }
read -r one_lo one_hi <<< "$one"
read -r two_lo two_hi <<< "$two"
same_step N "$one_cause" "$one_hi" || exit "$UNMEASURABLE"
same_step 2N "$two_cause" "$two_hi" || exit "$UNMEASURABLE"

# 그 표의 멈춘 칸의 끊긴 몫. 옛 네 칸 표거나 못 잰 칸이면 빈 값이다.
shed_at() {
    awk -F '\t' -v r="$2" '$1 !~ /^#/ && $1 + 0 == r + 0 { print ($5 == "-" ? "" : $5); exit }' "$1"
}
one_mix=$(shed_at "$one_steps" "$one_hi")
two_mix=$(shed_at "$two_steps" "$two_hi")
if [ -z "$one_mix" ] || [ -z "$two_mix" ]; then
    echo "::error title=증설 효율::멈춘 칸의 끊긴 몫이 없어 두 표의 결과 섞임을 못 견준다 — '${one_mix:-없음}'·'${two_mix:-없음}' (판정 불가)"
    exit "$UNMEASURABLE"
fi
echo "N 대·2N 대 멈춘 칸 끊긴 몫 ${one_mix}%·${two_mix}% (허용 차 ${mix_tolerance}%p)"
# **0.1%p 단위 정수로 견준다.** 뺄셈을 부동소수로 견주면 3.3·8.3 처럼 허용 차 정확히가 넘는 쪽으로 떨어진다.
if awk -v a="$one_mix" -v b="$two_mix" -v t="$mix_tolerance" 'BEGIN{
        d = int(a * 10 + 0.5) - int(b * 10 + 0.5); if (d < 0) d = -d; exit (d > int(t * 10 + 0.5)) ? 0 : 1 }'; then
    echo "::error title=증설 효율::두 표의 결과 섞임이 다르다 — 끊긴 몫 ${one_mix}%·${two_mix}% (판정 불가)"
    exit "$UNMEASURABLE"
fi

# 아래 끝 = 2N 선 칸 ÷ (2 × N 멈춘 칸), 위 끝 = 2N 멈춘 칸 ÷ (2 × N 선 칸).
awk -v a="$one_lo" -v b="$one_hi" -v c="$two_lo" -v d="$two_hi" -v t="$target" 'BEGIN{
    printf "N 대 천장 %.0f~%.0f/초 · 2N 대 천장 %.0f~%.0f/초 · 효율 %.1f%%~%.1f%% (기준 %s%%)\n",
        a, b, c, d, 100 * c / (2 * b), 100 * d / (2 * a), t
}'
# **곱으로 견준다.** 나눈 비율을 기준과 견주면 기준 정확히가 부동소수 오차로 한쪽에 떨어진다.
if awk -v b="$one_hi" -v c="$two_lo" -v m="$ceiling_pct" 'BEGIN{ exit (c * 100 > m * 2 * b) ? 0 : 1 }'; then
    echo "::error title=증설 효율::효율 아래 끝이 ${ceiling_pct}% 를 넘는다 — 두 표의 결과 섞임이 다르다 (판정 불가)"
    exit "$UNMEASURABLE"
fi
if awk -v b="$one_hi" -v c="$two_lo" -v t="$target" 'BEGIN{ exit (c * 100 >= t * 2 * b) ? 0 : 1 }'; then
    echo "판정: 충족"
    exit 0
fi
if awk -v a="$one_lo" -v d="$two_hi" -v t="$target" 'BEGIN{ exit (d * 100 < t * 2 * a) ? 0 : 1 }'; then
    echo "판정: 미달"
    exit "$UNDER"
fi
echo "::error title=증설 효율::효율 구간이 기준을 걸친다 — 사다리 간격을 좁혀 다시 잰다 (판정 불가)"
exit "$UNMEASURABLE"

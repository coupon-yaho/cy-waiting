#!/usr/bin/env bash
# 재분배 판정 — **열화한 대의 유입이 몇 초 만에 줄었는가.**
#
# 표본은 초마다 한 줄이다. 각 줄은 `<초> <열화한 대 도착> <전체 도착>` 이고,
# 열화를 넣은 시각을 0 초로 잡는다. 음수 초는 열화 전이다.
#
#   사용: evaluate-redistribute.sh <표본파일> <열화 후 목표 비율(%)>
#
# 종료 0 충족 · 1 미달 · 2 판정 불가.
#
# **"줄었다" 로는 판정이 안 된다.** 한 건만 줄어도 줄어든 것이라, 그러면
# 어떤 구현도 통과한다. 목표 비율에 **닿았고 거기 머무는** 첫 초를 찾는다.
set -uo pipefail

# 목표에서 이만큼까지는 닿은 것으로 본다(%p). 초당 표본이라 흔들림이 있다.
SLACK="${SLACK:-8}"
# 이만큼 연속으로 목표 안에 있어야 "머문다" 로 본다. 한 초만 보면 흔들림이
# 목표를 스치는 순간을 회복으로 적는다.
SUSTAIN="${SUSTAIN:-3}"
# 이 시간 안에 재분배돼야 한다(초). 게이트가 적은 값과 같아야 한다.
BUDGET_SEC="${BUDGET_SEC:-5}"
# 초당 도착이 이보다 적으면 그 초의 비율은 못 믿는다.
MIN_PER_SEC="${MIN_PER_SEC:-10}"

for setting in SLACK SUSTAIN BUDGET_SEC MIN_PER_SEC; do
    value=$(eval "printf '%s' \"\$$setting\"")
    case "$value" in
        ''|*[!0-9]*) echo "$setting 은 0 이상의 정수여야 한다: '$value'"; exit 2 ;;
    esac
done
[ "$SUSTAIN" -ge 1 ] || { echo "SUSTAIN 은 1 이상이어야 한다"; exit 2; }

samples=${1:-}
target=${2:-}
[ -n "$samples" ] && [ -f "$samples" ] || { echo "표본 파일이 없다: '$samples'"; exit 2; }
case "$target" in
    ''|*[!0-9]*) echo "목표 비율은 0 이상의 정수여야 한다: '$target'"; exit 2 ;;
esac

streak=0
settled=""
usable=0
while read -r second degraded total; do
    case "$second$degraded$total" in
        *[!0-9-]*|'') continue ;;
    esac
    [ "$total" -lt "$MIN_PER_SEC" ] && { streak=0; continue; }
    usable=$(( usable + 1 ))
    share=$(( degraded * 100 / total ))
    gap=$(( share - target ))
    magnitude=${gap#-}
    if [ "$second" -ge 0 ] && [ "$magnitude" -le "$SLACK" ]; then
        streak=$(( streak + 1 ))
        if [ "$streak" -ge "$SUSTAIN" ] && [ -z "$settled" ]; then
            # 연속의 **첫 초**가 닿은 시각이다. 마지막 초를 쓰면 유지 조건의
            # 길이만큼 늘 늦게 적힌다.
            settled=$(( second - SUSTAIN + 1 ))
        fi
    else
        streak=0
    fi
    printf '  %+4ds  열화한 대 %3d%%  (도착 %d/%d)\n' "$second" "$share" "$degraded" "$total"
done < "$samples"

echo
if [ "$usable" -lt "$SUSTAIN" ]; then
    echo "판정 불가 — 쓸 만한 표본이 $usable 초뿐이다. 부하가 안 흘렀다"
    exit 2
fi
if [ -z "$settled" ]; then
    echo "미달 — 목표 ${target}%±${SLACK}%p 에 ${SUSTAIN}초 연속으로 닿은 적이 없다"
    exit 1
fi
echo "재분배까지 ${settled}초 (예산 ${BUDGET_SEC}초)"
if [ "$settled" -gt "$BUDGET_SEC" ]; then
    echo "미달 — 예산 ${BUDGET_SEC}초를 넘었다"
    exit 1
fi
echo "충족"

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
#
# **바꾸기 전이 이미 목표면 충족으로 안 적는다.** 그러면 아무것도 안 바뀐
# 실행이 "재분배까지 0초" 로 나온다. 원래부터 제 몫이 안 가던 배선 결함이나,
# 여유를 낮추는 명령이 조용히 실패한 실행이 정확히 그 모양이다.
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
# **0 이면 0 으로 나뉜다.** 그때 스크립트가 죽으며 종료 1 을 내고, 게이트는 그것을
# 제품 미달로 읽는다 — 하네스 설정 오류가 제품 결함으로 적힌다.
[ "$MIN_PER_SEC" -ge 1 ] || { echo "MIN_PER_SEC 은 1 이상이어야 한다"; exit 2; }

samples=${1:-}
target=${2:-}
[ -n "$samples" ] && [ -f "$samples" ] || { echo "표본 파일이 없다: '$samples'"; exit 2; }
case "$target" in
    ''|*[!0-9]*) echo "목표 비율은 0 이상의 정수여야 한다: '$target'"; exit 2 ;;
esac

streak=0
settled=""
after_usable=0
before_degraded=0
before_total=0
while read -r second degraded total; do
    # **필드를 따로 본다.** 이어 붙여 보면 하나가 빠진 줄(`-1 5`)이 그대로
    # 통과하고, 그 뒤 나눗셈이 0 으로 나뉜다 — 못 잰 것이 그럴듯한 수로 나온다.
    case "${second:-}" in ''|*[!0-9-]*) continue ;; esac
    case "${degraded:-}" in ''|*[!0-9]*) continue ;; esac
    case "${total:-}" in ''|*[!0-9]*) continue ;; esac
    [ "$total" -lt "$MIN_PER_SEC" ] && { streak=0; continue; }
    if [ "$second" -lt 0 ]; then
        before_degraded=$(( before_degraded + degraded ))
        before_total=$(( before_total + total ))
    else
        # **바꾼 뒤의 표본만 센다.** 부하 생성기가 0 초에 죽으면 바꾸기 전
        # 표본만으로 이 수가 차고, 하네스 고장이 제품 미달로 적힌다.
        after_usable=$(( after_usable + 1 ))
    fi
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
if [ "$after_usable" -lt "$SUSTAIN" ]; then
    echo "판정 불가 — 바꾼 뒤 쓸 만한 표본이 $after_usable 초뿐이다. 부하가 안 흘렀다"
    exit 2
fi
if [ -z "$settled" ]; then
    echo "미달 — 목표 ${target}%±${SLACK}%p 에 ${SUSTAIN}초 연속으로 닿은 적이 없다"
    exit 1
fi
# 닿았다면 **무엇이 바뀌어 닿았는지**를 말할 수 있어야 한다. 바꾸기 전 표본이
# 없거나 그때 이미 목표 안이었으면 말할 수 없다.
if [ "$before_total" -lt "$MIN_PER_SEC" ]; then
    echo "판정 불가 — 바꾸기 전 표본이 없다. 무엇이 바뀌어 닿았는지 못 가른다"
    exit 2
fi
before_share=$(( before_degraded * 100 / before_total ))
before_gap=$(( before_share - target ))
before_gap=${before_gap#-}
if [ "$before_gap" -le "$SLACK" ]; then
    echo "판정 불가 — 바꾸기 전부터 ${before_share}% 로 목표 ${target}%±${SLACK}%p 안이다. 바뀐 것이 없다"
    exit 2
fi
echo "바꾸기 전 ${before_share}% · 재분배까지 ${settled}초 (예산 ${BUDGET_SEC}초)"
if [ "$settled" -gt "$BUDGET_SEC" ]; then
    echo "미달 — 예산 ${BUDGET_SEC}초를 넘었다"
    exit 1
fi
echo "충족"

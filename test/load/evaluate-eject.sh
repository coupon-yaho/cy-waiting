#!/usr/bin/env bash
# 배제 판정 — **즉시 실패하는 대의 유입이 끊기는가, 그리고 복귀가 안 튀는가.**
#
# 표본은 초마다 한 줄이고 `<초> <그 대에 닿은 수> <전체 닿은 수>` 다. 고장을
# 넣은 시각과 걷은 시각을 각각 0 초로 잡는다.
#
#   사용: evaluate-eject.sh <고장 표본> <복귀 표본> <정상 몫(%)>
#
# 종료 0 충족 · 1 미달 · 2 판정 불가.
#
# **두 가지를 따로 본다.** 진입은 몫이 0 근처로 가는가이고, 복귀는 몫이
# 정상으로 돌아오되 **한 번에 튀지 않는가** 다. 뒤엣것을 안 보면, 돌아오는
# 순간 전량을 받아 곧바로 다시 빠지는 구현이 진입만으로 통과한다.
set -uo pipefail

# 배제된 대의 몫이 이 아래면 끊긴 것으로 본다(%).
EJECTED_SHARE="${EJECTED_SHARE:-5}"
# 이 시간 안에 끊겨야 한다(초).
EJECT_BUDGET_SEC="${EJECT_BUDGET_SEC:-5}"
# 복귀 몫이 정상에서 이만큼까지는 닿은 것으로 본다(%p).
SLACK="${SLACK:-8}"
# 복귀는 이 시간 안에 끝나야 한다(초). 램프를 태우므로 진입보다 길게 준다.
RECOVER_BUDGET_SEC="${RECOVER_BUDGET_SEC:-75}"
# 이만큼 연속으로 안에 있어야 머문다고 본다.
SUSTAIN="${SUSTAIN:-3}"
# **복귀 중 어느 초도 정상 몫의 이 배를 넘으면 안 된다.** 절벽을 잡는 자리다.
SPIKE_RATIO="${SPIKE_RATIO:-150}"
# 초당 도착이 이보다 적으면 그 초의 비율은 못 믿는다.
MIN_PER_SEC="${MIN_PER_SEC:-10}"
# 믿을 수 있는 초가 이보다 적으면 판정하지 않는다.
MIN_USABLE_SEC="${MIN_USABLE_SEC:-3}"

for setting in EJECTED_SHARE EJECT_BUDGET_SEC SLACK RECOVER_BUDGET_SEC \
        SUSTAIN SPIKE_RATIO MIN_PER_SEC MIN_USABLE_SEC; do
    value=$(eval "printf '%s' \"\$$setting\"")
    case "$value" in
        ''|*[!0-9]*) echo "$setting 은 0 이상의 정수여야 한다: '$value'"; exit 2 ;;
    esac
done
[ "$SUSTAIN" -ge 1 ] || { echo "SUSTAIN 은 1 이상이어야 한다"; exit 2; }
# 0 이면 아래에서 0 으로 나뉘고, 그때 종료 1 이 나가 하네스 설정 오류가
# 제품 미달로 적힌다.
[ "$MIN_PER_SEC" -ge 1 ] || { echo "MIN_PER_SEC 은 1 이상이어야 한다"; exit 2; }
[ "$SPIKE_RATIO" -ge 100 ] || { echo "SPIKE_RATIO 는 100 이상이어야 한다"; exit 2; }

fault=${1:-}
recover=${2:-}
normal=${3:-}
for f in "$fault" "$recover"; do
    [ -n "$f" ] && [ -f "$f" ] || { echo "표본 파일이 없다: '$f'"; exit 2; }
done
case "$normal" in
    ''|*[!0-9]*) echo "정상 몫은 0 이상의 정수여야 한다: '$normal'"; exit 2 ;;
esac
[ "$normal" -ge 1 ] || { echo "정상 몫은 1 이상이어야 한다"; exit 2; }

# 한 초의 비율을 낸다. 못 믿는 초는 빈 문자열이다.
share_at() {
    local file=$1 want=$2 sec broken total
    while read -r sec broken total; do
        [ "$sec" = "$want" ] || continue
        [ "$total" -ge "$MIN_PER_SEC" ] || return 0
        echo $(( broken * 100 / total ))
        return 0
    done < "$file"
}

usable_count() {
    local file=$1 from=$2 sec broken total n=0
    while read -r sec broken total; do
        [ "$sec" -ge "$from" ] || continue
        [ "$total" -ge "$MIN_PER_SEC" ] && n=$(( n + 1 ))
    done < "$file"
    echo "$n"
}

fail=0

echo "── 진입 — 즉시 실패하는 대의 유입이 끊기는가"
before=$(share_at "$fault" -1)
if [ -z "$before" ]; then
    echo "  판정 불가 — 고장 직전 초를 못 믿는다 (초당 도착이 ${MIN_PER_SEC} 미만)"
    exit 2
fi
# **고장 전부터 안 가고 있었으면 아무것도 안 잰 것이다.** 배선이 어긋났거나
# 고장을 넣는 명령이 조용히 실패한 실행이 정확히 이 모양이다.
if [ "$before" -le "$EJECTED_SHARE" ]; then
    echo "  판정 불가 — 고장 전부터 그 대의 몫이 ${before}% 다. 잴 것이 없다"
    exit 2
fi
echo "  고장 직전 몫: ${before}%"

usable=$(usable_count "$fault" 0)
if [ "$usable" -lt "$MIN_USABLE_SEC" ]; then
    echo "  판정 불가 — 고장 뒤 믿을 수 있는 초가 ${usable} 개뿐이다"
    exit 2
fi

cut_at=
run=0
while read -r sec broken total; do
    [ "$sec" -ge 0 ] || continue
    [ "$total" -ge "$MIN_PER_SEC" ] || continue
    if [ $(( broken * 100 / total )) -le "$EJECTED_SHARE" ]; then
        run=$(( run + 1 ))
        [ "$run" -ge "$SUSTAIN" ] && [ -z "$cut_at" ] \
            && cut_at=$(( sec - SUSTAIN + 1 ))
    else
        run=0
    fi
done < "$fault"

if [ -z "$cut_at" ]; then
    echo "  미달 — ${EJECTED_SHARE}% 아래로 ${SUSTAIN}초 머문 적이 없다"
    fail=1
else
    echo "  끊기까지 ${cut_at}초 (예산 ${EJECT_BUDGET_SEC}초)"
    [ "$cut_at" -le "$EJECT_BUDGET_SEC" ] || { echo "  미달 — 예산을 넘었다"; fail=1; }
fi

echo
echo "── 복귀 — 돌아오되 한 번에 튀지 않는가"
usable=$(usable_count "$recover" 0)
if [ "$usable" -lt "$MIN_USABLE_SEC" ]; then
    echo "  판정 불가 — 걷은 뒤 믿을 수 있는 초가 ${usable} 개뿐이다"
    exit 2
fi

low=$(( normal - SLACK ))
[ "$low" -ge 0 ] || low=0
high=$(( normal + SLACK ))
spike=$(( normal * SPIKE_RATIO / 100 ))

back_at=
run=0
worst=0
worst_at=
while read -r sec broken total; do
    [ "$sec" -ge 0 ] || continue
    [ "$total" -ge "$MIN_PER_SEC" ] || continue
    pct=$(( broken * 100 / total ))
    if [ "$pct" -gt "$worst" ]; then worst=$pct; worst_at=$sec; fi
    if [ "$pct" -ge "$low" ] && [ "$pct" -le "$high" ]; then
        run=$(( run + 1 ))
        [ "$run" -ge "$SUSTAIN" ] && [ -z "$back_at" ] \
            && back_at=$(( sec - SUSTAIN + 1 ))
    else
        run=0
    fi
done < "$recover"

echo "  최고 몫: ${worst}% (${worst_at:-?}초) · 절벽 기준 ${spike}%"
if [ "$worst" -gt "$spike" ]; then
    echo "  미달 — 복귀가 절벽이다. 배제 동안 트래픽이 0 이라 그 대가 가장"
    echo "         한가해 보이고, 돌아오는 순간 제 몫을 넘겨 받았다"
    fail=1
fi

if [ -z "$back_at" ]; then
    echo "  미달 — 정상 몫 ${normal}%±${SLACK} 로 ${SUSTAIN}초 머문 적이 없다"
    fail=1
else
    echo "  돌아오기까지 ${back_at}초 (예산 ${RECOVER_BUDGET_SEC}초)"
    [ "$back_at" -le "$RECOVER_BUDGET_SEC" ] \
        || { echo "  미달 — 예산을 넘었다"; fail=1; }
fi

echo
[ "$fail" -eq 0 ] && echo "충족" || echo "미달"
exit "$fail"

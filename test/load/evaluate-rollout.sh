#!/usr/bin/env bash
# 롤링 배포 판정 — **새로 뜬 인스턴스가 처음부터 제 몫을 받지는 않는가.**
#
# 표본은 초마다 한 줄이다. 각 줄은 `<초> <새 인스턴스 도착> <전체 도착>` 이고,
# 새 식별자가 보고를 시작한 시각을 0 초로 잡는다.
#
#   사용: evaluate-rollout.sh <표본파일> <평상시 몫(%)>
#
# 종료 0 충족 · 1 미달 · 2 판정 불가.
#
# **"결국 제 몫을 받는다" 로는 판정이 안 된다.** 램프가 아예 없어도 그건
# 참이다. 처음 몇 초에 **덜 받는지**를 본다 — 찬 인스턴스를 지키는 것이
# 램프의 목적이다.
set -uo pipefail

# 첫 이 시간 동안은 평상시 몫보다 낮아야 한다(초).
EARLY_SEC="${EARLY_SEC:-5}"
# 그 구간의 몫이 평상시의 이 비율 아래라야 "덜 받았다" 로 본다(%).
EARLY_MAX_RATIO="${EARLY_MAX_RATIO:-70}"
# 초당 도착이 이보다 적으면 그 초의 비율은 못 믿는다.
MIN_PER_SEC="${MIN_PER_SEC:-10}"

for setting in EARLY_SEC EARLY_MAX_RATIO MIN_PER_SEC; do
    value=$(eval "printf '%s' \"\$$setting\"")
    case "$value" in
        ''|*[!0-9]*) echo "$setting 은 0 이상의 정수여야 한다: '$value'"; exit 2 ;;
    esac
done
[ "$EARLY_SEC" -ge 1 ] || { echo "EARLY_SEC 은 1 이상이어야 한다"; exit 2; }

samples=${1:-}
steady=${2:-}
[ -n "$samples" ] && [ -f "$samples" ] || { echo "표본 파일이 없다: '$samples'"; exit 2; }
case "$steady" in
    ''|*[!0-9]*) echo "평상시 몫은 0 이상의 정수여야 한다: '$steady'"; exit 2 ;;
esac
[ "$steady" -gt 0 ] || { echo "평상시 몫이 0 이면 견줄 것이 없다"; exit 2; }

early_new=0 early_total=0 usable=0
while read -r second fresh total; do
    case "$second$fresh$total" in
        *[!0-9-]*|'') continue ;;
    esac
    [ "$total" -lt "$MIN_PER_SEC" ] && continue
    usable=$(( usable + 1 ))
    share=$(( fresh * 100 / total ))
    printf '  %+4ds  새 인스턴스 %3d%%  (도착 %d/%d)\n' "$second" "$share" "$fresh" "$total"
    if [ "$second" -ge 0 ] && [ "$second" -lt "$EARLY_SEC" ]; then
        early_new=$(( early_new + fresh ))
        early_total=$(( early_total + total ))
    fi
done < "$samples"

echo
if [ "$early_total" -lt "$MIN_PER_SEC" ]; then
    echo "판정 불가 — 첫 ${EARLY_SEC}초에 쓸 만한 표본이 없다"
    exit 2
fi

early_share=$(( early_new * 100 / early_total ))
limit=$(( steady * EARLY_MAX_RATIO / 100 ))
echo "첫 ${EARLY_SEC}초 몫 ${early_share}% · 평상시 ${steady}% · 한계 ${limit}%"
if [ "$early_share" -gt "$limit" ]; then
    echo "미달 — 찬 인스턴스가 처음부터 제 몫을 받았다"
    exit 1
fi
echo "충족 — 처음에는 덜 받는다"

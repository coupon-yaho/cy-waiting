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
#
# **덜 받는 것만 봐도 안 된다.** 그러면 새 인스턴스가 끝까지 굶는 것이 가장
# 잘 지킨 모습으로 읽힌다 — 램프가 아니라 배제다. 램프 계수가 0 에 고착되거나
# 새 식별자가 후보에 영영 안 들어가는 회귀가 정확히 그 모양이고, 롤링 배포마다
# 새 인스턴스가 무기한 놀아도 게이트는 초록이다. 올라오는 것까지 본다 (TS-8).
set -uo pipefail

# 첫 이 시간 동안은 평상시 몫보다 낮아야 한다(초).
EARLY_SEC="${EARLY_SEC:-5}"
# 그 구간의 몫이 평상시의 이 비율 아래라야 "덜 받았다" 로 본다(%).
EARLY_MAX_RATIO="${EARLY_MAX_RATIO:-70}"
# 이 시각부터를 회복 구간으로 본다(초).
LATE_SEC="${LATE_SEC:-15}"
# 그 구간의 몫이 첫 구간보다 이만큼은 높아야 "올라왔다" 로 본다(%p).
#
# **평상시 몫의 비율로 걸지 않는다.** 램프가 도는 동안의 몫은 평상시의 일부라,
# 거기에 "평상시의 몇 %" 같은 상수를 걸면 램프 길이에 따라 도달 불가가 된다 —
# 실제로 그렇게 걸었다가 정상 실행을 미달로 만들었다 (AIJ-0218).
#
# 오르는 폭으로 보면 램프 길이와 창 길이가 바뀌어도 성립한다. 잡으려는 것은
# 두 가지다 — 끝까지 굶는 것(오름 0)과 올라오다 주저앉는 것(오름 음수).
LATE_RISE_POINTS="${LATE_RISE_POINTS:-5}"
# 초당 도착이 이보다 적으면 그 초의 비율은 못 믿는다.
MIN_PER_SEC="${MIN_PER_SEC:-10}"
# 각 구간에 쓸 만한 초가 이만큼은 있어야 한다. 도착 합계만 보면 한 초가 구간
# 전체를 대표할 수 있어, 끊긴 실행이 "억눌렸다 회복했다" 를 안 보이고 통과한다.
MIN_USABLE_SEC="${MIN_USABLE_SEC:-3}"
# 갈아 끼우기 전 몫이 평상시와 이만큼까지는 벌어져도 된다(%p). 그보다 멀면
# 기준선이 틀린 것이라, 그 위에서 잰 앞뒤 비교는 아무것도 뜻하지 않는다.
BEFORE_SLACK="${BEFORE_SLACK:-10}"

for setting in EARLY_SEC EARLY_MAX_RATIO LATE_SEC LATE_RISE_POINTS MIN_PER_SEC \
        MIN_USABLE_SEC BEFORE_SLACK; do
    value=$(eval "printf '%s' \"\$$setting\"")
    case "$value" in
        ''|*[!0-9]*) echo "$setting 은 0 이상의 정수여야 한다: '$value'"; exit 2 ;;
    esac
done
[ "$EARLY_SEC" -ge 1 ] || { echo "EARLY_SEC 은 1 이상이어야 한다"; exit 2; }
# **0 이면 0 으로 나뉜다.** 그때 스크립트가 죽으며 종료 1 을 내고, 게이트는 그것을
# 제품 미달로 읽는다 — 하네스 설정 오류가 제품 결함으로 적힌다.
[ "$MIN_PER_SEC" -ge 1 ] || { echo "MIN_PER_SEC 은 1 이상이어야 한다"; exit 2; }
[ "$LATE_SEC" -ge "$EARLY_SEC" ] || { echo "LATE_SEC 은 EARLY_SEC 이상이어야 한다"; exit 2; }

samples=${1:-}
steady=${2:-}
[ -n "$samples" ] && [ -f "$samples" ] || { echo "표본 파일이 없다: '$samples'"; exit 2; }
case "$steady" in
    ''|*[!0-9]*) echo "평상시 몫은 0 이상의 정수여야 한다: '$steady'"; exit 2 ;;
esac
[ "$steady" -gt 0 ] || { echo "평상시 몫이 0 이면 견줄 것이 없다"; exit 2; }

early_new=0 early_total=0 early_secs=0 late_new=0 late_total=0 late_secs=0
before_new=0 before_total=0
while read -r second fresh total; do
    # **필드를 따로 본다.** 이어 붙여 보면 하나가 빠진 줄(`-1 5`)이 그대로
    # 통과하고, 그 뒤 나눗셈이 0 으로 나뉜다 — 못 잰 것이 그럴듯한 수로 나온다.
    case "${second:-}" in ''|*[!0-9-]*) continue ;; esac
    case "${fresh:-}" in ''|*[!0-9]*) continue ;; esac
    case "${total:-}" in ''|*[!0-9]*) continue ;; esac
    [ "$total" -lt "$MIN_PER_SEC" ] && continue
    share=$(( fresh * 100 / total ))
    printf '  %+4ds  새 인스턴스 %3d%%  (도착 %d/%d)\n' "$second" "$share" "$fresh" "$total"
    if [ "$second" -lt 0 ]; then
        before_new=$(( before_new + fresh ))
        before_total=$(( before_total + total ))
    fi
    if [ "$second" -ge 0 ] && [ "$second" -lt "$EARLY_SEC" ]; then
        early_new=$(( early_new + fresh ))
        early_total=$(( early_total + total ))
        early_secs=$(( early_secs + 1 ))
    fi
    if [ "$second" -ge "$LATE_SEC" ]; then
        late_new=$(( late_new + fresh ))
        late_total=$(( late_total + total ))
        late_secs=$(( late_secs + 1 ))
    fi
done < "$samples"

echo
# **기준선부터 본다.** 갈아 끼우기 전 그 대가 제 몫을 받고 있었어야, 뒤에 낮아진
# 것이 램프 때문이라고 말할 수 있다. 원래 아무것도 안 받던 대는 0% 에서 5% 로
# 올라도 이 판정을 통과한다 — 그건 롤링 배포를 잰 것이 아니다.
if [ "$before_total" -lt "$MIN_PER_SEC" ]; then
    echo "판정 불가 — 갈아 끼우기 전 표본이 없다. 기준선을 못 세운다"
    exit 2
fi
before_share=$(( before_new * 100 / before_total ))
before_gap=$(( before_share - steady ))
before_gap=${before_gap#-}
if [ "$before_gap" -gt "$BEFORE_SLACK" ]; then
    echo "판정 불가 — 갈아 끼우기 전 몫이 ${before_share}% 로 평상시 ${steady}% 와 ${before_gap}%p 벌어졌다"
    exit 2
fi

if [ "$early_total" -lt "$MIN_PER_SEC" ] || [ "$early_secs" -lt "$MIN_USABLE_SEC" ]; then
    echo "판정 불가 — 첫 ${EARLY_SEC}초에 쓸 만한 초가 ${early_secs}뿐이다"
    exit 2
fi

early_share=$(( early_new * 100 / early_total ))
limit=$(( steady * EARLY_MAX_RATIO / 100 ))
echo "첫 ${EARLY_SEC}초 몫 ${early_share}% · 평상시 ${steady}% · 한계 ${limit}%"
if [ "$early_share" -gt "$limit" ]; then
    echo "미달 — 찬 인스턴스가 처음부터 제 몫을 받았다"
    exit 1
fi

# **회복 구간이 없으면 넘어가지 않는다.** 넘어가면 램프가 올라오는지를 한 번도
# 안 본 실행이 충족으로 적히고, 이 판정이 막으려던 상태가 그것이다. 앞 구간이
# 이미 미달이면 그것으로 끝난다 — 그때는 회복을 볼 것도 없다.
if [ "$late_total" -lt "$MIN_PER_SEC" ] || [ "$late_secs" -lt "$MIN_USABLE_SEC" ]; then
    echo "판정 불가 — ${LATE_SEC}초 이후에 쓸 만한 초가 ${late_secs}뿐이다. 램프가 올라오는지를 못 본다"
    exit 2
fi

late_share=$(( late_new * 100 / late_total ))
floor=$(( early_share + LATE_RISE_POINTS ))
echo "${LATE_SEC}초 이후 몫 ${late_share}% · 첫 ${EARLY_SEC}초보다 ${LATE_RISE_POINTS}%p 이상이라야 한다 (하한 ${floor}%)"
if [ "$late_share" -lt "$floor" ]; then
    echo "미달 — 시간이 지나도 안 올라온다. 램프가 아니라 배제다"
    exit 1
fi
echo "충족 — 처음에는 덜 받고 뒤에는 제 몫을 받는다"

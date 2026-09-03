#!/usr/bin/env bash
# 유입 비율 판정 — **도착 건수가 여유 비율을 따르는가.**
#
# 재는 일(도커·부하)에서 떼어 놓는다. 붙여 두면 판정을 확인하려고 매번 스택을
# 세워야 하고, 그러면 아무도 확인하지 않는다 (TS-9).
#
#   사용: evaluate-routing-ratio.sh <이름:여유:도착> ...
#   예:   evaluate-routing-ratio.sh backend:200:333 backend-small:40:67 backend-mid:120:200
#
# 종료 0 충족 · 1 미달 · 2 판정 불가.
set -uo pipefail

# 허용 편차(%). **게이트가 적은 값과 같아야 한다** (G9.1 은 ±15%). 여기만
# 더 엄하면 기준을 넘긴 구현이 미달로 적히고, 더 헐거우면 못 넘긴 것이
# 통과로 적힌다. 둘 다 조용히 일어난다.
MAX_DEVIATION="${MAX_DEVIATION:-15}"
# 도착이 이만큼도 안 되면 비율을 논할 표본이 아니다. 적은 표본에서는 한두
# 건의 흔들림이 편차 수십 %로 보여, 못 잰 것이 미달로 기록된다.
MIN_TOTAL="${MIN_TOTAL:-100}"

[ $# -ge 2 ] || { echo "뒷단이 둘 이상이어야 비율을 잰다"; exit 2; }

names=() credits=() arrived=()
credit_sum=0 total=0
for spec in "$@"; do
    IFS=: read -r name credit count <<<"$spec"
    case "$credit$count" in
        ''|*[!0-9]*) echo "여유와 도착은 숫자여야 한다: $spec"; exit 2 ;;
    esac
    names+=("$name"); credits+=("$credit"); arrived+=("$count")
    credit_sum=$(( credit_sum + credit ))
    total=$(( total + count ))
done

[ "$credit_sum" -gt 0 ] || { echo "여유 합계가 0 이라 기대값을 못 만든다"; exit 2; }

if [ "$total" -lt "$MIN_TOTAL" ]; then
    echo "판정 불가 — 도착 합계 $total 이 최소 표본 $MIN_TOTAL 에 못 미친다"
    exit 2
fi

printf '%-16s %8s %8s %9s\n' 뒷단 도착 기대 편차
worst=0
for idx in "${!names[@]}"; do
    expected=$(( total * credits[idx] / credit_sum ))
    if [ "$expected" -gt 0 ]; then
        deviation=$(( (arrived[idx] - expected) * 100 / expected ))
    else
        deviation=0
    fi
    magnitude=${deviation#-}
    [ "$magnitude" -gt "$worst" ] && worst=$magnitude
    printf '%-16s %8d %8d %8d%%\n' "${names[idx]}" "${arrived[idx]}" "$expected" "$deviation"
done

echo
if [ "$worst" -gt "$MAX_DEVIATION" ]; then
    echo "미달 — 최대 편차 ${worst}% 가 허용 ${MAX_DEVIATION}% 를 넘는다"
    exit 1
fi
echo "충족 — 최대 편차 ${worst}% (허용 ${MAX_DEVIATION}%)"

#!/usr/bin/env bash
# 두 라우팅 방식의 비교 판정 (9.4).
#
# **도착 비율로는 두 방식을 못 가른다.** 뒷단 셋이 물리적으로 같으면 "선언된
# 비율대로 갔는가" 를 묻게 되는데, 그 숫자를 그대로 재생하는 라운드로빈이
# 정의상 이긴다. 실제 능력이 다를 때 무엇이 벌어지는지가 이 판정의 질문이다.
#
# 그래서 재는 것은 **약한 대에 얼마나 덜 보냈는가** 와 **전체가 얼마나
# 처리했는가** 둘이다.
set -uo pipefail

result=${1:?결과 파일}
summary=${2:?k6 요약 파일}

# 느린 대가 정상 대보다 몇 배 느린가. 하네스가 준 값을 그대로 받는다.
slow_factor=${SLOW_FACTOR:-3}

if [ ! -s "$result" ]; then
    echo "::error title=라우팅 비교::결과가 비었다 — 회차가 안 끝났다"
    exit 1
fi
if [ ! -s "$summary" ]; then
    echo "::error title=라우팅 비교::k6 요약이 없거나 비었다"
    exit 1
fi

# `<처리> <밀어냄> <이름>` 세 칸씩 세 줄이다.
lines=$(grep -c '' "$result")
if [ "$lines" -ne 3 ]; then
    echo "::error title=라우팅 비교::뒷단이 세 줄이어야 한다 (${lines} 줄)"
    exit 1
fi
if cut -d' ' -f1,2 "$result" | tr ' ' '\n' | grep -qvE '^[0-9]+$'; then
    echo "::error title=라우팅 비교::처리·밀어냄에 숫자가 아닌 값이 있다"
    exit 1
fi

slow=$(sed -n '1p' "$result" | cut -d' ' -f1)
fast=$(( $(sed -n '2p' "$result" | cut -d' ' -f1) + $(sed -n '3p' "$result" | cut -d' ' -f1) ))
total=$((slow + fast))

# **한 건도 안 갔으면 잰 것이 없다.** 뒷단이 안 떴거나 부하가 안 닿은 회차를
# "완벽히 나눴다" 로 읽으면, 아무것도 안 한 회차가 판정을 낸다.
min_total=${MIN_TOTAL:-1000}
if [ "$total" -lt "$min_total" ]; then
    echo "::error title=라우팅 비교::처리 합이 ${total} 건이다 (최소 ${min_total}) — 부하가 안 닿았다"
    exit 1
fi

dropped=$(jq -r '(.metrics.dropped_iterations.values.count
    // .metrics.dropped_iterations.count) // 0' "$summary" 2>/dev/null)
case "$dropped" in ''|*[!0-9]*) dropped=0 ;; esac

printf '  %-24s %s\n' "느린 대 처리" "$slow"
printf '  %-24s %s\n' "정상 두 대 처리" "$fast"
printf '  %-24s %s\n' "처리 합" "$total"
printf '  %-24s %s\n' "흘린 회차" "$dropped"
printf '  %-24s %s\n' "제안 합" "$((total + dropped))"

# 느린 대의 몫. 실제 능력비는 1/(1+2×배수) 인데, 능력을 보는 쪽은 그쪽으로
# 가고 못 보는 쪽은 3분의 1 에 머문다.
share=$((slow * 1000 / total))
printf '  %-24s %s.%s%%\n' "느린 대 몫" "$((share / 10))" "$((share % 10))"

# 균등(33.3%)이면 실제 능력을 못 본 것이다. 능력비에 가까우면 본 것이다.
even=333
ideal=$(( 1000 / (1 + 2 * slow_factor) ))
printf '  %-24s %s.%s%% (균등) · %s.%s%% (능력비)\n' "견줄 값" \
    "$((even / 10))" "$((even % 10))" "$((ideal / 10))" "$((ideal % 10))"

# 균등과 능력비의 중간을 넘으면 "능력을 봤다" 로 읽는다.
mid=$(( (even + ideal) / 2 ))
if [ "$share" -lt "$mid" ]; then
    echo "판정: 실제 능력을 본다 — 느린 대 몫이 중간값 $((mid / 10)).$((mid % 10))% 아래다"
    exit 0
fi
echo "판정: 실제 능력을 못 본다 — 느린 대 몫이 균등에 가깝다"
exit 0

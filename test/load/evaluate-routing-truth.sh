#!/usr/bin/env bash
# 두 라우팅 방식의 비교 판정 (9.4).
#
# **도착 비율로는 두 방식을 못 가른다.** 뒷단 셋이 물리적으로 같으면 "선언된
# 비율대로 갔는가" 를 묻게 되는데, 그 숫자를 그대로 재생하는 가중 라운드로빈이
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

# **밀어낸 건수를 무시하면 과부하가 회피로 읽힌다.** 처리 건수만 세면, 약한
# 대에 잔뜩 보내 놓고 그 대가 동시 한도에서 밀어낸 회차가 "적게 보냈다" 로
# 나온다 — 능력을 보고 피한 것과 정반대인데 판정은 같다. 이 회차는 한도를
# 넉넉히 두어 밀어냄이 0 이어야 성립한다.
rejected=$(cut -d' ' -f2 "$result" | awk '{s+=$1} END {print s+0}')
if [ "$rejected" -gt 0 ]; then
    echo "::error title=라우팅 비교::뒷단이 ${rejected} 건을 밀어냈다 — 과부하 회차는 몫을 못 읽는다"
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

# **요약이 깨졌으면 0 으로 읽지 않는다.** `jq` 실패를 삼키고 빈 값을 0 으로
# 바꾸면, 잘린 요약으로도 판정이 난다 — 두 방식이 같은 부하를 받았다는 증거가
# 없는 채로 몫만 비교하게 된다.
if ! dropped=$(jq -er '(.metrics.dropped_iterations.values.count
        // .metrics.dropped_iterations.count) // empty' "$summary" 2>/dev/null); then
    echo "::error title=라우팅 비교::요약에서 흘린 회차를 못 읽었다 — 요약을 먼저 본다"
    exit 1
fi
case "$dropped" in ''|*[!0-9]*)
    echo "::error title=라우팅 비교::흘린 회차가 숫자가 아니다: '$dropped'"
    exit 1 ;;
esac

printf '  %-24s %s\n' "느린 대 처리" "$slow"
printf '  %-24s %s\n' "정상 두 대 처리" "$fast"
printf '  %-24s %s\n' "처리 합" "$total"
# **흘린 회차가 있다고 판정을 멈추지 않는다.** 이 회차에서 흘림은 고장이
# 아니라 **재려는 신호 자체**다 — VU 가 고정인데 뒷단이 느리면 고정 유입
# 실행기가 회차를 못 시작한다. 실제로 두 전략의 흘림이 4,427 대 2,520 으로
# 갈렸고, 그 차이가 곧 지연 차이다. 흘림이 0 이어야 판정한다고 걸면 이
# 하네스는 아무것도 못 잰다.
#
# **몫 계산은 흘림에 안 휘둘린다.** 흘린 회차는 게이트웨이에 닿지도 않아서
# 어느 뒷단으로도 안 갔다. 몫은 실제로 간 것들 사이의 비율이다.
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

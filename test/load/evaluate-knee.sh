#!/usr/bin/env bash
# CPU-지연 무릎 판정 (10.0.5 문턱의 근거).
#
# **무릎을 눈으로 고르지 않는다.** 표를 보고 사람이 짚으면 그 수가 취향이 된다.
# 지연이 저부하 기준선의 몇 배가 되는 지점을 규칙으로 정하고, 그때의 CPU 를
# 낸다 — 그 배수는 예산이 아니라 "꺾였다" 의 정의다.
set -uo pipefail

samples=${1:?표본 파일}
# 기준선 대비 이 배수를 넘으면 꺾인 것으로 본다. 두 배는 "지연이 두 배가 됐다"
# 는 뜻이고, 절대 예산이 없는 자리에서 쓸 수 있는 몇 안 되는 상대 기준이다.
knee_factor=${KNEE_FACTOR:-2}

if [ ! -s "$samples" ]; then
    echo "::error title=무릎 판정::표본이 비었다 — 회차가 안 돌았다"
    exit 1
fi

# 표본은 `<듀티%> <등록/초> <p99ms> <CPU%>` 네 칸이다. 첫 칸이 부하 축이다.
lines=$(grep -c '' "$samples")
min_lines=${MIN_LINES:-4}
if [ "$lines" -lt "$min_lines" ]; then
    echo "::error title=무릎 판정::표본이 ${lines} 줄뿐이다 (최소 ${min_lines}) — 무릎을 못 본다"
    exit 1
fi
if awk '{ if (NF != 4) exit 1; for (i = 2; i <= 4; i++) if ($i !~ /^[0-9.]+$/) exit 1 }' \
        "$samples"; then :; else
    echo "::error title=무릎 판정::표본에 숫자가 아닌 칸이 있다"
    exit 1
fi

# **표가 부하순인지 본다.** 첫 줄을 기준선으로 쓰고 첫 초과에서 멈추는 규칙이
# 전부 그 전제 위에 있다. 역순 표를 주면 가장 느린 줄이 기준선이 되어 무릎이
# 셋 있어도 "못 봤다" 가 나온다 — 조용히 초록이다.
if awk 'NR > 1 && $1 + 0 <= prev { exit 1 } { prev = $1 + 0 }' "$samples"; then :; else
    echo "::error title=무릎 판정::표본이 부하순이 아니다 — 첫 줄을 기준선으로 못 쓴다"
    exit 1
fi

base=$(awk 'NR == 1 { print $3 }' "$samples")
if awk -v b="$base" 'BEGIN { exit (b > 0) ? 0 : 1 }'; then :; else
    echo "::error title=무릎 판정::기준선 지연이 ${base} 다 — 못 잰 것이다"
    exit 1
fi

printf '  %-24s %s ms\n' "기준선 p99 (최저 부하)" "$base"
printf '  %-24s %s 배\n' "꺾였다고 보는 배수" "$knee_factor"

knee=$(awk -v b="$base" -v f="$knee_factor" '$3 > b * f { print $4; exit }' "$samples")
if [ -z "$knee" ]; then
    peak=$(awk '{ if ($4 + 0 > m) m = $4 + 0 } END { printf "%.1f", m }' "$samples")
    echo "판정: 무릎을 못 봤다 — 최고 부하에서도 안 꺾였다 (그때 CPU ${peak}%)"
    # **근거를 못 만든 것은 통과가 아니다.** 0 으로 끝내면 게이트가 초록으로
    # 읽고, 문턱은 여전히 근거 없는 채로 남는다.
    echo "::error title=무릎 판정::부하를 더 올려야 문턱 근거가 나온다"
    exit 1
fi

rate=$(awk -v b="$base" -v f="$knee_factor" '$3 > b * f { print $2; exit }' "$samples")
printf '  %-24s %s\n' "꺾인 지점의 등록/초" "$rate"
echo "판정: 무릎은 CPU ${knee}% 다 — 문턱의 근거로 쓴다"

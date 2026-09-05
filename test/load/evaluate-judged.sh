#!/usr/bin/env bash
# 판정한 요청 비율 (G10.10 의 기준).
#
# **`5xx < 0.1%` 로는 못 잰다.** 이 제품은 끊는 것도 줄 세우는 것도 정상
# 동작이라, 2xx 비율로 재면 보호 장치가 동작할수록 게이트가 빨개진다. 뒤집어도
# 못 쓴다 — 전원을 큐에 넣으면 무너져도 5xx 가 0 이다. Phase 6 이 O-7 로 이미
# 밝힌 것이고, 기준은 거기서 정한 **재료를 갖고 판정한 요청의 비율**이다.
#
# 실패로 세는 것은 게이트웨이가 제 일을 못 한 것뿐이다 — 재료가 없거나 낡아
# 상한을 못 걸고 흘린 것. 그 자리가 `quality="degraded"` 다.
set -uo pipefail

samples=${1:?지표 표본 파일}
target=${JUDGED_TARGET_PCT:-99.9}

if [ ! -s "$samples" ]; then
    echo "::error title=판정 비율::표본이 비었다 — 지표를 못 긁었다"
    exit 1
fi

read_metric() {
    awk -v q="$1" '
        $0 ~ "^waiting_judgement_total\\{" && $0 ~ ("quality=\"" q "\"") {
            print $NF; found = 1
        }
        END { if (!found) print "" }
    ' "$samples"
}

fresh=$(read_metric fresh)
degraded=$(read_metric degraded)

# **없는 것과 0 을 가른다.** 지표를 못 긁은 회차를 "열화가 0" 으로 읽으면
# 아무것도 안 잰 판이 100% 로 나온다.
if [ -z "$fresh" ]; then
    echo "::error title=판정 비율::신선 판정 계수가 없다 — 지표를 먼저 본다"
    exit 1
fi
[ -z "$degraded" ] && degraded=0

if awk -v f="$fresh" -v d="$degraded" \
        'BEGIN{ exit (f ~ /^[0-9.]+$/ && d ~ /^[0-9.]+$/) ? 0 : 1 }'; then :; else
    echo "::error title=판정 비율::계수가 숫자가 아니다: fresh='$fresh' degraded='$degraded'"
    exit 1
fi

total=$(awk -v f="$fresh" -v d="$degraded" 'BEGIN{ printf "%.0f", f + d }')
min_total=${MIN_TOTAL:-1000}
if [ "$total" -lt "$min_total" ]; then
    echo "::error title=판정 비율::판정이 ${total} 건뿐이다 (최소 ${min_total}) — 부하가 안 닿았다"
    exit 1
fi

pct=$(awk -v f="$fresh" -v t="$total" 'BEGIN{ printf "%.4f", f / t * 100 }')
printf '  %-24s %s\n' "재료를 갖고 판정" "$(awk -v f="$fresh" 'BEGIN{printf "%.0f", f}')"
printf '  %-24s %s\n' "재료 없이 흘림" "$(awk -v d="$degraded" 'BEGIN{printf "%.0f", d}')"
printf '  %-24s %s%%\n' "판정한 요청 비율" "$pct"
printf '  %-24s %s%%\n' "기준" "$target"

if awk -v p="$pct" -v t="$target" 'BEGIN{ exit (p >= t) ? 0 : 1 }'; then
    echo "판정: 충족 — 기준 이상이다"
    exit 0
fi
echo "판정: 미달 — 게이트웨이가 제 일을 못 한 요청이 기준을 넘었다"
exit 1

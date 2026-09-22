#!/usr/bin/env bash
# 앞단 LB 기준선이 최대치 회차의 유입을 감당하는가.
#
# **못 감당하면 게이트웨이 수치를 판정하지 않는다.** LB 가 못 받은 부하의 멈춤이 게이트웨이 천장으로 적힌다.
#
# 기준선은 최대치 판정기가 선 칸으로 본 가장 높은 요청 유입이다 — 천장을 못 봤으면 아래 경계로 충분하다. 회차는
# 멈춘 칸까지 센다. 그 칸의 유입을 LB 에 실었으니 LB 가 거기까지 받아야 그 멈춤을 게이트웨이 쪽으로 읽는다.
#
# **멈춤의 원인이 LB 면 감당한 것이 아니다.** 기준선은 CPU 로만 본 것이라, 경유 회차가 LB 의 연결 한도에서 막힌
# 칸을 못 가른다. 회차의 원인 파일을 같이 주면 그것도 본다.
#
#   사용: evaluate-lb-cover.sh <기준선 표> <회차 표> [회차 원인 파일]
#   종료: 0 감당 · 2 못 감당하거나 못 읽음
set -uo pipefail

UNMEASURABLE=2

if [ $# -lt 2 ]; then
    echo "::error title=LB 기준선 감당::표 둘이 필요하다 — 기준선 표, 회차 표 (판정 불가)"
    exit "$UNMEASURABLE"
fi
lb_table=$1 peak_table=$2 peak_cause=${3:-}

for f in "$lb_table" "$peak_table"; do
    if [ ! -s "$f" ]; then
        echo "::error title=LB 기준선 감당::표가 없거나 비었다: $f (판정 불가)"
        exit "$UNMEASURABLE"
    fi
done

# 바닥과 응답 기준은 물려받지 않는다. 최대치 회차에 건 응답 기준이 새면 멀쩡한 기준선 칸이 안 선 칸이 된다.
if [ -n "$peak_cause" ] && grep -q '^원인: LB' "$peak_cause" 2>/dev/null; then
    echo "::error title=LB 기준선 감당::회차가 LB 에서 막혔다 — $(grep -m1 '^원인' "$peak_cause") (판정 불가)"
    exit "$UNMEASURABLE"
fi

report=$(PEAK_FLOOR='' PEAK_LATENCY_P99_MS='' "$(dirname "$0")/evaluate-peak.sh" "$lb_table" 2>&1)
covered=$(printf '%s\n' "$report" | sed -n 's/^ *그때의 요청 유입 *\([0-9][0-9.]*\).*/\1/p' | head -n 1)
if [ -z "$covered" ]; then
    echo "::error title=LB 기준선 감당::기준선에 선 칸이 없다 (판정 불가)"
    printf '%s\n' "$report" | grep -E '::error|^판정' | sed 's/^/  /'
    exit "$UNMEASURABLE"
fi

needed=$(awk -F '\t' '
    $0 ~ /^#/ || NF == 0 { next }
    $1 !~ /^[0-9]+(\.[0-9]+)?$/ { bad = 1; exit }
    { if ($1 > max) max = $1; found = 1 }
    END { if (!bad && found) printf "%s", max }
' "$peak_table")
if [ -z "$needed" ]; then
    echo "::error title=LB 기준선 감당::회차 표에서 요청 유입을 못 읽었다 (판정 불가)"
    exit "$UNMEASURABLE"
fi

echo "  기준선이 선 요청 유입 ${covered}/초 · 회차의 가장 높은 요청 유입 ${needed}/초"
if awk -v c="$covered" -v n="$needed" 'BEGIN{ exit (c >= n) ? 0 : 1 }'; then
    echo "판정: 감당 — 게이트웨이 수치를 판정할 수 있다"
    exit 0
fi
echo "::error title=LB 기준선 감당::기준선이 회차 유입을 못 감당한다 — 게이트웨이 수치를 판정할 수 없다 (판정 불가)"
exit "$UNMEASURABLE"

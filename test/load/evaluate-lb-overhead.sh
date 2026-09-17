#!/usr/bin/env bash
# LB 기준선을 뺀 게이트웨이 오버헤드.
#
# **같은 요청 유입끼리, 둘 다 선 칸만 뺀다.** 칸이 다르면 부하가 다르고, 안 선 칸의 p99 는 못 만든 부하의 값이다.
# 기준은 짓지 않고 기록만 한다.
#
#   사용: evaluate-lb-overhead.sh <LB 경유 회차 표> <기준선 표>
#   종료: 0 짝이 하나 이상 · 2 짝이 없거나 못 읽음
set -uo pipefail

UNMEASURABLE=2

if [ $# -lt 2 ]; then
    echo "::error title=LB 오버헤드::표 둘이 필요하다 — LB 경유 회차 표, 기준선 표 (판정 불가)"
    exit "$UNMEASURABLE"
fi
peak_table=$1 lb_table=$2

tolerance=${PEAK_ARRIVAL_TOLERANCE:-0.95}
if ! printf '%s' "$tolerance" | grep -Eq '^[0-9]+(\.[0-9]+)?$' \
        || ! awk -v t="$tolerance" 'BEGIN{ exit (t > 0 && t <= 1) ? 0 : 1 }'; then
    echo "::error title=LB 오버헤드::허용 오차는 0 초과 1 이하의 수여야 한다: '$tolerance' (판정 불가)"
    exit "$UNMEASURABLE"
fi

for f in "$peak_table" "$lb_table"; do
    if [ ! -s "$f" ]; then
        echo "::error title=LB 오버헤드::표가 없거나 비었다: $f (판정 불가)"
        exit "$UNMEASURABLE"
    fi
done

awk -F '\t' -v t="$tolerance" '
    function num(v) { return v ~ /^[0-9]+(\.[0-9]+)?$/ }
    FNR == 1 { file++ }
    $0 ~ /^#/ || NF == 0 { next }
    NF < 4 || !num($1) || !num($2) || !num($4) {
        printf "::error title=LB 오버헤드::%s 번째 표 %d 번째 줄을 못 읽는다 (판정 불가)\n", file, FNR; bad = 1; exit
    }
    {
        stood = ($3 == "ok" && $2 >= $1 * t)
        if (file == 1) { order[++n] = $1; peak_ok[$1] = stood; peak_p99[$1] = $4 }
        else           { lb_seen[$1] = 1; lb_ok[$1] = stood; lb_p99[$1] = $4 }
    }
    END {
        if (bad) exit 2
        pairs = 0
        for (i = 1; i <= n; i++) {
            r = order[i]
            if (!peak_ok[r])       { printf "  요청 %s/초 · 짝 없음 — 회차 칸이 안 섰다\n", r; continue }
            if (!(r in lb_seen))   { printf "  요청 %s/초 · 짝 없음 — 기준선에 같은 유입이 없다\n", r; continue }
            if (!lb_ok[r])         { printf "  요청 %s/초 · 짝 없음 — 기준선 칸이 안 섰다\n", r; continue }
            printf "  요청 %s/초 · 게이트웨이 p99 %.1f ms · 기준선 p99 %.1f ms · 오버헤드 p99 %.1f ms\n",
                r, peak_p99[r], lb_p99[r], peak_p99[r] - lb_p99[r]
            pairs++
        }
        if (pairs == 0) { print "::error title=LB 오버헤드::같은 유입으로 둘 다 선 칸이 없다 (판정 불가)"; exit 2 }
        print "  분위수끼리 뺀 값이라 한 표본의 값이 아니다 — 기준선 분포가 바뀌면 게이트웨이가 그대로여도 움직인다"
        print "판정: 기록 — 기준을 안 건다"
    }
' "$peak_table" "$lb_table"

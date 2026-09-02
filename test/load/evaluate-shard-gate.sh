#!/usr/bin/env bash
# Phase 10 착수 판정 (10.0.5).
#
# **계산으로 정하지 않는다.** 계획서의 계산은 어느 자리가 위험한지를 가리키는
# 데까지만 쓰고, 착수는 실측이 정한다.
#
# 조건: 피크 ops 가 단일 노드 한계의 60% 를 넘으면 샤딩에 착수한다.
# p99 는 **기록만 한다** — 절대 예산이 아직 없다 (D-L1).
set -uo pipefail

samples=${1:?ops 표본 파일}
summary=${2:-}

# 단일 노드 한계. **가정이다** — 계획서가 적은 80~120K 의 아래쪽을 쓴다.
# 낮게 잡는 쪽이 안전하다: 실제 한계가 더 높으면 착수를 앞당길 뿐이고,
# 높게 잡으면 이미 무너진 상태를 "여유 있다" 로 읽는다.
limit=${REDIS_OPS_LIMIT:-80000}
threshold_pct=${SHARD_THRESHOLD_PCT:-60}

if [ ! -s "$samples" ]; then
    echo "::error title=착수 판정::ops 표본이 비었다 — 프로브가 안 돌았다"
    exit 1
fi

peak=$(sort -n "$samples" | tail -1)
count=$(grep -c '' "$samples")
if ! printf '%s' "$peak" | grep -qE '^[0-9]+$'; then
    echo "::error title=착수 판정::피크가 숫자가 아니다: $peak"
    exit 1
fi

# **판정은 정수 나눗셈으로 안 한다.** 60.00125% 가 60 으로 깎여 경계 바로
# 위가 보류로 읽힌다. 보여 줄 값만 나누고, 비교는 곱으로 한다.
pct=$((peak * 100 / limit))
printf '  %-28s %s\n' "표본 수" "$count"
printf '  %-28s %s\n' "피크 ops/s" "$peak"
printf '  %-28s %s%%\n' "단일 노드 한계 대비" "$pct"
printf '  %-28s %s\n' "가정한 한계 ops/s" "$limit"

# **p99 는 기록만 한다.** 계획서의 기준이 전부 "S=1 대비" 라는 상대값인데,
# S=16 을 할지 정하는 자리에서 "S=1 대비" 는 순환이다 (D-L1).
if [ -n "$summary" ] && [ -s "$summary" ]; then
    p99=$(jq -r '.metrics.http_req_duration.["p(99)"]
        // .metrics.http_req_duration.values["p(95)"] // empty' "$summary" 2>/dev/null)
    printf '  %-28s %s\n' "응답 p99(ms) — 기록만" "${p99:-없음}"
fi

if [ $((peak * 100)) -gt $((limit * threshold_pct)) ]; then
    echo "판정: 착수 — 피크가 한계의 ${threshold_pct}% 를 넘었다"
    exit 0
fi
echo "판정: 보류 — 피크가 한계의 ${threshold_pct}% 안이다. 키 스킴은 그대로 둔다"
exit 0

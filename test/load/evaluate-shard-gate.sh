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

# 표본은 `<비율> <창ms>` 두 칸이다. 첫 칸만 본다 — 창은 아래 해상도 검사가 쓴다.
peak=$(cut -d' ' -f1 "$samples" | sort -n | tail -1)
count=$(grep -c '' "$samples")

# **표본이 적으면 못 잰 것이다.** 한 개로도 판정이 나면, 프로브가 거의 안 돈
# 회차가 그대로 착수를 낸다 — 고장 난 회차의 열 개도 그렇게 통과했다.
min_samples=${MIN_SAMPLES:-20}
if [ "$count" -lt "$min_samples" ]; then
    echo "::error title=착수 판정::표본이 ${count} 개뿐이다 (최소 ${min_samples}) — 프로브를 먼저 본다"
    exit 1
fi
if ! printf '%s' "$peak" | grep -qE '^[0-9]+$'; then
    echo "::error title=착수 판정::피크가 숫자가 아니다: $peak"
    exit 1
fi

# **계기가 고장 나면 늘 "착수" 가 나온다.** 실제로 프로브의 시계가 초를 나노초로
# 읽어 값이 10 억 배로 부푼 채 판정이 그대로 통과했다 — 그 회차는 부하가 아니라
# 계기를 잰 것이다. 한계의 열 배를 넘는 값은 단일 노드가 낼 수 있는 수가 아니다.
if [ "$peak" -gt $(( limit * 10 )) ]; then
    echo "::error title=착수 판정::피크 ${peak} 는 한계 ${limit} 의 열 배를 넘는다 — 계기를 먼저 본다"
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
    # **없으면 없다고 적는다.** 다른 분위수로 대신 채우면 기록의 뜻이 바뀐다 —
    # 나중에 이 줄을 보고 p99 라고 믿는다. 요약의 두 모양을 다 본다.
    p99=$(jq -r '(.metrics.http_req_duration.values["p(99)"]
        // .metrics.http_req_duration["p(99)"]) // empty' "$summary" 2>/dev/null)
    printf '  %-28s %s\n' "응답 p99(ms) — 기록만" "${p99:-없음}"

    # **유입도 같이 남긴다 — 기록만.** 봉우리가 유입을 따라가는데 산출물에
    # 유입이 없으면, 회차마다 다른 값이 나왔을 때 부하가 달랐던 것인지 배선이
    # 달랐던 것인지 나중에 못 가른다. 실제로 88% · 88% · 32% 로 갈린 회차들이
    # 유입 1,656 · 1,089 · 857/s 였다. 문턱으로 걸지 않는다 — 한 러너가 낼 수
    # 있는 유입에 근거가 없어서, 걸면 그 순간 근거 없는 문턱이 판정을 정한다.
    rate=$(jq -r '(.metrics.http_reqs.values.rate
        // .metrics.http_reqs.rate) // empty' "$summary" 2>/dev/null)
    printf '  %-28s %s\n' "유입 req/s — 기록만" "${rate:-없음}"
fi

# **부하가 레디스에 안 닿았으면 잰 것이 없다.** 한산한 쿠폰은 통과 경로에서
# 레디스를 안 친다(불변식 1). 게이트웨이가 앞에서 다 흘려보내면 등록 경로를
# 한 번도 안 밟은 채 제어 평면 몫의 세 자릿수 ops 가 찍히고, 판정기는 그것을
# "여유 있다" 로 읽는다 — 실제로 줄에 선 것이 0 인 회차가 보류를 냈다.
if [ -n "$summary" ] && [ -s "$summary" ]; then
    queued=$(jq -r '(.metrics.queued_responses.values.count
        // .metrics.queued_responses.count) // 0' "$summary" 2>/dev/null)
    case "$queued" in ''|*[!0-9]*) queued=0 ;; esac
    printf '  %-28s %s\n' "줄에 선 건수" "$queued"

    min_queued=${MIN_QUEUED:-1000}
    if [ "$queued" -lt "$min_queued" ]; then
        echo "::error title=착수 판정::줄에 선 것이 ${queued} 건이다 (최소 ${min_queued}) — 부하가 레디스에 안 닿았다"
        exit 1
    fi
fi

if [ $((peak * 100)) -gt $((limit * threshold_pct)) ]; then
    echo "판정: 착수 — 피크가 한계의 ${threshold_pct}% 를 넘었다"
    exit 0
fi
echo "판정: 보류 — 피크가 한계의 ${threshold_pct}% 안이다. 키 스킴은 그대로 둔다"
exit 0

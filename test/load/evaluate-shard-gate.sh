#!/usr/bin/env bash
# Phase 10 착수 판정 (10.0.5).
#
# **계산으로 정하지 않는다.** 계획서의 계산은 어느 자리가 위험한지를 가리키는
# 데까지만 쓰고, 착수는 실측이 정한다.
#
# 조건: **레디스 CPU 사용률**의 봉우리가 60% 를 넘으면 샤딩에 착수한다.
#
# **명령 수로 판정하지 않는다.** `total_commands_processed` 는 Lua 안에서 부른
# 명령까지 센다 — `enqueue.lua` 가 안에서 아홉 번 치므로 등록 한 건이 명령
# 아홉으로 찍힌다(실측 9.4). 그 값을 단순 명령 기준 한계와 비교해 "한계의 88%,
# 착수" 라는 판정이 한 번 나왔는데, 같은 회차의 CPU 는 16% 였다 (AIJ-0235).
#
# **CPU 에는 가정한 한계가 필요 없다.** 레디스의 명령 처리는 한 스레드라 코어
# 하나가 곧 100% 다. 앞의 판정이 기대던 "단일 노드 한계 80,000" 은 근거가
# 없었고, 이제 그 수가 없어도 판정이 선다.
#
# p99 는 **기록만 한다** — 절대 예산이 아직 없다 (D-L1).
set -uo pipefail

samples=${1:?표본 파일}
# **요약도 필수다.** 없으면 줄에 선 건수를 못 읽어 "닿았는지" 검사가 통째로
# 생략되고, 판정만 나온다 — 가드를 넣어 놓고 우회로를 열어 둔 셈이다.
summary=${2:?k6 요약 파일}

# **이 60 은 근거가 없다.** 옛 게이트에서 그대로 승계한 수다 — 거기서는 "가정한
# ops 한계 80,000 대비 60%" 였고, 그 한계가 근거 없다는 것이 드러나 계수를
# CPU 로 바꿨다. 분모는 없앴는데 문턱은 안 옮겼다.
#
# 레디스 CPU 40% 에서 이미 지연이 무너지는 구성도, 75% 에서 멀쩡한 구성도
# 이 게이트는 못 가른다. 지연 예산이 서거나(D-L1) 레디스만 때려 CPU-지연
# 무릎을 찾기 전까지 **이 수를 근거로 인용하지 않는다.**
threshold_pct=${SHARD_THRESHOLD_PCT:-60}

if [ ! -s "$samples" ]; then
    echo "::error title=착수 판정::표본이 비었다 — 프로브가 안 돌았다"
    exit 1
fi
if [ ! -s "$summary" ]; then
    echo "::error title=착수 판정::k6 요약이 없거나 비었다 — 회차가 안 끝났다"
    exit 1
fi

# 표본은 `<CPU 백분율×100> <창ms> <명령/초>` 세 칸이다. 첫 칸이 판정에 쓰인다.
# 둘째 칸은 계기가 고장 났을 때 사람이 부하인지 창인지 가르라고 프로브가 적는
# 것이고, 셋째 칸은 기록이다 — 둘 다 판정에는 안 쓴다.
peak=$(cut -d' ' -f1 "$samples" | sort -n | tail -1)
count=$(grep -c '' "$samples")

# **표본이 적으면 못 잰 것이다.** 한 개로도 판정이 나면, 프로브가 거의 안 돈
# 회차가 그대로 착수를 낸다 — 고장 난 회차의 열 개도 그렇게 통과했다.
min_samples=${MIN_SAMPLES:-20}
if [ "$count" -lt "$min_samples" ]; then
    echo "::error title=착수 판정::표본이 ${count} 개뿐이다 (최소 ${min_samples}) — 프로브를 먼저 본다"
    exit 1
fi

# **한 줄만 깨져도 막는다.** `sort -n` 은 숫자가 아닌 줄을 0 으로 읽으므로,
# 서른 줄 중 하나가 깨져도 봉우리만 보면 조용히 지나간다 — 그 회차는 프로브가
# 중간에 흔들렸다는 뜻이고, 그 사실이 어디에도 안 남는다.
if cut -d' ' -f1 "$samples" | grep -qvE '^[0-9]+$'; then
    echo "::error title=착수 판정::표본에 숫자가 아닌 줄이 있다 — 프로브를 먼저 본다"
    exit 1
fi

# **계기가 고장 나면 늘 "착수" 가 나온다.** 실제로 프로브의 시계가 초를 나노초로
# 읽어 값이 10 억 배로 부푼 채 판정이 그대로 통과했다. 명령 처리는 한 스레드라
# 배경 스레드를 얹어도 코어 몇 개를 넘지 못한다 — 열 배는 계기가 틀린 것이다.
if [ "$peak" -gt 100000 ]; then
    echo "::error title=착수 판정::봉우리 CPU 가 $((peak / 100))% 다 — 계기를 먼저 본다"
    exit 1
fi

printf '  %-28s %s\n' "표본 수" "$count"
printf '  %-28s %s.%02d%%\n' "봉우리 CPU" "$((peak / 100))" "$((peak % 100))"

# 평균은 천장을 셈하는 데 쓴다. 봉우리는 창 하나짜리라 그 창의 등록률을 모른다.
mean=$(cut -d' ' -f1 "$samples" | awk '{s+=$1} END {printf "%d", s/NR}')
printf '  %-28s %s.%02d%%\n' "평균 CPU" "$((mean / 100))" "$((mean % 100))"

# **없으면 없다고 적는다.** 다른 분위수로 대신 채우면 기록의 뜻이 바뀐다 —
# 나중에 이 줄을 보고 p99 라고 믿는다. 요약의 두 모양을 다 본다.
p99=$(jq -r '(.metrics.http_req_duration.values["p(99)"]
    // .metrics.http_req_duration["p(99)"]) // empty' "$summary" 2>/dev/null)
printf '  %-28s %s\n' "응답 p99(ms) — 기록만" "${p99:-없음}"

rate=$(jq -r '(.metrics.http_reqs.values.rate
    // .metrics.http_reqs.rate) // empty' "$summary" 2>/dev/null)
printf '  %-28s %s\n' "유입 req/s — 기록만" "${rate:-없음}"

ops=$(cut -d' ' -f3 "$samples" | sort -n | tail -1)
printf '  %-28s %s\n' "봉우리 명령/초 — 기록만" "${ops:-없음}"

# **부하가 레디스에 안 닿았으면 잰 것이 없다.** 한산한 쿠폰은 통과 경로에서
# 레디스를 안 친다(불변식 1). 게이트웨이가 앞에서 다 흘려보내면 등록 경로를
# 한 번도 안 밟은 채 제어 평면 몫의 CPU 만 찍히고, 판정기는 그것을 "여유 있다"
# 로 읽는다 — 실제로 줄에 선 것이 0 인 회차가 보류를 냈다.
queued=$(jq -r '(.metrics.queued_responses.values.count
    // .metrics.queued_responses.count) // 0' "$summary" 2>/dev/null)
case "$queued" in ''|*[!0-9]*) queued=0 ;; esac
printf '  %-28s %s\n' "줄에 선 건수" "$queued"

# **천장을 같이 낸다 — 기록만.** 등록률을 CPU 사용률로 나누면 코어 하나가
# 낼 수 있는 등록률이 나온다. 판정에는 안 쓴다: 이 하네스가 만드는 부하가
# 계획서의 오픈보다 훨씬 작아 외삽이고, 외삽으로 페이즈를 열지 않는다.
# 다만 **샤딩보다 스크립트가 먼저인지**를 보는 것은 이 수다.
enq=$(jq -r '(.metrics.queued_responses.values.rate
    // .metrics.queued_responses.rate) // empty' "$summary" 2>/dev/null)
if [ -n "$enq" ] && [ "$mean" -gt 0 ]; then
    ceiling=$(awk -v e="$enq" -v m="$mean" 'BEGIN {printf "%d", e / (m / 10000)}')
    printf '  %-28s %s\n' "등록/초 — 기록만" "$(awk -v e="$enq" 'BEGIN {printf "%d", e}')"
    printf '  %-28s %s\n' "코어 하나 천장 — 기록만" "$ceiling"
else
    printf '  %-28s %s\n' "코어 하나 천장 — 기록만" "없음"
fi

min_queued=${MIN_QUEUED:-1000}
if [ "$queued" -lt "$min_queued" ]; then
    echo "::error title=착수 판정::줄에 선 것이 ${queued} 건이다 (최소 ${min_queued}) — 부하가 레디스에 안 닿았다"
    exit 1
fi

# **판정은 정수 나눗셈으로 안 한다.** 봉우리가 백분율×100 이므로 문턱도 같은
# 배율로 올려 비교한다.
if [ "$peak" -gt $((threshold_pct * 100)) ]; then
    echo "판정: 착수 — 봉우리 CPU 가 ${threshold_pct}% 를 넘었다"
    exit 0
fi
echo "판정: 보류 — 봉우리 CPU 가 ${threshold_pct}% 안이다. 키 스킴은 그대로 둔다"
exit 0

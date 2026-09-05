#!/usr/bin/env bash
# CPU-지연 무릎을 찾는다 (10.0.5 의 문턱 근거).
#
# **착수 문턱 60% 에 근거가 없다.** 옛 게이트에서 승계한 수인데, 거기서는
# "가정한 ops 한계 대비 60%" 였고 그 한계가 무너졌다. 분모는 없앴는데 문턱은
# 안 옮겼다 — 레디스 CPU 40% 에서 이미 지연이 무너지는 구성도, 75% 에서
# 멀쩡한 구성도 지금 게이트는 못 가른다.
#
# **지연 예산은 우리가 못 정한다** (D-L1). 그러니 절대값 대신 **무릎**을 찾는다:
# 부하를 올리며 CPU 사용률과 등록 지연을 같이 재고, 지연이 꺾이는 지점의
# CPU 를 문턱으로 쓴다. 그 수는 이 기계와 이 스크립트의 성질이지 가정이 아니다.
#
# **등록 스크립트로 때린다.** 단순 명령으로 재면 우리 워크로드가 아니다 —
# 등록 한 건이 안에서 아홉 번 치므로 비용 구조가 다르다.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1

COMPOSE="docker compose -f test/load/compose.yml -f test/load/compose.pinned.yml"
OUT="${OUT:-redis-knee.txt}"
SEED="${SEED:-20000}"
REQUESTS="${REQUESTS:-40000}"
# 동시 연결을 올리며 잰다. 하나씩이면 왕복이 지배해 무릎이 안 보인다.
LEVELS="${LEVELS:-1 2 4 8 16 32 64 128}"

r() { $COMPOSE exec -T redis redis-cli "$@"; }

$COMPOSE up -d --wait --wait-timeout 120 redis >/dev/null 2>&1 || {
    echo "레디스를 못 세웠다"; exit 2; }

Q='queue:{knee}'; M='maxscore:{knee}'; A='alive:{knee}'; AD='admitted:{knee}'; G='grace:{knee}'
r DEL "$Q" "$M" "$A" "$AD" "$G" >/dev/null
# 줄을 미리 채운다. 빈 줄에서 재면 ZSET 연산이 실제보다 싸다.
r EVAL "for i=1,tonumber(ARGV[1]) do
          redis.call('ZADD', KEYS[1], i, 'seed'..i)
        end return 1" 1 "$Q" "$SEED" >/dev/null

sha=$(r SCRIPT LOAD "$(cat src/main/resources/redis/enqueue.lua)")
[ -n "$sha" ] || { echo "등록 스크립트를 못 올렸다"; exit 2; }

: > "$OUT"
printf '  %-6s %10s %10s %10s\n' "동시" "등록/초" "p99(ms)" "CPU%"
for c in $LEVELS; do
    before=$(r INFO cpu | tr -d '\r' | awk -F: '/^used_cpu_(user|sys)_main_thread:/ {s+=$2} END {print s}')
    t0=$(date +%s.%N)
    # **회원 아이디를 겹치지 않게 준다.** 같은 아이디면 재등록 빠른 경로로 빠져
    # 콜이 셋뿐이라, 재려던 열 콜짜리 경로를 안 밟는다.
    out=$($COMPOSE exec -T redis redis-benchmark -n "$REQUESTS" -c "$c" -q --precision 3 \
        evalsha "$sha" 5 "$Q" "$M" "$A" "$AD" "$G" "__rand_int__" 86400 3600 -1 1800000000 300 2>&1)
    t1=$(date +%s.%N)
    after=$(r INFO cpu | tr -d '\r' | awk -F: '/^used_cpu_(user|sys)_main_thread:/ {s+=$2} END {print s}')

    rps=$(printf '%s' "$out" | grep -oE 'throughput summary: [0-9.]+' | grep -oE '[0-9.]+' | head -1)
    [ -z "$rps" ] && rps=$(printf '%s' "$out" | grep -oE '[0-9.]+ requests per second' | grep -oE '^[0-9.]+')
    p99=$(printf '%s' "$out" | awk '/p99/ {print $(NF-1); exit}')
    [ -z "$p99" ] && p99=$(printf '%s' "$out" | grep -oE 'p99=[0-9.]+' | cut -d= -f2)
    cpu=$(awk -v a="$after" -v b="$before" -v t0="$t0" -v t1="$t1" \
        'BEGIN{printf "%.1f", (a-b)/(t1-t0)*100}')

    printf '  %-6s %10s %10s %10s\n' "$c" "${rps:-?}" "${p99:-?}" "$cpu"
    printf '%s %s %s %s\n' "$c" "${rps:-0}" "${p99:-0}" "$cpu" >> "$OUT"
done

r DEL "$Q" "$M" "$A" "$AD" "$G" >/dev/null
echo
echo "표본은 $OUT 에 있다. 지연이 꺾이는 지점의 CPU 가 문턱 근거다."

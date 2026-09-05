#!/usr/bin/env bash
# CPU-지연 무릎을 찾는다 (10.0.5 문턱의 근거).
#
# **착수 문턱 60% 에 근거가 없다.** 옛 게이트에서 승계한 수인데, 거기서는
# "가정한 ops 한계 대비 60%" 였고 그 한계가 무너졌다. 분모는 없앴는데 문턱은
# 안 옮겼다 — 레디스 CPU 40% 에서 이미 지연이 무너지는 구성도, 75% 에서
# 멀쩡한 구성도 지금 게이트는 못 가른다.
#
# **지연 예산은 우리가 못 정한다** (D-L1). 그러니 절대값 대신 **무릎**을 찾는다:
# 부하를 올리며 CPU 사용률과 등록 지연을 같이 재고, 지연이 꺾이는 지점의
# CPU 를 문턱으로 쓴다. 그 수는 이 기계와 이 스크립트의 성질이지 가정이 아니다.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1

COMPOSE="docker compose -f test/load/compose.yml -f test/load/compose.pinned.yml"
OUT="${OUT:-redis-knee.txt}"
SEED="${SEED:-20000}"
REQUESTS="${REQUESTS:-40000}"
# **부하를 유입률로 올린다. 동시성으로는 못 올린다.**
#
# `redis-benchmark` 에는 유입 제한이 없다 — 늘 최대로 민다. 그래서 동시성을
# 1 로 줘도 코어가 이미 76% 다(실측). 동시성만 올리면 지연이 큐잉으로 정비례해
# 늘 뿐이고 CPU 는 평평하다 — 그 표에서 "무릎" 을 읽으면 포화한 지점을 무릎으로
# 적게 된다. 실제로 첫 시도가 그렇게 나왔다.
#
# 대신 **회차마다 쉬어 유입률을 만든다.** 한 묶음을 보내고 그만큼 쉬면 평균
# 유입이 내려간다. 거친 방법이지만 이 도구로 만들 수 있는 유일한 축이다.
LEVELS="${LEVELS:-5 10 20 40 60 80 100}"
# **등록 상한을 유한값으로 준다.** `-1` 은 상한 없음이라 `GET admitted` 와
# 줄 전체를 훑는 `ZCOUNT` 가 통째로 빠진다. 그런데 제품의 판정기는 그 값을
# 절대 안 낸다 — 늘 유한이다. `-1` 로 재면 여덟 콜짜리 경로를 재게 된다.
CAP="${CAP:-900000}"
# 한 레벨을 몇 묶음으로 나눌지. 많을수록 유입이 고르지만 기동 비용이 는다.
CHUNKS="${CHUNKS:-4}"
# **파이프라인으로 천장을 올린다.** 듀티 100 에서도 한 클라이언트가 내는 부하는
# CPU 50% 에서 멎는다 — 왕복이 지배해 레디스가 놀고 있다. 파이프라인을 키우면
# 그 위로 간다. 우리 제품은 파이프라인을 안 쓰지만, 여기서 재려는 것은 제품의
# 유입 모양이 아니라 **이 스크립트가 코어 하나를 어디까지 채우는가** 다.
PIPELINE="${PIPELINE:-1}"

# **이 하네스로는 CPU 를 50% 위로 못 올린다** (실측).
#
# 듀티를 100 으로 올려도, 파이프라인을 여덟로 키워도 주 스레드가 49~50% 에서
# 멎는다. 처리량은 33K 에서 45K 로 오르는데 CPU 는 그대로다 — 계기가 틀린 것이
# 아니라(독립 확인함) **벤치마크 클라이언트가 레디스와 같은 컨테이너, 같은
# 코어에서 돌아 그쪽이 병목**이다. 코어를 나눠 쓰니 레디스 몫이 절반이다.
#
# 그래서 이 도구로는 무릎을 못 찾는다. 찾으려면 클라이언트를 **다른 코어의
# 다른 컨테이너**에서 돌려야 하고, 그건 이 겹침의 구조를 바꾸는 일이다.
# 지금은 그 한계를 표에 남기는 데까지 한다 — 판정기가 "부하를 더 올려야
# 한다" 로 끊으므로 근거 없는 문턱이 근거 있는 척하지는 않는다.

r() { $COMPOSE exec -T redis redis-cli "$@"; }

$COMPOSE up -d --wait --wait-timeout 120 redis >/dev/null 2>&1 || {
    echo "레디스를 못 세웠다"; exit 2; }

# **배경이 도는지 본다.** 제어 평면이 붙어 있으면 그 몫이 CPU 에 섞인다.
idle=$(r INFO stats | tr -d '\r' | awk -F: '/^instantaneous_ops_per_sec:/ {print $2}')
case "$idle" in ''|*[!0-9]*) idle=0 ;; esac
if [ "$idle" -gt 20 ]; then
    echo "::error title=무릎::유휴 ops 가 ${idle} 다 — 배경이 돌고 있다. 게이트웨이를 내리고 잰다"
    exit 2
fi

Q='queue:{knee}'; M='maxscore:{knee}'; A='alive:{knee}'; AD='admitted:{knee}'; G='grace:{knee}'

sha=$(r SCRIPT LOAD "$(cat src/main/resources/redis/enqueue.lua)")
[ -n "$sha" ] || { echo "등록 스크립트를 못 올렸다"; exit 2; }

# **레벨마다 같은 상태에서 시작한다.** 안 그러면 뒤 레벨이 앞 레벨이 쌓은
# 줄 위에서 돌아, 동시성 효과와 줄 길이 효과가 붙어 무릎이 어느 쪽인지 못
# 가린다. 레벨이 여덟이면 마지막은 첫째의 열여섯 배 위에서 재게 된다.
seed_queue() {
    r DEL "$Q" "$M" "$A" "$AD" "$G" >/dev/null
    r EVAL "for i = 1, tonumber(ARGV[1]) do
              redis.call('ZADD', KEYS[1], i, 'seed' .. i)
            end return 1" 1 "$Q" "$SEED" >/dev/null
}

: > "$OUT"
printf '  %-6s %10s %10s %8s %10s\n' "듀티%" "등록/초" "p99(ms)" "CPU%" "신규등록"
for duty in $LEVELS; do
    seed_queue
    w0=$(date +%s.%N)
    before=$(r INFO cpu | tr -d '\r' | awk -F: '/^used_cpu_(user|sys)_main_thread:/ {s+=$2} END {print s}')
    # **`-r` 이 있어야 `__rand_int__` 가 치환된다.** 없으면 리터럴이 그대로
    # 나가 스무 번째부터 전부 같은 회원이 되고, 재등록 빠른 경로(콜 셋)로
    # 빠진다 — 재려던 열 콜짜리 경로를 한 번도 안 밟는다.
    # 묶음을 보내고 쉬어 유입률을 만든다. 듀티 100 이면 안 쉰다.
    chunk=$((REQUESTS / CHUNKS))
    rest=$(awk -v d="$duty" -v n="$CHUNKS" 'BEGIN{ printf "%.3f", (100 - d) / d / n * 2 }')
    out=""
    i=0
    while [ "$i" -lt "$CHUNKS" ]; do
        out=$($COMPOSE exec -T redis redis-benchmark -n "$chunk" -c 4 -P "$PIPELINE" -r 100000000 --csv \
            evalsha "$sha" 5 "$Q" "$M" "$A" "$AD" "$G" \
            "__rand_int__" 86400 3600 "$CAP" 1800000000 300 2>&1)
        [ "$duty" -lt 100 ] && sleep "$rest"
        i=$((i + 1))
    done
    w1=$(date +%s.%N)
    after=$(r INFO cpu | tr -d '\r' | awk -F: '/^used_cpu_(user|sys)_main_thread:/ {s+=$2} END {print s}')

    # csv 는 헤더 한 줄 + 값 한 줄이다. 값 줄의 2 번째가 rps, 7 번째가 p99 다.
    line=$(printf '%s' "$out" | grep -v '^"test"' | tr -d '"' | tail -1)
    rps=$(printf '%s' "$line" | cut -d, -f2)
    p99=$(printf '%s' "$line" | cut -d, -f7)

    # **신규 등록이 실제로 일어났는지 본다.** 빠른 경로로 빠지면 줄이 안 는다.
    # 이 한 줄이 없어서 위의 `-r` 누락을 못 봤다.
    grown=$(r ZCARD "$Q")
    added=$((grown - SEED))

    # **CPU 창을 요청 수로 나눈다.** 벽시계로 나누면 `docker compose exec` 기동
    # 시간(0.1초)이 분모에 섞이는데, 회차가 짧아질수록 그 몫이 커져 높은
    # 동시성에서 CPU 가 체계적으로 낮게 나온다 — 하필 무릎이 있는 쪽이다.
    # 요청당 CPU 에 초당 등록을 곱하면 창과 무관한 사용률이 된다.
    # **쉰 시간을 포함한 벽시계로 나눈다.** 유입률을 낮춘 효과가 CPU 에 보여야
    # 무릎이 보인다 — 요청당으로 나누면 듀티와 무관하게 같은 값이 나온다.
    cpu=$(awk -v a="$after" -v b="$before" -v t0="$w0" -v t1="$w1" \
        'BEGIN{ d = t1 - t0; if (d > 0) printf "%.1f", (a - b) / d * 100; else print "0" }')

    printf '  %-6s %10s %10s %8s %10s\n' "$duty" "${rps:-?}" "${p99:-?}" "$cpu" "$added"
    printf '%s %s %s %s\n' "$duty" "${rps:-0}" "${p99:-0}" "$cpu" >> "$OUT"

    if [ "$added" -lt $((REQUESTS / 2)) ]; then
        echo "::error title=무릎::신규 등록이 ${added} 건이다 (요청 ${REQUESTS}) — 빠른 경로로 빠졌다"
        exit 2
    fi
done

r DEL "$Q" "$M" "$A" "$AD" "$G" >/dev/null
echo
echo "표본은 $OUT 에 있다. 판정은 test/load/evaluate-knee.sh 가 낸다."

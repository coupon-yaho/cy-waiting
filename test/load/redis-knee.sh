#!/usr/bin/env bash
# CPU-지연 무릎을 찾는다 (10.0.5 문턱의 근거).
#
# **착수 문턱 60% 에 근거가 없다** — 경위는 `plan/10-scale-out.md` 1절이 든다.
#
# 지연 예산은 우리가 못 정하므로(D-L1) 절대값 대신 **무릎**을 찾는다: 부하를
# 올리며 CPU 사용률과 등록 지연을 같이 재고, 지연이 꺾이는 지점의 CPU 를
# 문턱으로 쓴다. 그 수는 이 기계와 이 스크립트의 성질이지 가정이 아니다.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1

COMPOSE="docker compose -f test/load/compose.yml -f test/load/compose.pinned.yml"
OUT="${OUT:-redis-knee.txt}"
# 아래에서 이 파일을 비운다. 환경에서 온 값이라 무엇을 지우는지 보고 간다.
case "$OUT" in
    *.txt) ;;
    *) echo "OUT 은 .txt 여야 한다: '$OUT'"; exit 2 ;;
esac
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

# **이 하네스로는 무릎을 못 찾는다.** 두 가지가 막는다 — 벤치마크가 레디스와
# 같은 코어에서 돌아 CPU 를 절반밖에 못 올리는 것, 그리고 유입 제한이 없어
# 묶음 안의 지연이 늘 포화 값인 것. 실측과 대안은
# `ai/journal/2026/09/AIJ-0241-knee-not-found.md` 가 든다.
#
# 그래도 표는 남긴다. 판정기가 "부하를 더 올려야 한다" 로 끊으므로 근거 없는
# 문턱이 근거 있는 척하지는 않는다.

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
# **끊겨도 키를 남기지 않는다.** 남으면 다음 회차의 유휴 검사와 시드에 섞인다.
trap 'r DEL "$Q" "$M" "$A" "$AD" "$G" >/dev/null 2>&1' EXIT INT TERM

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
    # **묶음마다 모은다.** 덮어쓰면 마지막 묶음 값만 남는다.
    worst=0
    i=0
    while [ "$i" -lt "$CHUNKS" ]; do
        # **실패한 묶음을 넘기지 않는다.** 넘기면 절반만 돈 회차가 멀쩡한 표본을
        # 내고, 그 표가 문턱 근거가 된다.
        bench_rc=0
        out=$($COMPOSE exec -T redis redis-benchmark -n "$chunk" -c 4 -P "$PIPELINE" -r 100000000 --csv \
            evalsha "$sha" 5 "$Q" "$M" "$A" "$AD" "$G" \
            "__rand_int__" 86400 3600 "$CAP" 1800000000 300 2>&1) || bench_rc=$?
        if [ "$bench_rc" -ne 0 ] || ! printf '%s' "$out" | grep -q '^"test"'; then
            echo "::error title=무릎::벤치마크 묶음이 실패했다 — 이 회차로는 못 잰다"
            printf '%s\n' "$out" | tail -3 | sed 's/^/  /'
            exit 2
        fi
        one=$(printf '%s' "$out" | grep -v '^"test"' | tr -d '"' | tail -1 | cut -d, -f7)
        worst=$(awk -v a="$worst" -v b="${one:-0}" 'BEGIN{ print (b > a) ? b : a }')
        [ "$duty" -lt 100 ] && sleep "$rest"
        i=$((i + 1))
    done
    w1=$(date +%s.%N)
    after=$(r INFO cpu | tr -d '\r' | awk -F: '/^used_cpu_(user|sys)_main_thread:/ {s+=$2} END {print s}')

    # **p99 는 묶음 중 최악을 쓴다.** 다만 이 값은 여전히 묶음 **안**의 지연이고,
    # 묶음 안에서 벤치마크는 늘 최대로 민다 — 듀티를 낮춰도 이 수는 안 내려간다.
    # 그래서 CPU 는 유입을 따라가는데 p99 는 안 따라간다. **이 표로는 무릎을
    # 구조적으로 못 본다.** 아래 머리말이 그 한계를 적는다.
    p99=$worst

    # **쉰 시간을 포함한 벽시계로 나눈다.** 유입률을 낮춘 효과가 CPU 에 보여야
    # 하고, 요청당으로 나누면 듀티와 무관하게 같은 값이 나온다.
    cpu=$(awk -v a="$after" -v b="$before" -v t0="$w0" -v t1="$w1" \
        'BEGIN{ d = t1 - t0; if (d > 0) printf "%.1f", (a - b) / d * 100; else print "0" }')

    # **신규 등록이 실제로 일어났는지 본다.** 빠른 경로로 빠지면 줄이 안 는다.
    # 이 한 줄이 없어서 위의 `-r` 누락을 못 봤다.
    grown=$(r ZCARD "$Q")
    added=$((grown - SEED))

    # **등록/초도 벽시계 기준이다.** 벤치마크가 낸 수는 묶음 안의 버스트라
    # 듀티 10 에서 실제 유입의 열 배쯤 된다 — 그 수를 "코어 하나 천장" 으로
    # 인용하면 열 배 어긋난다.
    rps=$(awk -v n="$added" -v t0="$w0" -v t1="$w1" \
        'BEGIN{ d = t1 - t0; if (d > 0) printf "%.0f", n / d; else print "0" }')

    printf '  %-6s %10s %10s %8s %10s\n' "$duty" "$rps" "${p99:-?}" "$cpu" "$added"
    printf '%s %s %s %s\n' "$duty" "$rps" "${p99:-0}" "$cpu" >> "$OUT"

    # **이 지연은 부하 축에 안 걸린다.** 묶음 안에서 벤치마크는 늘 최대로 밀어
    # 듀티와 무관하게 포화 값이 나온다. 그 열로 무릎을 판정하면 잡음이 문턱이
    # 된다 — 판정기는 "기준선의 두 배" 만 보므로 흔들림 한 번이면 답이 난다.
    #
    # 그래서 표를 남기되 **판정에 먹이지 못하게 표시한다.** 유입 제한이 되는
    # 도구로 바꾸기 전에는 이 열이 근거가 될 수 없다.
    saturated=1

    if [ "$added" -lt $((REQUESTS / 2)) ]; then
        echo "::error title=무릎::신규 등록이 ${added} 건이다 (요청 ${REQUESTS}) — 빠른 경로로 빠졌다"
        exit 2
    fi
done

r DEL "$Q" "$M" "$A" "$AD" "$G" >/dev/null
echo
if [ "${saturated:-0}" = 1 ]; then
    # 판정기가 이 표를 못 받게 한다. 사람이 눈으로 보는 것은 막지 않는다.
    printf '# 포화-지연: 이 표의 p99 는 부하 축에 안 걸린다. 판정 근거로 못 쓴다\n' >> "$OUT"
    echo "표본은 $OUT 에 있다. **판정에는 못 쓴다** — p99 가 부하 축에 안 걸린다."
    echo "유입 제한이 되는 도구로 바꾸기 전에는 문턱 근거가 안 나온다."
else
    echo "표본은 $OUT 에 있다. 판정은 test/load/evaluate-knee.sh 가 낸다."
fi

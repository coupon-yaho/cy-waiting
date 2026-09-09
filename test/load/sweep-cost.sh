#!/usr/bin/env bash
# 이탈자 청소 한 회차의 비용을 잰다 (10-scale-out 1절 "샤딩보다 먼저 할 것").
#
# **샤딩보다 먼저 회수할 것이 남았는지를 이 수가 정한다.** 샤딩은 키 형식을 바꾸는
# 일이라 큐가 빈 상태에서만 되고 되돌리기가 비싸다. 단일 코어에서 줄일 수 있는 것이
# 남아 있으면 그쪽이 먼저다.
#
# 재는 것은 **스크립트 한 번의 평균 마이크로초**다. 레디스가 명령별로 누적한 값을
# 그대로 읽는다 — 클라이언트에서 재면 왕복이 섞이고, 그 왕복이 스크립트보다 크다.
#
#   SCRIPT=<경로>   잴 스크립트. 고치기 전과 뒤를 같은 하네스로 재려고 인자로 받는다
#   SEED=20000      큐에 세울 사람 수
#   SCAN=3000       검사 범위 K
#   RUNS=5          평균을 낼 회차 수
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1

COMPOSE="docker compose -f test/load/compose.yml -f test/load/compose.pinned.yml"
SCRIPT="${SCRIPT:-src/main/resources/redis/sweep.lua}"
SEED="${SEED:-20000}"
SCAN="${SCAN:-3000}"
RUNS="${RUNS:-5}"
NOW="${NOW:-1800000000}"
RETENTION="${RETENTION:-3600}"
BUDGET="${BUDGET:-1000}"

[ -f "$SCRIPT" ] || { echo "스크립트가 없다: $SCRIPT"; exit 2; }

r() { $COMPOSE exec -T redis redis-cli "$@"; }

$COMPOSE up -d --wait --wait-timeout 120 redis >/dev/null 2>&1 || {
    echo "레디스를 못 세웠다"; exit 2; }

# **배경이 도는지 본다.** 게이트웨이가 붙어 있으면 그 명령이 같은 계수에 섞여,
# 스크립트가 아니라 남의 부하를 재게 된다.
# **한 박자 쉬고 본다.** 앞 회차가 방금 끝났으면 그 명령이 아직 표본 창에 남아,
# 배경이 없는데도 있다고 읽는다.
sleep 2
idle=$(r INFO stats | tr -d '\r' | awk -F: '/^instantaneous_ops_per_sec:/ {print $2}')
case "$idle" in ''|*[!0-9]*) idle=0 ;; esac
if [ "$idle" -gt 20 ]; then
    echo "유휴 ops 가 ${idle} 다 — 배경이 돌고 있다. 게이트웨이를 내리고 잰다"
    exit 2
fi

Q='queue:{cost}'; A='alive:{cost}'; AD='admitted:{cost}'; G='grace:{cost}'
trap 'r DEL "$Q" "$A" "$AD" "$G" >/dev/null 2>&1' EXIT INT TERM

sha=$(r SCRIPT LOAD "$(cat "$SCRIPT")" | tr -d '\r')
[ -n "$sha" ] || { echo "청소 스크립트를 못 올렸다"; exit 2; }

# **아무도 안 걷히게 세운다.** 걷는 회차는 제거와 기록이 비용을 지배해서 창을 읽는
# 값이 묻힌다. 여기서 재려는 것은 창을 읽고 훑는 쪽이고, 그 회차가 정상 구간의
# 대부분이다 — 이탈자는 드물다.
seed() {
    r DEL "$Q" "$A" "$AD" "$G" >/dev/null
    r EVAL "for i = 1, tonumber(ARGV[1]) do
              redis.call('ZADD', KEYS[1], 1787938822000000 + i, 'm' .. i)
              redis.call('ZADD', KEYS[2], tonumber(ARGV[2]) + 300, 'm' .. i)
            end
            redis.call('SET', KEYS[3], '1787938822000000')
            return 1" 3 "$Q" "$A" "$AD" "$SEED" "$NOW" >/dev/null
}

# 한 회차의 평균 마이크로초. 계수를 비운 뒤의 차분이라 앞 회차가 안 섞인다.
#
# **자리를 세어 읽는다.** 한 줄에 `calls`·`usec`(누계)·`usec_per_call`(평균)이 같이
# 있어 한 칸만 밀려도 회차 수만큼 부푼 수가 나오고, 그 수가 계획서의 판단에 들어간다.
# 그래서 평균 곱하기 회차가 누계와 맞는지 그 자리에서 본다.
#
# **실패한 호출도 좋은 성적을 낸다.** 인자가 틀려 첫 검증에서 죽어도 계수에는 남아
# 45μs 같은 훌륭한 수를 찍는다. 실패 수와 회차 수를 같이 봐야 그것이 안 지나간다.
measured() {
    r INFO commandstats | tr -d '\r' \
        | awk -F'[:,=]' -v c="cmdstat_evalsha" -v want="$RUNS" '
            $1 != c { next }
            {
                calls = $3; usec = $5; per = $7; failed = $11
                if (calls != want) {
                    printf "회차 수가 %s 다 (기대 %s) — 배경이 섞였다\n", calls, want > "/dev/stderr"
                    exit 3
                }
                if (failed != 0) {
                    printf "실패한 호출이 %s 건이다 — 인자를 본다\n", failed > "/dev/stderr"
                    exit 3
                }
                diff = per * calls - usec
                if (diff < 0) { diff = -diff }
                if (diff > usec / 100 + 1) {
                    printf "평균과 누계가 안 맞는다 (%s · %s) — 자리를 잘못 읽었다\n",
                            per, usec > "/dev/stderr"
                    exit 3
                }
                printf "%.0f", per
            }'
}

run_round() {
    remove_front="$1"
    seed
    # **한 번 태워 본다.** 아래 루프는 반환을 버리므로, 인자가 틀려 매번 죽어도
    # 표는 초록이다. 여기서 반환이 네 칸인지 보고 간다.
    probe=$(r EVALSHA "$sha" 4 "$Q" "$G" "$A" "$AD" \
        "$SCAN" "$NOW" "$RETENTION" "$BUDGET" 0 "$remove_front" | wc -l)
    if [ "$probe" -ne 4 ]; then
        echo "청소가 네 칸을 안 돌려준다 — 인자를 본다" >&2
        exit 3
    fi
    seed
    r CONFIG RESETSTAT >/dev/null
    for _ in $(seq 1 "$RUNS"); do
        r EVALSHA "$sha" 4 "$Q" "$G" "$A" "$AD" \
            "$SCAN" "$NOW" "$RETENTION" "$BUDGET" 0 "$remove_front" >/dev/null
    done
    measured
}

printf '스크립트  %s\n' "$SCRIPT"
printf '조건      큐 %s · 검사 범위 %s · %s회 평균(한 회차)\n\n' "$SEED" "$SCAN" "$RUNS"
printf '%-24s %s\n' "회차" "평균(μs)"
printf '%-24s %s\n' "앞줄을 훑는다" "$(run_round 1)"
printf '%-24s %s\n' "앞줄을 접는다" "$(run_round 0)"

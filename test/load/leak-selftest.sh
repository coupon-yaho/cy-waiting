#!/usr/bin/env bash
# 누수 판정의 자기검증 (TS-9).
#
# **수명이 대신 걷어 주는 것과 놓는 자리가 도는 것을 갈라야 한다.** 안 가르면
# 표를 놓는 줄을 통째로 지운 구현도 초록이 뜬다 — 수명이 지나면 어차피 0 이다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1
. test/load/selftest-lib.sh

SELFTEST_JUDGE=$PWD/test/load/evaluate-leak.sh
work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

unset TTL_SEC ZERO_BUDGET_SEC

check() {
    local name=$1 want_rc=$2 want_word=$3 body=$4
    printf '%s\n' "$body" > "$work/s.txt"
    run_case "$name" "$want_rc" "$want_word" -- "$work/s.txt"
}

echo "누수 판정 자기검증"

export TTL_SEC=30

# 부하 중 올라갔다가 부하가 끝나고 1.5초에 0 이 된다.
clean='-1000 390
-500 398
0 250
500 80
1000 12
1500 0
2000 0'

check "예산 안에 돌아오면 충족" 0 "충족" "$clean"

# **수명이 걷어 준 것은 회수가 아니다.** 놓는 줄을 지워도 29 초에 0 이 된다.
ttl_only='-1000 390
-500 398
0 398
5000 398
20000 398
29000 0'
check "수명이 걷어 주면 미달" 1 "예산 안에" "$ttl_only"

# 끝까지 안 내려오면 누수다.
leaked='-1000 390
-500 398
0 398
20000 398
40000 398'
check "안 내려오면 미달" 1 "놓는 자리가 안 돈다" "$leaked"

# **음수는 이중 감소다** (R-8). 0 만 보면 이 갈래가 안 보인다.
negative='-1000 390
-500 -3
0 0
1000 0'
check "음수면 미달" 1 "음수" "$negative"

# 부하 중에도 0 이면 요청이 균형기를 안 탄 것이다. 그것을 "누수 없음" 으로
# 적으면 아무것도 안 지난 회차가 게이트를 통과한다.
check "부하 중에도 0 이면 판정 불가" 2 "요청이 균형기를 안 탔다" '-1000 0
-500 0
0 0
1000 0'

check "부하 중 표본이 없으면 판정 불가" 2 "부하 중 표본이 없다" '0 500
1000 0'
check "부하 뒤 표본이 없으면 판정 불가" 2 "부하 뒤 표본이 없다" '-1000 390
-500 398'
check "칸 수가 다르면 판정 불가" 2 "두 칸" '-1000 390 7
0 0'
check "숫자가 아니면 판정 불가" 2 "숫자가 아닌" '-1000 삼백구십
0 0'

: > "$work/empty"
run_case "표본이 비면 판정 불가" 2 "표본 파일이 비었다" -- "$work/empty"

# **얕은 회차는 누수를 못 잰다.** 한 건이 물렸다 빠진 것으로 "누수 0" 을 적으면
# 부하가 뒷단에 거의 안 닿은 회차가 게이트를 통과한다.
check "얕으면 판정 불가" 2 "이 깊이로는" '-1000 40
-500 45
0 10
1000 0'

# **바닥은 부르는 쪽이 준다.** 조건에서 유도한 값이 안 들어오면, 유입이나 지연을
# 바꾼 회차에서 맞게 도는 구현이 미달로 적힌다.
printf '%s\n' "$clean" > "$work/s.txt"
MIN_PEAK=500 run_case "바닥이 실측보다 높으면 판정 불가" 2 "이 깊이로는" -- "$work/s.txt"

# 예산이 수명 이상이면 둘을 못 가른다 — 그 설정으로는 이 판정이 무의미하다.
printf '%s\n' "$clean" > "$work/s.txt"
ZERO_BUDGET_SEC=30 run_case "예산이 수명 이상이면 판정 불가" 2 "둘을 못 가른다" -- "$work/s.txt"
TTL_SEC=abc run_case "수명이 숫자가 아니면 판정 불가" 2 "TTL_SEC" -- "$work/s.txt"

exit "$selftest_failed"

#!/usr/bin/env bash
# 쏠림 판정의 자기검증 (TS-9).
#
# **이 판정이 기본 전략을 뒤집는 근거가 된다.** 헐거우면 게이트웨이 둘이 같은
# 대로 몰리는 구현에도 초록이 뜨고, 그 초록으로 R-4 를 고치게 된다 — 재 본 적
# 없는 위험과 재 본 위험을 맞바꾸는 그 실수를 판정기가 대신 저지르는 셈이다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1
. test/load/selftest-lib.sh

SELFTEST_JUDGE=$PWD/test/load/evaluate-skew.sh
work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

unset MAX_DEVIATION MAX_SKEW MIN_DEPTH_RATIO

# 여유 50/10/30. 기대 점유는 55.6 / 11.1 / 33.3 이다.
check() {
    local name=$1 want_rc=$2 want_word=$3 samples=$4; shift 4
    printf '%s\n' "$samples" > "$work/s.txt"
    SAMPLES="$work/s.txt" run_case "$name" "$want_rc" "$want_word" -- "$@"
}

echo "쏠림 판정 자기검증"

# 여유대로 흐르고 순간 점유도 기대에 붙는다. 깊이는 여유 합 90 을 넘긴다.
even='50 10 30
55 11 33
50 10 30
45 9 27'

# 게이트웨이 둘이 같은 대로 몰린다. **집계는 그대로 맞는다** — 각자는 여유대로
# 나눴고, 순간마다 몰리는 대가 돌아가며 바뀌기 때문이다. 이것이 이 판정이
# 잡아야 하는 유일한 모양이다.
herd='90 0 0
0 90 0
0 0 90
90 0 0'

check "고르게 흐르면 통과" 0 "집계 편차" "$even" \
    backend:50:5000 backend-small:10:1000 backend-mid:30:3000

MAX_SKEW=40 check "집계가 맞아도 순간이 몰리면 잡는다" 1 "상위 5% 초과" "$herd" \
    backend:50:5000 backend-small:10:1000 backend-mid:30:3000
# **문턱 안쪽은 지나가야 한다.** 늘 미달을 내는 판정은 늘 통과하는 판정과
# 똑같이 아무 신호도 안 준다.
MAX_SKEW=900 check "문턱 안쪽이면 통과" 0 "상위 5% 초과" "$herd" \
    backend:50:5000 backend-small:10:1000 backend-mid:30:3000
check "문턱을 안 주면 재기만 한다" 0 "문턱 없이 재기만" "$herd" \
    backend:50:5000 backend-small:10:1000 backend-mid:30:3000

# 집계가 어긋나면 순간 점유와 무관하게 미달이다.
check "집계 편차가 크면 미달" 1 "집계 편차" "$even" \
    backend:50:2000 backend-small:10:4000 backend-mid:30:3000

# 얕은 표본은 버린다. 한두 건이 점유를 통째로 흔들어, 고르게 도는 구현도
# 순간 초과 수백 퍼센트로 적힌다.
shallow='1 0 0
0 1 0
2 0 1'
check "얕은 표본만 있으면 판정 불가" 2 "쓸 만한 표본이 없다" "$shallow" \
    backend:50:5000 backend-small:10:1000 backend-mid:30:3000

# 모양이 어긋나면 조용히 지나가면 안 된다. 열 수가 다르거나 숫자가 아니면
# 그 회차는 아무것도 안 잰 것이고, 그것을 통과로 적으면 게이트가 사라진다.
check "열 수가 다르면 판정 불가" 2 "열이" '50 10' \
    backend:50:5000 backend-small:10:1000 backend-mid:30:3000
check "숫자가 아니면 판정 불가" 2 "숫자가 아닌" '50 열 30' \
    backend:50:5000 backend-small:10:1000 backend-mid:30:3000

# 도착이 0 이면 부하가 뒷단에 안 닿은 것이다. 0/0 을 편차 0 으로 읽으면
# 아무것도 안 지나간 회차가 통과로 적힌다.
check "도착이 0 이면 판정 불가" 2 "도착이 0" "$even" \
    backend:50:0 backend-small:10:0 backend-mid:30:0

# 표본 파일이 비면 순간 점유를 아예 못 잰다.
: > "$work/empty"
SAMPLES="$work/empty" run_case "표본이 비면 판정 불가" 2 "표본 파일이 비었다" -- \
    backend:50:5000 backend-small:10:1000 backend-mid:30:3000

# 스펙이 어긋나면 판정 전에 끊는다.
printf '50 10 30\n' > "$work/s.txt"
SAMPLES="$work/s.txt" run_case "스펙이 어긋나면 판정 불가" 2 "스펙이 어긋난다" -- \
    backend:50:5000 backend-small:열:1000 backend-mid:30:3000

exit "$selftest_failed"

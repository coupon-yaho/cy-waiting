#!/usr/bin/env bash
# 쏠림 판정의 자기검증 (TS-9).
#
# **이 판정이 기본 전략을 뒤집는 근거가 된다.** 헐거우면 게이트웨이 둘이 같은
# 대로 몰리는 구현에도 초록이 뜨고, 그 초록으로 결정 문서를 고치게 된다 —
# 재 본 적 없는 위험과 재 본 위험을 맞바꾸는 그 실수를 판정기가 대신 저지른다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1
. test/load/selftest-lib.sh

SELFTEST_JUDGE=$PWD/test/load/evaluate-skew.sh
work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

unset MAX_DEVIATION MAX_SKEW MIN_DEPTH_RATIO MIN_TOTAL

# 여유 200/40/120 — 실측이 쓰는 값이다. 기대 점유는 55.6 / 11.1 / 33.3.
# **깊이를 360 으로 둔다.** 얕으면 한 건이 점유를 크게 흔들어, 판정기의 눈금
# 가드가 먼저 끊는다 — 그 가드 자체를 재는 사례는 따로 둔다.
SPECS=(backend:200:5000 backend-small:40:1000 backend-mid:120:3000)

# 표본을 만든다: <고른 표본 수> <몰린 표본 수> [몰릴 대의 자리]
samples() {
    local even=$1 herd=$2 who=${3:-1} i
    for i in $(seq 1 "$even"); do echo "200 40 120"; done
    for i in $(seq 1 "$herd"); do
        case "$who" in
            1) echo "180 100 80" ;;   # 작은 대로 몰린다
            *) echo "300 20 40" ;;    # 큰 대로 몰린다
        esac
    done
}

check() {
    local name=$1 want_rc=$2 want_word=$3 samples=$4; shift 4
    printf '%s\n' "$samples" > "$work/s.txt"
    SAMPLES="$work/s.txt" run_case "$name" "$want_rc" "$want_word" -- "${@:-${SPECS[@]}}"
}

echo "쏠림 판정 자기검증"

check "고르게 흐르면 통과" 0 "충족" "$(samples 40 0)"

# **몰림은 여러 표본에 걸쳐 나타난다.** 상위 5% 가 따라 오른다.
check "여러 표본에 걸쳐 몰리면 잡는다" 1 "상위 5%" "$(samples 30 10)"

# **튄 표본 하나는 안 따라온다.** 최댓값으로 재면 여기서 미달이 나고, 그러면
# 통과 여부가 표본 하나의 운에 걸린다 — 백분위를 쓰는 이유가 이것이다.
check "표본 하나만 튀면 통과" 0 "충족" "$(samples 39 1)"

# **집계가 맞아도 순간이 몰리면 잡는다.** 각자는 여유대로 나눴는데 같은 순간에
# 겹치는 것이 문제라, 집계만 보면 이 갈래가 원리적으로 안 보인다.
check "집계가 맞아도 순간이 몰리면 잡는다" 1 "상위 5%" "$(samples 20 20)"

# 집계 편차는 순간 점유와 무관하게 문다.
check "집계 편차가 크면 미달" 1 "집계 편차" "$(samples 40 0)" \
    backend:200:2000 backend-small:40:4000 backend-mid:120:3000

# **한 대가 굶는 것도 편차다.** 절댓값을 안 씌우면 음의 편차가 통과한다 —
# 두 게이트웨이가 큰 대와 중간 대로만 가는 갈래가 정확히 그 모양이다.
check "한 대가 굶으면 미달" 1 "집계 편차" "$(samples 40 0)" \
    backend:200:5000 backend-small:40:0 backend-mid:120:3000

# 얕은 표본은 버린다. 한두 건이 점유를 통째로 흔든다.
check "얕은 표본만 있으면 판정 불가" 2 "쓸 만한 표본이 없다" '1 0 0
0 1 0
2 0 1'

# **절반 넘게 버렸으면 못 잰 것이다.** 남은 몇 개로 낸 백분위는 사실상
# 최댓값이고, 그 판정은 통과 여부가 운에 걸린다.
check "절반 넘게 버리면 판정 불가" 2 "쓸 만한 표본이" "$(samples 5 0; 샘플 0 0; for i in $(seq 1 40); do echo '1 0 0'; done)"

# **눈금이 문턱에 가까우면 못 잰다.** 깊이 90 에서는 작은 대의 한 건이 10% 라,
# 행동이 같은 구현도 한두 건 흔들림에 미달로 적힌다.
printf '%s\n' "$(for i in $(seq 1 40); do echo '50 10 30'; done)" > "$work/s.txt"
SAMPLES="$work/s.txt" MIN_DEPTH_RATIO=20 run_case "눈금이 굵으면 판정 불가" 2 "눈금" \
    -- "${SPECS[@]}"

# 도착이 적으면 비율이 우연히 맞는다.
check "도착이 적으면 판정 불가" 2 "도착이" "$(samples 40 0)" \
    backend:200:5 backend-small:40:1 backend-mid:120:3

# 모양이 어긋나면 조용히 지나가면 안 된다.
check "열 수가 다르면 판정 불가" 2 "열이" '200 40'
check "숫자가 아니면 판정 불가" 2 "숫자가 아닌" '200 열 120'
check "도착이 0 이면 판정 불가" 2 "도착이 0" "$(samples 40 0)" \
    backend:200:0 backend-small:40:0 backend-mid:120:0
check "여유가 0 이면 판정 불가" 2 "여유가 0" "$(samples 40 0)" \
    backend:200:5000 backend-small:0:1000 backend-mid:120:3000
check "칸이 비면 판정 불가" 2 "스펙이 어긋난다" "$(samples 40 0)" \
    backend:200:5000 backend-small::1000 backend-mid:120:3000
check "이름이 비면 판정 불가" 2 "모양은" "$(samples 40 0)" \
    :200:5000 backend-small:40:1000 backend-mid:120:3000

# 설정값이 숫자가 아니면 미달이 아니라 판정 불가다. **둘을 섞으면 망가진
# 설정이 "못 넘겼다" 로 적히고, 이 판정기의 미달은 기본값을 뒤집은 근거다.**
printf '%s\n' "$(samples 40 0)" > "$work/s.txt"
SAMPLES="$work/s.txt" MAX_DEVIATION=abc run_case "한계가 숫자가 아니면 판정 불가" 2 \
    "MAX_DEVIATION" -- "${SPECS[@]}"
SAMPLES="$work/s.txt" MAX_SKEW=abc run_case "문턱이 숫자가 아니면 판정 불가" 2 \
    "MAX_SKEW" -- "${SPECS[@]}"

# 표본 파일이 비면 순간 점유를 아예 못 잰다.
: > "$work/empty"
SAMPLES="$work/empty" run_case "표본이 비면 판정 불가" 2 "표본 파일이 비었다" -- "${SPECS[@]}"

exit "$selftest_failed"

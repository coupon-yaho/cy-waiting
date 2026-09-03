#!/usr/bin/env bash
# 롤링 배포 판정의 자기검증 (TS-9).
#
# **이 판정이 "찬 인스턴스로 안 몰린다" 게이트를 열고 닫는다.** 헐거우면
# 램프가 아예 없어도 초록이 뜨고, 그 초록을 근거로 램프를 지우게 된다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

judge=$PWD/test/load/evaluate-rollout.sh
work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

unset EARLY_SEC EARLY_MAX_RATIO MIN_PER_SEC

fail=0
check() {
    local name=$1 want_rc=$2 want_word=$3 steady=$4 rows=$5
    printf '%s\n' "$rows" > "$work/s.txt"
    local out rc
    out=$(EARLY_SEC="${EARLY_:-5}" EARLY_MAX_RATIO="${RATIO_:-70}" \
          MIN_PER_SEC="${MINPS_:-10}" "$judge" "$work/s.txt" "$steady" 2>&1)
    rc=$?
    if [ "$rc" -eq "$want_rc" ] && printf '%s' "$out" | grep -q "$want_word"; then
        echo "  ✓ $name"
    else
        echo "  ✗ $name — 종료 $rc (기대 $want_rc), '$want_word' 없음"
        printf '%s\n' "$out" | sed 's/^/      /'
        fail=1
    fi
}

echo "롤링 배포 판정 자기검증"

# 평상시 몫 33%. 램프가 있으면 처음 몇 초는 그보다 한참 낮다.
check "처음에 덜 받으면 충족" 0 "충족" 33 "0 5 100
1 9 100
2 14 100
3 20 100
4 26 100
5 33 100"

# **램프가 없으면 처음부터 제 몫을 받는다.** 이 갈래가 안 물면 램프를 지워도
# 초록이 뜬다 — 이 판정이 지키려는 것이 정확히 그것이다.
check "처음부터 제 몫이면 미달" 1 "미달" 33 "0 33 100
1 34 100
2 33 100
3 32 100
4 33 100"

# **"결국 제 몫을 받는다" 로는 판정이 안 된다.** 위 미달 사례도 그건 참이다.
check "나중에 맞는 것으로는 못 덮는다" 1 "미달" 33 "0 30 100
1 31 100
2 33 100
3 33 100
4 33 100"

# 보고 시작 전(음수 초)은 안 센다. 그때는 새 인스턴스가 아예 없다.
check "보고 전 표본은 안 센다" 0 "충족" 33 "-2 0 100
-1 0 100
0 5 100
1 8 100
2 12 100
3 18 100
4 24 100"

check "부하가 안 흐르면 판정 불가" 2 "판정 불가" 33 "0 1 3
1 0 2"
check "평상시 몫이 0 이면 막는다" 2 "견줄 것이 없다" 0 "0 5 100"
check "평상시 몫이 숫자가 아니면 막는다" 2 "정수여야" oops "0 5 100"
RATIO_=oops check "한계 비율이 숫자가 아니면 막는다" 2 "정수여야" 33 "0 5 100"

out=$("$judge" "$work/없는파일" 33 2>&1)
if [ $? -eq 2 ] && printf '%s' "$out" | grep -q "표본 파일이 없다"; then
    echo "  ✓ 표본 파일이 없으면 막는다"
else
    echo "  ✗ 표본 파일이 없으면 막는다"
    fail=1
fi

exit $fail

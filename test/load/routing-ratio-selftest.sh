#!/usr/bin/env bash
# 유입 비율 판정의 자기검증 (TS-9).
#
# **이 판정이 라우팅 게이트를 열고 닫는다.** 늘 "충족" 을 내면 비율이 무너져도
# 통과하고, 늘 "미달" 을 내면 맞는 구현도 못 넘는다. 둘 다 조용히 일어난다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

judge=$PWD/test/load/evaluate-routing-ratio.sh
fail=0

# 허용치를 못 박고 부른다. 기본값에 기대면 기본값이 움직일 때 경계 사례가
# 조용히 다른 것을 재게 된다.
check() {
    local name=$1 want_rc=$2 want_word=$3; shift 3
    local out rc
    out=$(MAX_DEVIATION="${TOLERANCE:-10}" "$judge" "$@" 2>&1); rc=$?
    if [ "$rc" -eq "$want_rc" ] && printf '%s' "$out" | grep -q "$want_word"; then
        echo "  ✓ $name"
    else
        echo "  ✗ $name — 종료 $rc (기대 $want_rc), '$want_word' 없음"
        printf '%s\n' "$out" | sed 's/^/      /'
        fail=1
    fi
}

echo "유입 비율 판정 자기검증"

# 실제로 잰 값 둘. 하나는 비율대로 왔고 하나는 안 왔다 — 판정이 이 둘을
# 갈라내지 못하면 무엇을 재도 같은 답이 나온다.
check "여유대로 온 실측은 충족" 0 "충족" \
    backend:200:333 backend-small:40:67 backend-mid:120:200
check "작은 대에 몰린 실측은 미달" 1 "미달" \
    backend:200:206 backend-small:40:188 backend-mid:120:206

# **경계를 양쪽에서 밟는다.** 한쪽만 보면 부등호 방향이 뒤집혀도 안 걸린다.
# 합계 300, 여유가 같으면 기대는 각 100 이다.
check "허용 안이면 충족" 0 "충족" a:1:110 b:1:100 c:1:90
check "허용 밖이면 미달" 1 "미달" a:1:112 b:1:100 c:1:88

# 표본이 적으면 한두 건이 편차 수십 %로 보인다. 못 잰 것을 미달로 적으면
# 없는 결함을 쫓게 된다.
check "표본이 모자라면 판정 불가" 2 "판정 불가" a:1:10 b:1:10

# 숫자가 아니면 셸 산술이 0 으로 읽어, 아무것도 안 온 것이 "기대와 같다" 로
# 나올 수 있다.
check "숫자가 아니면 막는다" 2 "숫자여야" a:1:100 b:xx:100 c:1:100
check "여유 합계가 0 이면 막는다" 2 "여유 합계" a:0:200 b:0:200
check "뒷단이 하나면 비율이 없다" 2 "둘 이상" a:1:500

# **기본 허용치가 게이트와 같은지 본다.** 여기가 갈라지면 같은 실측을 놓고
# 스크립트와 계획서가 다른 답을 낸다. 편차 12% 는 게이트(±15%) 안이고 자기
# 검증이 쓰는 10% 밖이다 — 기본값이 무엇인지가 결과를 가른다.
out=$("$judge" a:1:112 b:1:100 c:1:88 2>&1)
if printf '%s' "$out" | grep -q "충족"; then
    echo "  ✓ 기본 허용치가 게이트와 같다 (±15%)"
else
    echo "  ✗ 기본 허용치가 게이트(±15%)와 다르다"
    printf '%s\n' "$out" | sed 's/^/      /'
    fail=1
fi

exit $fail

#!/usr/bin/env bash
# 종료 유출 판정의 자기검증 (TS-9).
#
# **이 판정이 "종료 시 5xx 0" 게이트를 열고 닫는다.** 헐거우면 사용자가 장애를
# 봐도 초록이 뜨고, 그 초록을 근거로 재시도 배선을 안 넣게 된다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

judge=$PWD/test/load/evaluate-kill.sh
work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

# 물려받은 값을 버린다. 판정이 무는지를 보는 자리에서 그 입력이 흔들리면
# 무엇을 본 것인지 알 수 없다.
unset MAX_5XX MIN_SAMPLE

fail=0
check() {
    local name=$1 want_rc=$2 want_word=$3 body=$4
    printf '%s\n' "$body" > "$work/codes"
    local out rc
    out=$(MAX_5XX="${ALLOW_:-0}" MIN_SAMPLE="${MIN_:-20}" "$judge" "$work/codes" 2>&1)
    rc=$?
    if [ "$rc" -eq "$want_rc" ] && printf '%s' "$out" | grep -q "$want_word"; then
        echo "  ✓ $name"
    else
        echo "  ✗ $name — 종료 $rc (기대 $want_rc), '$want_word' 없음"
        printf '%s\n' "$out" | sed 's/^/      /'
        fail=1
    fi
}

many() { local code=$1 n=$2 i; for i in $(seq 1 "$n"); do echo "$code"; done; }

echo "종료 유출 판정 자기검증"

check "전부 200 이면 충족" 0 "충족" "$(many 200 30)"
# **한 건이라도 새면 미달이다.** 이 갈래가 안 물면 재시도가 없어도 초록이 뜬다.
check "5xx 한 건이면 미달" 1 "미달" "$(many 200 29)
503"
# 502·504 도 사용자에게는 같은 장애다. 앞자리로 본다.
check "504 도 5xx 로 센다" 1 "미달" "$(many 200 29)
504"
# 대기(202)와 한도(429)는 유출이 아니다. 이것을 미달로 세면 붐빌 때마다 빨개진다.
check "202·429 는 유출이 아니다" 0 "충족" "$(many 202 15)
$(many 429 15)"
check "표본이 모자라면 판정 불가" 2 "판정 불가" "$(many 200 5)"
# 숫자가 아닌 줄이 섞이면 셈이 조용히 어긋난다.
check "응답 코드가 아니면 막는다" 2 "응답 코드가 아닌" "$(many 200 30)
oops"
ALLOW_=oops check "허용치가 숫자가 아니면 막는다" 2 "정수여야" "$(many 200 30)"

out=$("$judge" "$work/없는파일" 2>&1)
if [ $? -eq 2 ] && printf '%s' "$out" | grep -q "응답 코드 파일이 없다"; then
    echo "  ✓ 파일이 없으면 막는다"
else
    echo "  ✗ 파일이 없으면 막는다"
    fail=1
fi

exit $fail

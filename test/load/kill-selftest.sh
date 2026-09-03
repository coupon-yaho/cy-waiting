#!/usr/bin/env bash
# 종료 유출 판정의 자기검증 (TS-9).
#
# **이 판정이 "종료 시 5xx 0" 게이트를 열고 닫는다.** 헐거우면 사용자가 장애를
# 봐도 초록이 뜨고, 그 초록을 근거로 재시도 배선을 안 넣게 된다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1
. test/load/selftest-lib.sh

SELFTEST_JUDGE=$PWD/test/load/evaluate-kill.sh
work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

# 물려받은 값을 버린다. 판정이 무는지를 보는 자리에서 그 입력이 흔들리면
# 무엇을 본 것인지 알 수 없다.
unset MAX_LEAK MIN_SAMPLE MIN_ADMITTED

check() {
    local name=$1 want_rc=$2 want_word=$3 body=$4
    printf '%s\n' "$body" > "$work/codes"
    MAX_LEAK="${ALLOW_:-0}" MIN_SAMPLE="${MIN_:-20}" MIN_ADMITTED="${ADMIT_:-20}" \
        run_case "$name" "$want_rc" "$want_word" -- "$work/codes"
}

# 설정을 안 덮어쓰고 부른다. **하네스는 아무것도 안 넘긴다** — 그러니 실제
# 게이트를 가르는 것은 판정기의 기본값인데, 위 `check` 는 그 값을 매번 덮어써서
# 기본값을 한 번도 안 태운다. 기본 허용 유출을 9999 로 바꿔도 다 통과한다.
bare() {
    local name=$1 want_rc=$2 want_word=$3 body=$4
    printf '%s\n' "$body" > "$work/codes"
    run_case "$name" "$want_rc" "$want_word" -- "$work/codes"
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
check "202·429 는 유출이 아니다" 0 "충족" "$(many 200 20)
$(many 202 10)
$(many 429 10)"
# **전부 돌려보내졌으면 아무것도 안 잰 것이다.** 이 갈래가 안 물면 죽은 대로
# 가는 경로를 한 번도 안 밟은 실행이 "충족" 으로 적힌다. 실제로 그랬다.
check "전부 대기면 판정 불가" 2 "죽은 대로 가는 경로" "$(many 202 100)"
check "뒷단까지 간 것이 적으면 판정 불가" 2 "죽은 대로 가는 경로" "$(many 200 5)
$(many 202 100)"
# **응답이 아예 없는 것도 유출이다.** 안 세면 매달린 요청 위에 초록이 뜬다.
check "응답 없음(000)도 유출이다" 1 "미달" "$(many 200 29)
000"
check "표본이 모자라면 판정 불가" 2 "판정 불가" "$(many 200 5)"
# 숫자가 아닌 줄이 섞이면 셈이 조용히 어긋난다.
check "응답 코드가 아니면 막는다" 2 "응답 코드가 아닌" "$(many 200 30)
oops"
ALLOW_=oops check "허용치가 숫자가 아니면 막는다" 2 "정수여야" "$(many 200 30)"

# 기본값이 게이트를 가르는 자리 셋. 여기가 없으면 허용 0 을 9999 로, 최소
# 표본을 1 로 바꿔도 위 사례가 전부 통과한다.
bare "기본 허용 유출은 0 이다" 1 "미달" "$(many 200 29)
503"
bare "기본 허용은 응답 없음도 센다" 1 "미달" "$(many 200 29)
000"
bare "기본 최소 표본은 20 이다" 2 "판정 불가" "$(many 200 19)"

out=$("$SELFTEST_JUDGE" "$work/없는파일" 2>&1); rc=$?
if [ "$rc" -eq 2 ] && printf '%s' "$out" | grep -q "응답 코드 파일이 없다"; then
    echo "  ✓ 파일이 없으면 막는다"
else
    echo "  ✗ 파일이 없으면 막는다"
    selftest_failed=1
fi

exit $selftest_failed

#!/usr/bin/env bash
# 판정 스크립트의 자기검증 (TS-9).
#
# **이 스크립트가 병합을 막는 유일한 자리다.** 브랜치 보호가 요구하는 검사는
# `판정` 하나이고, 개별 잡은 요구 목록에 없다. 여기서 0 으로 끝나면 카오스가
# 빨개져도 병합이 열린다 — 실제로 그랬다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

judge=$PWD/.github/scripts/verdict.sh
out=$(mktemp)
trap 'rm -f "$out"' EXIT

fail=0
# **셸 지역변수는 영문이어야 한다.** bash 의 local 이 한글 식별자를 안 받는다.
check() {
    local name=$1 results=$2 want_rc=$3 want_status=$4
    : > "$out"
    RESULTS="$results" GITHUB_OUTPUT="$out" "$judge" >/dev/null 2>&1
    local rc=$? status
    status=$(grep '^status=' "$out" | cut -d= -f2)
    if [ "$rc" -eq "$want_rc" ] && [ "$status" = "$want_status" ]; then
        echo "  ✓ $name"
    else
        echo "  ✗ $name — 종료 $rc (기대 $want_rc), 판정 '$status' (기대 '$want_status')"
        fail=1
    fi
}

echo "판정 자기검증"
check "전부 성공이면 통과"          "success success success"        0 success
check "건너뛴 것은 통과로 센다"      "success skipped success"        0 success
check "하나라도 실패면 막는다"       "success failure success"        1 failure
check "카오스만 실패해도 막는다"     "success success failure"        1 failure
check "취소는 통과가 아니다"         "success cancelled success"      1 failure
check "맨 앞 실패도 잡는다"          "failure success"                1 failure
check "맨 뒤 실패도 잡는다"          "success failure"                1 failure
check "결과가 비면 막는다"           ""                               1 failure
check "공백만 와도 막는다"           "   "                            1 failure
# 탭 구분자를 받으므로 탭만 온 판도 "결과가 없다" 다.
check "탭만 와도 막는다"             "$(printf '\t')"                 1 failure
# **부분 일치로 잡지 않는다.** 'failured' 같은 값은 결과가 아니고, 여기서
# 걸리면 정상 판을 막는다 — 게이트가 시끄러우면 사람이 우회한다.
check "비슷한 낱말은 안 잡는다"      "success successful"             0 success
# **구분자에 안 기댄다.** join() 의 기본 구분자는 쉼표다. 여기가 비어 있으면
# 새 워크플로 한 줄로 게이트가 조용히 사라진다.
check "쉼표로 이어도 막는다"         "success,failure"                1 failure
check "탭으로 이어도 막는다"         "$(printf 'success\tcancelled')" 1 failure
check "쌍반점으로 이어도 막는다"     "success;failure"                1 failure

# **배선도 본다.** 스크립트만 검사하면 액션이 그걸 안 불러도 통과한다 —
# 옛 동작을 액션 쪽에 되돌려 넣어 봤더니 이 검사도 워크플로 셸 검사도 안 물었다.
action=.github/actions/verdict/action.yml
if grep -q 'scripts/verdict\.sh' "$action"; then
    echo "  ✓ 액션이 스크립트를 부른다"
else
    echo "  ✗ 액션이 스크립트를 안 부른다 — $action"
    fail=1
fi

[ "$fail" -eq 0 ] && echo "판정 자기검증 통과" || echo "판정 자기검증 실패"
exit "$fail"

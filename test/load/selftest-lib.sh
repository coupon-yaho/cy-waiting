#!/usr/bin/env bash
# 판정 자기검증이 함께 쓰는 부분.
#
# **왜 뺐나.** 자기검증 넷이 "판정을 부르고 종료 코드와 문구를 대조한다" 를
# 글자 그대로 똑같이 적고 있었다. 다른 것은 판정기에 넘기는 설정뿐이다.
# 대조 규칙이 네 곳에 있으면 그중 하나만 헐거워져도 알 수 없다.
#
#   사용: . test/load/selftest-lib.sh
#         run_case <이름> <기대 종료코드> <기대 문구> -- <판정기 인자...>
#         환경 설정은 부르는 쪽이 앞에 붙인다.

[ -n "${SELFTEST_LIB_LOADED:-}" ] && return 0
SELFTEST_LIB_LOADED=1

# 실패한 사례가 하나라도 있으면 1 이 된다. 부르는 쪽이 이 값으로 끝낸다.
selftest_failed=0

# 판정기를 부르고 **종료 코드와 문구를 둘 다** 본다.
#
# 종료 코드만 보면 "미달" 과 "판정 불가" 가 같은 1 로 보일 수 있고, 문구만
# 보면 종료 코드가 뒤집혀도 지나간다. 게이트가 이 종료 코드로 갈리므로 둘 다
# 맞아야 한다.
run_case() {
    local name=$1 want_rc=$2 want_word=$3
    shift 3
    [ "${1:-}" = "--" ] && shift
    local out rc
    out=$("$SELFTEST_JUDGE" "$@" 2>&1)
    rc=$?
    if [ "$rc" -eq "$want_rc" ] && printf '%s' "$out" | grep -q "$want_word"; then
        echo "  ✓ $name"
    else
        echo "  ✗ $name — 종료 $rc (기대 $want_rc), '$want_word' 없음"
        printf '%s\n' "$out" | sed 's/^/      /'
        selftest_failed=1
    fi
}

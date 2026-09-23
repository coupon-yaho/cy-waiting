#!/usr/bin/env bash
# 판정 자기검증이 함께 쓰는 부분.
#
# **왜 뺐나.** 자기검증 넷이 "판정을 부르고 종료 코드와 문구를 대조한다" 를
# 글자 그대로 똑같이 적고 있었다. 다른 것은 판정기에 넘기는 설정뿐이다.
# 대조 규칙이 네 곳에 있으면 그중 하나만 헐거워져도 알 수 없다.
#
#   사용: . test/load/selftest-lib.sh
#         run_case <이름> <기대 종료코드> <기대 문구> -- <판정기 인자...>
#         fixture <파일명> <내용>      # $work 에 쓰고 경로를 낸다
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
    # `--` 를 넣는다. 없으면 `-1.0초` 같은 기대 문구가 grep 의 옵션으로 먹혀,
    # 판정기가 맞게 찍었는데도 케이스가 빨개진다.
    if [ "$rc" -eq "$want_rc" ] && printf '%s' "$out" | grep -q -- "$want_word"; then
        echo "  ✓ $name"
    else
        echo "  ✗ $name — 종료 $rc (기대 $want_rc), '$want_word' 없음"
        printf '%s\n' "$out" | sed 's/^/      /'
        selftest_failed=1
    fi
}

# 표본 파일 하나를 만들고 **경로를 낸다.** 이름만 다른 같은 함수가 파일마다 있었고
# (`summary`·`enq`), 형식이 하나 바뀌면 나머지가 조용히 갈렸다.
#
# `$work` 는 부르는 쪽이 만든다 — 지우는 책임도 거기 있다.
# **부르는 자리마다 다른 파일을 준다.** 이름을 그대로 쓰면 같은 이름의 두 번째 표본이 앞의 것을
# 덮어, 경로를 변수에 잡아 둔 자리가 그 뒤로 엉뚱한 입력을 가리킨다 — 사례가 조용히 딴것을 잰다.
#
# 일련번호를 파일로 센다. 부르는 자리가 `$(fixture ...)` 라 셸 변수는 하위 셸에서 늘고 사라진다.
fixture() {
    local seq path
    seq=$(( $(cat "$work/.fixture-seq" 2>/dev/null || echo 0) + 1 ))
    printf '%s' "$seq" > "$work/.fixture-seq"
    path=$work/$seq-$1
    printf '%s' "$2" > "$path"
    printf '%s' "$path"
}

# 함수 하나를 직접 재는 사례. **판정기가 아니라 러너의 부분을 잴 때 쓴다** —
# 값을 만드는 쪽이 조용히 거짓을 내면 판정기가 아무리 견고해도 소용이 없다.
#
#   사용: lib_case <이름> <기대> <실제>
lib_case() {
    local name=$1 want=$2 got=$3
    if [ "$got" = "$want" ]; then
        echo "  ✓ $name"
    else
        echo "  ✗ $name — '$got' (기대 '$want')"
        selftest_failed=1
    fi
}


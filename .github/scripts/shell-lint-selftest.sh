#!/usr/bin/env bash
# 셸 검사의 프로젝트 규칙 자기검증 (TS-9).
#
# **텍스트로 보는 규칙은 거짓 통과가 쉽다.** 대리 소스를 인정하면 정작 그 대리가
# 목록을 안 읽어도 조용하다 — 실제로 그 구멍이 있었다. 규칙이 무는지를 잰다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

fail=0
check() {
    local name=$1 want_rc=$2 body=$3 out rc
    printf '%s\n' "$body" > "$work/runner.sh"
    out=$(LINT_SELFTEST=1 LINT_TARGETS="$work/runner.sh" .github/scripts/shell-lint.sh 2>&1)
    rc=$?
    local ok=1
    [ "$rc" -eq "$want_rc" ] || ok=0
    # **종료 코드만 보면 다른 이유로 난 1 도 통과로 읽는다.** 무슨 말을 했는지까지 본다.
    if [ "$want_rc" -eq 1 ]; then
        case "$out" in *"queue-keys.sh 를 안 읽는다"*) ;; *) ok=0 ;; esac
    fi
    if [ "$ok" -eq 1 ]; then
        echo "  ✓ $name"
    else
        echo "  ✗ $name — 종료 $rc (기대 $want_rc)"
        printf '%s\n' "$out" | sed 's/^/      /'
        fail=1
    fi
}

echo "셸 검사 규칙 자기검증"

check "목록을 읽고 쓰면 통과" 0 '#!/usr/bin/env bash
. test/load/queue-keys.sh || exit 2
redis-cli DEL $(queue_keys c1)'
check "안 읽고 쓰면 문다" 1 '#!/usr/bin/env bash
redis-cli DEL $(queue_keys c1)'
check "대리 소스는 안 봐준다" 1 '#!/usr/bin/env bash
. test/load/peak-lib.sh || exit 2
redis-cli DEL $(queue_keys c1)'
check "안 쓰면 안 본다" 0 '#!/usr/bin/env bash
echo 아무것도 안 한다'

# **표식 없이는 이음매가 안 열린다.** 환경 변수 하나로 프로덕션 검사가 꺼지면 안 된다.
out=$(LINT_TARGETS="$work/runner.sh" .github/scripts/shell-lint.sh 2>&1)
case "$out" in
    *"자기검증 회차"*) echo "  ✗ 표식 없이 이음매가 열렸다"; fail=1 ;;
    *) echo "  ✓ 표식 없이는 이음매가 안 열린다" ;;
esac

[ "$fail" -eq 0 ] || exit 1
echo "  셸 검사 규칙 자기검증 통과"

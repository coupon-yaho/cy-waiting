#!/usr/bin/env bash
# PR 을 올리기 전에 로컬 리뷰를 강제한다.
#
# **왜 차단인가.** "올리기 전에 돌려라" 는 규범은 잊힌다. 실제로 잊었고,
# CodeRabbit 이 두 라운드에 걸쳐 14건을 지적했는데 그중 셋은 우리 자신의
# MUST 규칙 위반이었다 — 로컬에서 1초면 잡히는 것들이다.
#
# PreToolUse(Bash) 훅. `gh pr create` 를 만나면 기계 검사를 돌리고
# 위반이 있으면 exit 2 로 막는다.

set -uo pipefail

input=$(cat)
cmd=$(printf '%s' "$input" | jq -r '.tool_input.command // empty')

# PR 생성이 아니면 통과
[[ "$cmd" != *"gh pr create"* ]] && exit 0

ROOT=$(git rev-parse --show-toplevel 2>/dev/null) || exit 0
RUNNER="$ROOT/.claude/hooks/review-branch.sh"
[[ -x "$RUNNER" ]] || exit 0

# base 를 명령에서 뽑는다. 없으면 develop
base=$(printf '%s' "$cmd" | grep -oE -e '--base[= ]+[A-Za-z0-9._/-]+' | head -1 \
       | sed -E 's/--base[= ]+//')
base="origin/${base:-develop}"

out=$("$RUNNER" "$base" 2>&1)
status=$?

if ((status != 0)); then
    {
        echo "PR 을 올리기 전에 로컬 리뷰가 통과해야 한다."
        echo
        # 전체를 보여 준다. 걸러내면 정작 필요한 줄이 빠진다.
        printf '%s\n' "$out"
        echo
        echo "고친 뒤 다시 시도한다.  수동 실행: .claude/hooks/review-branch.sh $base"
        echo "전체 절차: /review"
    } >&2
    exit 2
fi

# 통과했어도 기계가 못 보는 것이 남는다 — 막지는 않고 알린다.
{
    echo "로컬 기계 검사 통과. 아직 안 한 것이 있는지 본다:"
    echo "  · ./gradlew build jacocoTestCoverageVerification pitest"
    printf '%s\n' "$out" | sed -n '/사람·에이전트가 볼 것/,$p' | sed 's/^/  /'
} >&2
exit 0

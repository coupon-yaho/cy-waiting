#!/usr/bin/env bash
# 커밋 메시지 규약 검사. ai/rules/60-workflow.md (WF-1, WF-2)
#
# PreToolUse(Bash) 훅.
#
# 명령 탐지에 주의할 것: `git -c user.name=X commit -m ...` 처럼 git 과 commit 사이에
# 옵션이 끼면 "git commit" 부분 문자열 매칭은 조용히 빗나간다. 검사를 통과시키는
# 훅은 없는 훅보다 나쁘다 — 지켜지고 있다고 착각하게 만들기 때문이다.
set -uo pipefail

input=$(cat)
cmd=$(printf '%s' "$input" | jq -r '.tool_input.command // empty')

[[ -z "$cmd" ]] && exit 0
printf '%s' "$cmd" | grep -qE '(^|[[:space:];&|(])git([[:space:]]|$)' || exit 0
printf '%s' "$cmd" | grep -qE '[[:space:]]commit([[:space:]]|$)' || exit 0

# 메시지를 전달하지 않는 형태는 통과시킨다 (에디터·파일·재사용)
if ! printf '%s' "$cmd" | grep -qE '(^|[[:space:]])-m'; then
    if printf '%s' "$cmd" | grep -qE '(--no-edit|--amend|-F|--file|-C|--reuse-message|--fixup|--squash)'; then
        exit 0
    fi
    echo "[WF-1] 커밋 메시지를 -m 으로 전달한다. 에디터 커밋은 규약 검사를 우회한다." >&2
    exit 2
fi

# 첫 번째 -m 값(제목)만 뽑는다. -m 이 여럿이면 두 번째부터는 본문이다.
subject=$(printf '%s' "$cmd" \
    | grep -oE -- "-m[[:space:]]*(\"[^\"]*\"|'[^']*')" \
    | head -1 \
    | sed -E "s/^-m[[:space:]]*//; s/^[\"']//; s/[\"']$//")

if [[ -z "$subject" ]]; then
    echo "[WF-1] -m 뒤의 메시지를 따옴표로 감싼다." >&2
    exit 2
fi

# 규칙은 .githooks/lib/ 하나에만 둔다. 여기에 복사하면 git 훅과 갈라지고,
# 그때부터 어느 쪽이 맞는지 알 수 없다.
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../../.githooks/lib/commit-subject-rules.sh
source "$here/../../.githooks/lib/commit-subject-rules.sh"

if ! violations=$(check_commit_subject "$subject"); then
    {
        echo "커밋 메시지 규약 위반"
        echo "  제목: $subject"
        echo
        echo "$violations"
        commit_rule_help
    } >&2
    exit 2
fi
exit 0

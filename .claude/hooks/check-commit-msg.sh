#!/usr/bin/env bash
# 커밋 메시지 규약 검사. ai/rules/60-workflow.md (WF-1, WF-2)
#
# PreToolUse(Bash) 훅.
#
# **낱말을 세지 않는다.** 두 낱말을 따로 찾으면 양쪽으로 틀린다 — 같은 명령일 필요도
# 인접할 필요도 없어서 읽기 전용 명령이 막히고, 경로를 붙여 부르면 통째로 지나간다.
# 검사를 통과시키는 훅은 없는 훅보다 나쁘고, 오탐이 잦은 훅은 우회되어 결국 같은 곳에
# 도달한다.
#
# **쪼개는 일은 파이썬이 한다.** 셸로 자르면 인용 안의 `#`·`|`·`;` 를 문법으로 읽어
# 제목이 잘리고 세그먼트가 갈린다. 표준 라이브러리의 셸 렉서를 쓴다.
set -uo pipefail

input=$(cat)
cmd=$(printf '%s' "$input" | jq -r '.tool_input.command // empty')

[[ -z "$cmd" ]] && exit 0

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

subjects=$(COMMIT_CMD="$cmd" python3 "$here/commit-subjects.py")

if [[ "$subjects" == *"__UNPARSED__"* ]]; then
    echo "[WF-1] 명령의 인용 짝이 안 맞는다 — 무엇을 커밋하는지 몰라 막는다." >&2
    exit 2
fi
if [[ "$subjects" == *"__NO_MESSAGE__"* ]]; then
    echo "[WF-1] 커밋 메시지를 -m 으로 전달한다. 에디터 커밋은 규약 검사를 우회한다." >&2
    exit 2
fi
[[ -z "${subjects//[$'\n'[:space:]]/}" ]] && exit 0

# 규칙은 .githooks/lib/ 하나에만 둔다. 여기에 복사하면 git 훅과 갈라지고,
# 그때부터 어느 쪽이 맞는지 알 수 없다.
# shellcheck source=../../.githooks/lib/commit-subject-rules.sh
source "$here/../../.githooks/lib/commit-subject-rules.sh"

while IFS= read -r subject; do
    [[ -z "${subject// /}" ]] && continue
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
done <<< "$subjects"

exit 0

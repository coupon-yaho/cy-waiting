#!/usr/bin/env bash
# 보호 경로 쓰기 차단. ai/rules/60-workflow.md (WF-5)
#
# PreToolUse(Write|Edit|Bash) 훅. waiting-legacy/ 는 참조 전용이다.
# 파일 도구뿐 아니라 Bash 경로도 막는다 — 한쪽만 막으면 막지 않은 것과 같다.
set -uo pipefail

input=$(cat)
file=$(printf '%s' "$input" | jq -r '.tool_input.file_path // empty')
cmd=$(printf '%s' "$input" | jq -r '.tool_input.command // empty')

deny() {
    {
        echo "[WF-5] waiting-legacy/ 는 참조 전용이다. 수정할 수 없다."
        echo
        echo "  차단된 대상: $1"
        echo
        echo "이 디렉터리는 제로베이스 재작성 이전 구현이며, 설계 논거를 참조하기"
        echo "위해서만 보존한다. 읽는 것은 자유, 쓰는 것은 금지다."
        echo "새 구현은 이 저장소의 src/ 에서 한다."
    } >&2
    exit 2
}

[[ "$file" == *"/waiting-legacy/"* ]] && deny "$file"

if [[ -n "$cmd" && "$cmd" == *"waiting-legacy"* ]]; then
    # 읽기 전용 명령은 통과시킨다
    if printf '%s' "$cmd" | grep -qE '(^|[[:space:];&|])(rm|mv|cp|sed|tee|truncate|chmod|chown|dd|install|ln)([[:space:]]|$)' \
        || printf '%s' "$cmd" | grep -qE '>[[:space:]]*[^[:space:]]*waiting-legacy'; then
        deny "$cmd"
    fi
fi

exit 0

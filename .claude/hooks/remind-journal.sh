#!/usr/bin/env bash
# 작업 로그 누락 알림. ai/rules/60-workflow.md (WF-6)
#
# Stop 훅. 코드를 바꿨는데 journal 엔트리가 없으면 알린다.
# 차단하지 않는다 — 작업 흐름을 끊는 대신 상기시킨다.
# (준수율이 낮으면 exit 2 차단으로 올린다. AIJ-0003 참조)
set -uo pipefail

cd "${CLAUDE_PROJECT_DIR:-.}" 2>/dev/null || exit 0
git rev-parse --git-dir >/dev/null 2>&1 || exit 0

changed=$(git status --porcelain 2>/dev/null) || exit 0
[[ -z "$changed" ]] && exit 0

# 경로만 추린다 (porcelain: "XY path" 또는 "XY old -> new")
paths=$(printf '%s\n' "$changed" | sed -E 's/^.{3}//; s/^.* -> //')

# 근거를 남겨야 하는 변경만 본다.
#   대상: src/ · *.lua · 빌드 스크립트 · .github/workflows · .github/actions
#   제외: 문서·규칙·계획 — 그 자체가 근거 문서다
code_changed=$(printf '%s\n' "$paths" \
    | grep -vE '^(plan|ai|\.claude)/' \
    | grep -vE '^\.github/(README\.md|CODEOWNERS|PULL_REQUEST_TEMPLATE\.md|ISSUE_TEMPLATE/)' \
    | grep -vE '\.md$' \
    | grep -vE '^\.git(ignore|attributes)$' \
    | head -1)

[[ -z "$code_changed" ]] && exit 0

journal_changed=$(printf '%s\n' "$paths" | grep -E '^ai/journal/' | head -1)
[[ -n "$journal_changed" ]] && exit 0

cat >&2 <<EOF
[WF-6] 코드를 변경했지만 작업 로그가 없다.
       (예: $code_changed)

ai/journal/TEMPLATE.md 를 복사해 ai/journal/$(date +%Y/%m)/AIJ-####-<slug>.md 를 만들고
ai/journal/index.md 에 한 줄을 더한다.

특히 이 세 절을 비우지 말 것 — 나머지는 짧아도 된다:
  - 왜 (근거)
  - 고려했으나 택하지 않은 것
  - 확신이 낮은 부분 / 남은 위험

코드는 무엇을 했는지 보여주지만, 왜 그렇게 했는지는 남기지 않으면 사라진다.
EOF
exit 0

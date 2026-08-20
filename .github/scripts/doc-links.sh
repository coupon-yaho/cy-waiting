#!/usr/bin/env bash
# 문서 링크 검사. CI(_verify-conventions.yml)와 **같은 스크립트**를 쓴다.
#
# 전에는 CI 워크플로 안에 인라인으로만 있어서 로컬에서 못 돌렸다. 그래서
# 파일을 지우고 링크를 안 고친 PR 이 CI 에서야 걸렸다 — 여러 번.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

fail=0
while IFS= read -r f; do
    d=$(dirname "$f")
    while IFS= read -r l; do
        [[ -z "$l" || "$l" == http* || "$l" == '#'* ]] && continue
        [[ -e "$d/$l" ]] || { echo "::error file=$f::깨진 링크 — $l"; fail=1; }
    done < <(
        # 코드를 먼저 걷어내고 링크를 뽑는다. 오탐이 잦은 검사는 곧
        # 무시되고, 무시되는 검사는 없는 것과 같다.
        awk '/^[[:space:]]*```/ { fence = !fence; next } !fence' "$f" \
            | sed -E 's/`[^`]*`//g' \
            | grep -oE '\]\(([^)#]+)(#[^)]*)?\)' 2>/dev/null \
            | sed -E 's/^\]\(//; s/\)$//; s/#.*//'
    )
done < <(git ls-files '*.md')
exit $fail

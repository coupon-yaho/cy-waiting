#!/usr/bin/env bash
# 잡 결과 목록을 하나의 통과·실패로 판정하고, 실패면 이 잡도 실패시킨다.
#
# **출력만 내고 성공으로 끝나면 게이트가 게이트가 아니다.** 브랜치 보호가
# 요구하는 것은 이 잡의 성공 여부인데, 여기서 늘 0 으로 끝나면 카오스가
# 빨개져도 병합이 열린다 (G8.13). 실제로 그렇게 돼 있었다.
set -uo pipefail

results=${RESULTS:-}

emit() {
    [ -n "${GITHUB_OUTPUT:-}" ] && printf '%s\n' "$1" >> "$GITHUB_OUTPUT"
    return 0
}

# **비어 있으면 실패다.** 결과를 못 받은 것은 "이상 없다" 가 아니라 "안 봤다" 다.
# 통과로 넘기면 needs 를 잘못 엮는 순간 게이트가 조용히 사라진다.
if [ -z "${results// /}" ]; then
    emit "status=failure"
    emit "ok=false"
    echo "::error title=판정::결과 목록이 비었다 — 무엇을 봤는지 알 수 없다"
    exit 1
fi

# skipped 는 통과로 본다 — 문서 PR 에서 빌드·테스트를 건너뛰는 것은 정상이다.
# failure 와 cancelled 는 통과가 아니다. 취소는 "검사하지 않았다"는 뜻이지
# "이상 없다"가 아니다.
if grep -qE '(^| )(failure|cancelled)( |$)' <<<"$results"; then
    emit "status=failure"
    emit "ok=false"
    echo "::error title=판정::실패 — $results"
    exit 1
fi

emit "status=success"
emit "ok=true"
echo "::notice title=판정::통과 — $results"

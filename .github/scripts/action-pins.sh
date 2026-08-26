#!/usr/bin/env bash
# 서드파티 액션의 SHA 핀이 옆 주석과 맞는지 본다.
#
# **주석이 거짓말하면 핀을 하는 의미가 없다.** 실제로 그랬다 —
# `download-artifact` 는 주석이 `v7.0.0` 인데 핀은 `v6.0.0` 의 것이었고,
# `gradle/actions` 는 커밋이 아니라 **태그 객체**를 핀하고 있었다. 둘 다
# 사람이 읽어서는 안 드러난다. 리뷰어는 주석을 믿고 넘어간다.
#
# 정확한 판(`v4.4.3`)을 요구한다. 메이저만 적으면(`v4`) Dependabot 이 그것을
# 의도로 읽어 **그 메이저에 영구히 묶는다** — 그래서 `gradle/actions` 가 두
# 메이저, `codeql-action` 이 한 메이저 뒤처져 있었다.
#
# 네트워크가 필요하다. GH_TOKEN 이 없으면 검사를 건너뛰지 않고 실패한다 —
# 조용히 통과하는 검사는 없는 것보다 나쁘다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

# 우리 저장소의 액션은 제외한다. 같은 조직이 소유하므로 공급망 경계가 아니고,
# 판을 우리가 올리므로 `v1` 같은 떠 있는 태그를 쓰는 것이 의도다.
readonly OURS='coupon-yaho/'

fail=0
found=0

while IFS=' ' read -r ref comment; do
    [[ -z "$ref" ]] && continue
    [[ "$ref" == "$OURS"* ]] && continue
    found=1

    path=${ref%@*}
    sha=${ref#*@}
    repo=$(cut -d/ -f1,2 <<<"$path")

    if [[ ! "$comment" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "::error::$path — 주석이 정확한 판이 아니다: '$comment'." \
             "메이저만 적으면 Dependabot 이 그 메이저에 묶는다"
        fail=1
        continue
    fi

    # 태그가 가리키는 커밋과 핀이 같아야 한다. `tags` 는 주석 달린 태그도
    # 커밋까지 풀어 주므로 `git/ref` 를 직접 보는 것보다 이쪽이 맞다.
    actual=$(gh api "repos/$repo/tags?per_page=100" \
        --jq "[.[] | select(.name==\"$comment\") | .commit.sha] | first" 2>/dev/null)

    if [[ -z "$actual" || "$actual" == "null" ]]; then
        echo "::error::$path — 주석의 태그 '$comment' 를 $repo 최근 태그에서 못 찾았다"
        fail=1
    elif [[ "$actual" != "$sha" ]]; then
        echo "::error::$path — 핀과 주석이 어긋난다." \
             "주석=$comment 는 $actual 인데 핀은 $sha 다"
        fail=1
    fi
done < <(
    grep -rhoE "uses: [a-zA-Z0-9._/-]+@[0-9a-f]{40}[[:space:]]*#[[:space:]]*\S+" .github/ \
        | sed -E 's/uses: //; s/[[:space:]]*#[[:space:]]*/ /' \
        | sort -u
)

# **하나도 못 찾으면 실패다.** 정규식이 어긋나면 검사가 조용히 0건을 보고
# 통과하고, 그때부터 아무것도 안 막는다.
if [[ $found -eq 0 ]]; then
    echo "::error::핀 고정된 서드파티 액션을 하나도 못 찾았다 — 검사가 헛돈다"
    fail=1
fi

exit $fail

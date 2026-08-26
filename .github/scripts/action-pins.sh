#!/usr/bin/env bash
# 서드파티 액션이 커밋 SHA 로 핀됐고, 옆 주석이 그 SHA 의 판과 맞는지 본다.
#
# **모든 `uses:` 를 열거해서 규격에 안 맞는 것을 잡는다.** 규격에 맞는 것만
# 정규식으로 찾으면 그물이 거꾸로 걸린다 — `@v1`, `@main`, 따옴표를 씌운 핀,
# 주석 없는 핀이 전부 무성 통과하고, 그때 초록불은 "핀이 검증됐다" 로 읽힌다.
# 없는 검사보다 나쁘다.
#
# 실제로 이렇게 지나갈 수 있었다:
#     uses: "actions/checkout@<공격자 SHA>"  # v7.0.1
# YAML 로 유효하고 러너는 그 SHA 를 체크아웃한다. diff 에는 따옴표가 붙고 16진수
# 40자가 바뀐 것뿐이라 사람 눈에는 정상 갱신으로 읽힌다.
#
# **주석이 거짓말한 적도 있다.** `download-artifact` 는 주석이 `v7.0.0` 인데
# `v8.0.1` 을 핀하고 있었고, `gradle/actions` 는 커밋이 아니라 태그 객체를 핀했다.
#
# 네트워크가 필요하다. 못 물으면 실패한다 — 조용히 통과하는 검사는 없는 것보다
# 나쁘다는 것이 이 파일의 전제다.
set -uo pipefail

root=$(git rev-parse --show-toplevel 2>/dev/null) || root=.
cd "$root" || exit 1

# 1) 모든 `uses:` 를 YAML 로 열거한다. 셸 검사(workflow-shell.sh)와 같은 방식이다.
refs=$(python3 - <<'PY'
import pathlib, sys

try:
    import yaml
except ImportError:
    print("::error::PyYAML 이 없어 액션 핀을 못 읽는다", file=sys.stderr)
    sys.exit(1)

# composite action 도 본다. `.github/workflows` 만 보면 `setup-gradle` 안의
# 서드파티 액션이 통째로 빠진다 — dependabot.yml 이 같은 이유로 경로를 나눈다.
targets = sorted(pathlib.Path('.github/workflows').glob('*.y*ml')) \
    + sorted(pathlib.Path('.github/actions').glob('*/action.y*ml'))

if not targets:
    print("::error::검사할 워크플로·액션 파일이 없다 — 검사가 헛돈다", file=sys.stderr)
    sys.exit(1)


def walk(node, out):
    """`uses:` 는 워크플로(steps)와 재사용 워크플로(jobs) 양쪽에 나온다."""
    if isinstance(node, dict):
        value = node.get('uses')
        if isinstance(value, str):
            out.append(value)
        for child in node.values():
            walk(child, out)
    elif isinstance(node, list):
        for child in node:
            walk(child, out)


found = []
for path in targets:
    try:
        doc = yaml.safe_load(path.read_text())
    except yaml.YAMLError as e:
        print(f"::error file={path}::YAML 을 못 읽는다: {e}", file=sys.stderr)
        sys.exit(1)
    seen = []
    walk(doc, seen)
    for ref in seen:
        found.append(f"{path}\t{ref}")

if not found:
    print("::error::`uses:` 를 하나도 못 찾았다 — 검사가 헛돈다", file=sys.stderr)
    sys.exit(1)

print("\n".join(found))
PY
) || exit 1

# 2) 주석은 YAML 이 버리므로 원문에서 따로 읽는다. 참조 → 주석.
declare -A comment_of
while IFS=$'\t' read -r ref note; do
    [[ -n "$ref" ]] && comment_of["$ref"]="$note"
done < <(
    grep -rhoE "uses:[[:space:]]*[\"']?[^\"'[:space:]]+[\"']?[[:space:]]*#[[:space:]]*\S+" \
        .github/workflows .github/actions 2>/dev/null \
        | sed -E "s/uses:[[:space:]]*[\"']?//; s/[\"']?[[:space:]]*#[[:space:]]*/\t/"
)

# 우리 조직 액션은 **판 주석 규칙만** 면제한다. 판을 우리가 올리므로 주석이
# 뒤처지는 것은 사고가 아니다. 핀 자체는 그대로 요구한다 — 그 잡에 Atlassian
# 토큰이 붙는데, `@v1` 로 바꿔도 아무 말 안 하면 제외가 구멍이 된다.
readonly OURS='coupon-yaho/'

fail=0
checked=0

while IFS=$'\t' read -r file ref; do
    [[ -z "$ref" ]] && continue

    # 이 저장소 안의 액션. 우리 코드라 공급망 경계가 아니다.
    [[ "$ref" == ./* || "$ref" == .github/* ]] && continue

    if [[ "$ref" == docker://* ]]; then
        # 태그는 움직인다. 다이제스트만 받는다.
        [[ "$ref" == *"@sha256:"* ]] \
            || { echo "::error file=$file::$ref — docker 액션은 다이제스트로 핀한다"; fail=1; }
        checked=$((checked + 1))
        continue
    fi

    checked=$((checked + 1))
    path=${ref%@*}
    rev=${ref##*@}

    if [[ "$ref" != *@* || ! "$rev" =~ ^[0-9a-f]{40}$ ]]; then
        echo "::error file=$file::$ref — 커밋 SHA 40자로 핀해야 한다." \
             "태그와 브랜치는 움직인다"
        fail=1
        continue
    fi

    [[ "$path" == "$OURS"* ]] && continue

    version=${comment_of[$ref]:-}
    if [[ -z "$version" ]]; then
        echo "::error file=$file::$path — 핀 옆에 판 주석이 없다." \
             "없으면 무엇이 핀됐는지 사람도 Dependabot 도 모른다"
        fail=1
        continue
    fi

    if [[ ! "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "::error file=$file::$path — 주석이 정확한 판이 아니다: '$version'." \
             "메이저만 적으면 Dependabot 이 그 메이저에 묶는다"
        fail=1
        continue
    fi

    repo=$(cut -d/ -f1,2 <<<"$path")

    # **페이지네이션을 안 탄다.** `tags?per_page=100` 은 첫 장만 보므로, 판을
    # 오래 안 올리면 태그가 밖으로 밀려 멀쩡한 핀이 빨간불이 된다.
    if ! object=$(gh api "repos/$repo/git/ref/tags/$version" --jq '.object.type + " " + .object.sha' 2>&1); then
        echo "::error file=$file::$path — $repo 의 태그 '$version' 을 못 읽었다: $object"
        fail=1
        continue
    fi

    read -r kind actual <<<"$object"
    # 주석 달린 태그는 한 겹 더 푼다. 안 풀면 태그 객체와 커밋을 비교하게 된다 —
    # `gradle/actions` 가 그 상태로 핀돼 있었다.
    if [[ "$kind" == tag ]]; then
        if ! actual=$(gh api "repos/$repo/git/tags/$actual" --jq '.object.sha' 2>&1); then
            echo "::error file=$file::$path — 태그 객체를 못 풀었다: $actual"
            fail=1
            continue
        fi
    fi

    if [[ "$actual" != "$rev" ]]; then
        echo "::error file=$file::$path — 핀과 주석이 어긋난다." \
             "주석=$version 은 $actual 인데 핀은 $rev 다"
        fail=1
    fi
done <<<"$refs"

# 검사 대상이 0 이면 정규식이나 경로가 어긋난 것이다. 통과시키지 않는다.
if [[ $checked -eq 0 ]]; then
    echo "::error::검사한 서드파티 액션이 없다 — 검사가 헛돈다"
    fail=1
fi

exit $fail

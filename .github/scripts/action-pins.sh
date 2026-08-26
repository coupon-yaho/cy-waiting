#!/usr/bin/env bash
# 서드파티 액션이 커밋 SHA 로 핀됐고, 그 SHA 가 락파일이 적은 판과 맞는지 본다.
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
# **검사 자체는 네트워크를 안 탄다.** 원격에 물어서 판정하면 두 가지가 구별이 안
# 된다 — 조직이 IP 허용 목록을 걸어 못 묻는 것과, 아예 없는 저장소를 못 묻는 것.
# 앞은 공급망 신호가 아니고 뒤는 그 자체가 사고인데, 응답은 똑같이 실패다. 어느
# 쪽으로 정해도 틀린다: 막으면 게이트가 흔들리고, 넘기면 공격자가 없는 저장소를
# 적어 지나간다. 실제로 `aquasecurity` 가 러너에서 403 을 냈고, 관대하게 바꾸자
# 곧바로 가짜 저장소가 통과했다.
#
# 그래서 **원격에 묻는 일을 검사에서 떼어 `--refresh` 로 옮겼다.** 결과는
# 락파일에 커밋되고, 그 파일의 diff 가 곧 "이 SHA 는 이 판이다" 라는 주장이 되어
# 사람이 리뷰한다. `go.sum` 이 하는 일과 같다 — 처음 볼 때 확인하고, 그 뒤로는
# 바뀌었는지만 본다.
set -uo pipefail

root=$(git rev-parse --show-toplevel 2>/dev/null) || root=.
cd "$root" || exit 1

readonly LOCK=.github/action-pins.lock

# 우리 조직 액션은 **판 주석·락파일 규칙만** 면제한다. 판을 우리가 올리므로
# 주석이 뒤처지는 것은 사고가 아니다. 핀 자체는 그대로 요구한다 — 그 잡에
# Atlassian 토큰이 붙는데, `@v1` 로 바꿔도 아무 말 안 하면 제외가 구멍이 된다.
readonly OURS='coupon-yaho/'

# 모든 `uses:` 를 YAML 로 열거한다. 셸 검사(workflow-shell.sh)와 같은 방식이다.
list_uses() {
    python3 - <<'PY'
import pathlib, sys

try:
    import yaml
except ImportError:
    print("::error::PyYAML 이 없어 액션 핀을 못 읽는다", file=sys.stderr)
    sys.exit(1)

# **없는 것과 사라진 것을 가른다.** 워크플로 디렉터리 자체가 없으면 검사 대상이
# 아니다 — 픽스처 저장소가 그렇다. 있는데 비었으면 검사가 사라진 것이라 막는다.
# (`workflow-shell.sh` 와 같은 규칙이다.)
workflows = pathlib.Path('.github/workflows')
if not workflows.is_dir():
    sys.exit(0)

# composite action 도 본다. `.github/workflows` 만 보면 `setup-gradle` 안의
# 서드파티 액션이 통째로 빠진다 — dependabot.yml 이 같은 이유로 경로를 나눈다.
# **중첩까지 훑는다.** 한 겹만 보면 액션 안의 액션이 통째로 검사에서 빠지고,
# 빠지는 것이 곧 이 검사가 막으려던 실패 형태다.
targets = sorted(workflows.glob('*.y*ml')) \
    + sorted(pathlib.Path('.github/actions').rglob('action.y*ml'))

if not targets:
    print("::error::워크플로 디렉터리가 비었다", file=sys.stderr)
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
}

# 원문을 줄 단위로 읽어 **항목마다** 따로 본다.
#
# **맵으로 들면 안 된다.** 참조를 키로 삼으면 같은 핀이 여러 번 나올 때 마지막에
# 읽힌 주석이 전부를 대신한다 — 한 줄의 틀린 판 주석이 다른 줄의 맞는 주석에
# 가려지고, 그 가림은 아무 흔적을 안 남긴다. 파일까지 키에 넣어도 한 파일 안에서
# 같은 문제가 남는다.
occurrences=$(
    grep -rHnoE "uses:[[:space:]]*[\"']?[^\"'[:space:]]+[\"']?([[:space:]]*#[[:space:]]*\S+)?" \
        .github/workflows .github/actions 2>/dev/null \
        | sed -E "s/:([0-9]+):uses:[[:space:]]*[\"']?/\t\1\t/; s/[\"']?[[:space:]]*#[[:space:]]*/\t/"
)

refs=$(list_uses) || exit 1

# 검사 대상 자체가 없는 저장소다. 위에서 0 으로 끝냈다.
[[ -z "$refs" ]] && exit 0

# **YAML 이 본 수와 원문이 본 수가 같아야 한다.** 원문 훑기가 못 보는 표기(여러
# 줄로 쓴 값 같은 것)가 있으면 그 항목은 검사에서 통째로 빠진다.
yaml_count=$(grep -c . <<<"$refs")
raw_count=$(grep -c . <<<"$occurrences")
if [[ "$yaml_count" != "$raw_count" ]]; then
    echo "::error::uses 항목 수가 안 맞는다 — YAML $yaml_count, 원문 $raw_count." \
         "원문 훑기가 못 보는 표기가 있다"
    exit 1
fi

# ── 판을 원격에 물어 락파일을 다시 쓴다 ──────────────────────────────────────
# 검사가 아니라 갱신이다. 결과는 커밋되어 사람이 diff 로 본다.
if [[ "${1:-}" == "--refresh" ]]; then
    fail=0
    : >"$LOCK.tmp"
    while IFS=$'\t' read -r file line ref version; do
        [[ -z "$ref" ]] && continue
        [[ "$ref" == ./* || "$ref" == .github/* || "$ref" == docker://* ]] && continue
        path=${ref%@*}
        rev=${ref##*@}
        [[ "$ref" != *@* || ! "$rev" =~ ^[0-9a-f]{40}$ ]] && continue
        [[ "$path" == "$OURS"* ]] && continue
        [[ -z "$version" ]] && continue
        repo=$(cut -d/ -f1,2 <<<"$path")

        if ! remote=$(timeout 30 git ls-remote "https://github.com/$repo" \
                "refs/tags/$version" "refs/tags/$version^{}" 2>&1); then
            echo "::error::$repo 에 못 물었다: $remote" >&2
            fail=1
            continue
        fi
        # `^{}` 가 있으면 주석 달린 태그다. 그쪽이 커밋이고 위는 태그 객체다 —
        # 안 풀면 태그 객체와 커밋을 비교하게 된다. `gradle/actions` 가 그랬다.
        actual=$(awk -v v="refs/tags/$version^{}" '$2 == v { print $1 }' <<<"$remote")
        [[ -z "$actual" ]] \
            && actual=$(awk -v v="refs/tags/$version" '$2 == v { print $1 }' <<<"$remote")

        if [[ -z "$actual" ]]; then
            echo "::error::$repo 에 태그 '$version' 이 없다" >&2
            fail=1
        elif [[ "$actual" != "$rev" ]]; then
            echo "::error::$path — 핀과 주석이 어긋난다." \
                 "주석=$version 은 $actual 인데 핀은 $rev 다" >&2
            fail=1
        else
            printf '%s\t%s\t%s\n' "$path" "$rev" "$version" >>"$LOCK.tmp"
        fi
    done <<<"$occurrences"

    if [[ $fail -ne 0 ]]; then
        rm -f "$LOCK.tmp"
        echo "::error::어긋난 핀이 있어 락파일을 안 고쳤다" >&2
        exit 1
    fi
    sort -u "$LOCK.tmp" >"$LOCK"
    rm -f "$LOCK.tmp"
    echo "락파일 갱신: $(wc -l <"$LOCK") 건"
    exit 0
fi

# ── 검사 ────────────────────────────────────────────────────────────────────
if [[ ! -f "$LOCK" ]]; then
    echo "::error::$LOCK 이 없다. \`.github/scripts/action-pins.sh --refresh\` 로 만든다"
    exit 1
fi

declare -A locked
while IFS=$'\t' read -r path sha version; do
    [[ -n "$path" ]] && locked["$path@$sha"]="$version"
done <"$LOCK"

fail=0
checked=0

while IFS=$'\t' read -r file line ref version; do
    [[ -z "$ref" ]] && continue

    # 이 저장소 안의 액션. 우리 코드라 공급망 경계가 아니다.
    [[ "$ref" == ./* || "$ref" == .github/* ]] && continue

    if [[ "$ref" == docker://* ]]; then
        # 태그는 움직인다. 다이제스트만 받는다.
        [[ "$ref" == *"@sha256:"* ]] \
            || { echo "::error file=$file,line=$line::$ref — docker 액션은 다이제스트로 핀한다"; fail=1; }
        checked=$((checked + 1))
        continue
    fi

    checked=$((checked + 1))
    path=${ref%@*}
    rev=${ref##*@}

    if [[ "$ref" != *@* || ! "$rev" =~ ^[0-9a-f]{40}$ ]]; then
        echo "::error file=$file,line=$line::$ref — 커밋 SHA 40자로 핀해야 한다." \
             "태그와 브랜치는 움직인다"
        fail=1
        continue
    fi

    [[ "$path" == "$OURS"* ]] && continue

    if [[ -z "$version" ]]; then
        echo "::error file=$file,line=$line::$path — 핀 옆에 판 주석이 없다." \
             "없으면 무엇이 핀됐는지 사람도 Dependabot 도 모른다"
        fail=1
        continue
    fi

    if [[ ! "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "::error file=$file,line=$line::$path — 주석이 정확한 판이 아니다: '$version'." \
             "메이저만 적으면 Dependabot 이 그 메이저에 묶는다"
        fail=1
        continue
    fi

    # **락파일에 없으면 막는다.** 새 핀이나 바뀐 핀은 `--refresh` 를 거쳐야 하고,
    # 그때 원격에 물은 결과가 락파일 diff 로 남아 사람이 본다.
    entry=${locked["$path@$rev"]:-}
    if [[ -z "$entry" ]]; then
        echo "::error file=$file,line=$line::$path@$rev 이 $LOCK 에 없다." \
             "\`.github/scripts/action-pins.sh --refresh\` 로 갱신하고 같이 커밋한다"
        fail=1
    elif [[ "$entry" != "$version" ]]; then
        echo "::error file=$file,line=$line::$path — 주석과 락파일이 어긋난다." \
             "주석=$version 인데 락파일=$entry 다"
        fail=1
    fi
done <<<"$occurrences"

# 검사 대상이 0 이면 정규식이나 경로가 어긋난 것이다. 통과시키지 않는다.
if [[ $checked -eq 0 ]]; then
    echo "::error::검사한 서드파티 액션이 없다 — 검사가 헛돈다"
    fail=1
fi

exit $fail

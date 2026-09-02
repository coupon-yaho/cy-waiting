#!/usr/bin/env bash
# 서드파티 액션이 커밋 SHA 로 핀됐고, 그 SHA 가 락파일이 적은 버전과 맞는지 본다.
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
# 락파일에 커밋되고, 그 파일의 diff 가 곧 "이 SHA 는 이 버전이다" 라는 주장이 되어
# 사람이 리뷰한다. `go.sum` 이 하는 일과 같다 — 처음 볼 때 확인하고, 그 뒤로는
# 바뀌었는지만 본다.
set -uo pipefail

root=$(git rev-parse --show-toplevel 2>/dev/null) || root=.
cd "$root" || exit 1

readonly LOCK=.github/action-pins.lock

# 우리 조직 액션은 **버전 주석·락파일 규칙만** 면제한다. 버전을 우리가 올리므로
# 주석이 뒤처지는 것은 사고가 아니다. 핀 자체는 그대로 요구한다 — 그 잡에
# Atlassian 토큰이 붙는데, `@v1` 로 바꿔도 아무 말 안 하면 제외가 구멍이 된다.
readonly OURS='coupon-yaho/'

# 모든 `uses:` 를 YAML 로 열거한다. 셸 검사(workflow-shell.sh)와 같은 방식이다.
#
# **주석도 여기서 같이 뽑는다.** 원문을 따로 훑으면 `run: echo "uses: ..."` 같은
# 평범한 문자열이나 주석 속 예시까지 걸려, 두 목록의 개수가 어긋나 멀쩡한
# 저장소에서 검사가 멎는다. 노드 위치를 알면 그 줄에서만 주석을 읽으면 된다.
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


def walk(node, out, in_steps=False, job_depth=0, at_root=True):
    """스텝과 잡의 `uses` 를 모은다. 위치를 알아야 그 줄에서 주석을 읽을 수 있다.

    **이름이 `uses` 인 것을 다 모으면 안 된다.** composite action 의 `inputs`
    아래에 그 이름의 입력을 둘 수 있고, 그건 액션 참조가 아니라 값이다.

    **재사용 워크플로 호출은 `jobs.<id>.uses` 라 `steps` 아래가 아니다.** 스텝만
    보면 그 줄이 통째로 검사 밖이고, 거기에 `secrets: inherit` 를 붙이면 저장소
    시크릿이 남의 저장소가 정하는 코드로 넘어간다.
    """
    if isinstance(node, yaml.MappingNode):
        for key, value in node.value:
            name = getattr(key, 'value', None)
            if (in_steps or job_depth == 1) and name == 'uses' \
                    and isinstance(value, yaml.ScalarNode):
                out.append((value.start_mark.line, value.value))
            # `steps` 아래의 항목만 스텝이다. `inputs`·`outputs` 로 내려가면 끈다.
            #
            # **잡은 거부목록으로 안 가른다.** 잡 참조는 `jobs.<id>.uses` 딱 한
            # 겹이라 깊이로 표현된다. 거부목록으로 두면 `outputs` 나
            # `strategy.matrix` 아래의 `uses` 라는 이름을 액션으로 잘못 읽고,
            # 그건 MUST 게이트의 오탐이라 우회를 부른다.
            #
            # **뿌리에서만 잡 컨테이너로 친다.** 아무 데서나 `jobs` 라는 이름에
            # 깊이를 되돌리면, `jobs` 라는 이름의 잡이 자기 참조를 검사 밖으로
            # 밀어낸다 — 게이트를 우회하는 이름을 짓기만 하면 된다.
            walk(value, out,
                 name == 'steps' or (in_steps and name not in
                                     ('inputs', 'outputs', 'env', 'with')),
                 2 if (at_root and name == 'jobs') else max(job_depth - 1, 0),
                 False)
    elif isinstance(node, yaml.SequenceNode):
        for child in node.value:
            walk(child, out, in_steps, job_depth, False)


found = []
for path in targets:
    text = path.read_text()
    try:
        root = yaml.compose(text)
    except yaml.YAMLError as e:
        print(f"::error file={path}::YAML 을 못 읽는다: {e}", file=sys.stderr)
        sys.exit(1)
    if root is None:
        continue
    lines = text.splitlines()
    seen = []
    walk(root, seen, False)
    for index, ref in seen:
        raw = lines[index] if index < len(lines) else ''
        # 주석은 YAML 이 버리므로 그 줄에서 직접 읽는다. 값 뒤의 `#` 부터가 주석이다.
        comment = ''
        marker = raw.find('#', raw.find(ref) + len(ref)) if ref in raw else -1
        if marker >= 0:
            comment = raw[marker + 1:].strip()
        found.append(f"{path}\t{index + 1}\t{ref}\t{comment}")

print("\n".join(found))
PY
}

refs=$(list_uses) || exit 1

# 검사 대상 자체가 없는 저장소다. 위에서 0 으로 끝냈다.
[[ -z "$refs" ]] && exit 0


# ── 버전을 원격에 물어 락파일을 다시 쓴다 ──────────────────────────────────────
# 검사가 아니라 갱신이다. 결과는 커밋되어 사람이 diff 로 본다.
if [[ "${1:-}" == "--refresh" ]]; then
    fail=0
    : >"$LOCK.tmp"
    while IFS=$'\t' read -r file line ref version; do
        [[ -z "$ref" ]] && continue
        # `$/` 는 러너 2.336 이후의 같은 저장소 참조다. SHA 를 요구하지 않는다.
        [[ "$ref" == ./* || "$ref" == .github/* || "$ref" == '$/'* \
            || "$ref" == docker://* ]] && continue
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
    done <<<"$refs"

    if [[ $fail -ne 0 ]]; then
        rm -f "$LOCK.tmp"
        echo "::error::어긋난 핀이 있어 락파일을 안 고쳤다" >&2
        exit 1
    fi
    # **쓰기 실패를 성공으로 보고하지 않는다.** 여기서 조용히 넘어가면 갱신했다고
    # 믿은 채로 옛 락파일이 남고, 다음 검사가 새 핀을 막는다.
    if ! sort -u "$LOCK.tmp" >"$LOCK"; then
        rm -f "$LOCK.tmp"
        echo "::error::$LOCK 을 못 썼다" >&2
        exit 1
    fi
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
    # `$/` 는 러너 2.336 이후의 같은 저장소 참조 표기다.
    [[ "$ref" == ./* || "$ref" == .github/* || "$ref" == '$/'* ]] && continue

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
        echo "::error file=$file,line=$line::$path — 핀 옆에 버전 주석이 없다." \
             "없으면 무엇이 핀됐는지 사람도 Dependabot 도 모른다"
        fail=1
        continue
    fi

    if [[ ! "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "::error file=$file,line=$line::$path — 주석이 정확한 버전이 아니다: '$version'." \
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
done <<<"$refs"

# 검사 대상이 0 이면 정규식이나 경로가 어긋난 것이다. 통과시키지 않는다.
if [[ $checked -eq 0 ]]; then
    echo "::error::검사한 서드파티 액션이 없다 — 검사가 헛돈다"
    fail=1
fi

exit $fail

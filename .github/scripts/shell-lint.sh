#!/usr/bin/env bash
# 셸 정적 검사.
#
# **저장소에 셸이 마흔 개가 넘는다.** 훅·CI 검사·부하 하네스가 전부 셸이고,
# 그중 여럿이 게이트를 열고 닫는다. 문법이 맞는 것과 뜻대로 도는 것은 다르다 —
# 따옴표를 빠뜨린 변수 하나가 공백 있는 경로에서만 틀리고, 그 사실은 그런 경로가
# 처음 들어온 날에 드러난다.
#
# **error 급만 막는다.** warning 급은 이 저장소에서 대부분 거짓 양성이다 —
# 라이브러리를 읽어 쓰는 변수를 shellcheck 가 "안 쓴다" 로 본다.
set -uo pipefail

root=$(git rev-parse --show-toplevel 2>/dev/null) || root=.
cd "$root" || exit 1

# **없으면 막는다. 건너뛰지 않는다.** 건너뛰면 이 검사가 안 도는 것이 통과로
# 보이고, 도구가 없는 사람만 늘 초록을 본다. 고치는 법은 우회가 아니라 설치다.
if ! command -v shellcheck >/dev/null 2>&1; then
    {
        echo "  shellcheck 가 없어 검사를 못 한다"
        echo "  러너에는 깔려 있다 (ubuntu-latest). 로컬에서는 아래 중 하나로 넣는다:"
        echo "    dnf install ShellCheck   ·   apt install shellcheck   ·   brew install shellcheck"
        echo "    또는 https://github.com/koalaman/shellcheck 릴리스의 정적 바이너리를 PATH 에 둔다"
    } >&2
    exit 1
fi

# **못 읽는 파일을 목록으로 든다.** shellcheck 는 한글 함수 이름에서 파싱을
# 멈추고, 그러면 그 파일의 나머지가 통째로 안 검사된다. 그것을 모르고 지나가면
# "검사했다" 가 거짓이 된다. 여기 적힌 것은 **검사 안 한 파일**이다.
#
# 시험 픽스처의 한글 식별자는 규칙이 허용한다 (식별자 규칙은 src/main 에만 건다).
# 이름을 영문으로 바꾸면 목록에서 뺄 수 있다.
UNPARSABLE=(
    test/load/gate-selftest.sh
)

# **벤더 파일은 뺀다.** 우리가 못 고치는 남의 코드라, 여기 잡히면 검사를
# 끄는 쪽으로 몰린다. 확장자가 없고 shebang 이 있어 아래 고르기에 걸린다.
VENDORED=(gradlew)

listed() {
    local f=$1 s
    shift
    for s in "$@"; do [ "$f" = "$s" ] && return 0; done
    return 1
}

skipped() { listed "$1" "${UNPARSABLE[@]}"; }
vendored() { listed "$1" "${VENDORED[@]}"; }

# **확장자로만 고르면 게이트를 여는 스크립트가 빠진다.** `.githooks/commit-msg`
# 는 커밋 메시지 게이트 그 자체인데 확장자가 없어, 이 검사가 처음 들어왔을 때
# 통째로 밖에 있었다. 확장자가 없는 파일은 shebang 으로 고른다.
targets=()
while IFS= read -r f; do
    skipped "$f" && continue
    vendored "$f" && continue
    case "${f##*/}" in
        *.sh) targets+=("$f") ;;
        *.*) ;;
        *) head -1 "$f" 2>/dev/null \
               | grep -qE '^#!.*[/ ](ba|da|k|z)?sh( |$)' && targets+=("$f") ;;
    esac
done < <(git ls-files)

[ ${#targets[@]} -gt 0 ] || { echo "  검사할 셸이 없다" >&2; exit 1; }

if ! shellcheck -S error "${targets[@]}"; then
    exit 1
fi

echo "  셸 ${#targets[@]} 개 통과 · 못 읽어 건너뛴 것 ${#UNPARSABLE[@]} 개"

# **건너뛴 파일이 실제로 못 읽는 것인지 확인한다.** 고쳐서 읽히게 된 뒤에도
# 목록에 남아 있으면, 그 파일은 영영 검사 밖이다.
for f in "${UNPARSABLE[@]}"; do
    if [ ! -f "$f" ]; then
        echo "  건너뛸 목록에 없는 파일이 있다: $f" >&2
        exit 1
    fi
    if shellcheck -S error "$f" >/dev/null 2>&1; then
        echo "  이제 읽을 수 있다 — 건너뛸 목록에서 뺀다: $f" >&2
        exit 1
    fi
done

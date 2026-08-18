#!/usr/bin/env bash
# 커밋 메시지 규약 검사. ai/rules/60-workflow.md (WF-1, WF-2)
#
# PreToolUse(Bash) 훅.
#
# 명령 탐지에 주의할 것: `git -c user.name=X commit -m ...` 처럼 git 과 commit 사이에
# 옵션이 끼면 "git commit" 부분 문자열 매칭은 조용히 빗나간다. 검사를 통과시키는
# 훅은 없는 훅보다 나쁘다 — 지켜지고 있다고 착각하게 만들기 때문이다.
set -uo pipefail

input=$(cat)
cmd=$(printf '%s' "$input" | jq -r '.tool_input.command // empty')

[[ -z "$cmd" ]] && exit 0
printf '%s' "$cmd" | grep -qE '(^|[[:space:];&|(])git([[:space:]]|$)' || exit 0
printf '%s' "$cmd" | grep -qE '[[:space:]]commit([[:space:]]|$)' || exit 0

# 메시지를 전달하지 않는 형태는 통과시킨다 (에디터·파일·재사용)
if ! printf '%s' "$cmd" | grep -qE '(^|[[:space:]])-m'; then
    if printf '%s' "$cmd" | grep -qE '(--no-edit|--amend|-F|--file|-C|--reuse-message|--fixup|--squash)'; then
        exit 0
    fi
    echo "[WF-1] 커밋 메시지를 -m 으로 전달한다. 에디터 커밋은 규약 검사를 우회한다." >&2
    exit 2
fi

# 첫 번째 -m 값(제목)만 뽑는다. -m 이 여럿이면 두 번째부터는 본문이다.
subject=$(printf '%s' "$cmd" \
    | grep -oE -- "-m[[:space:]]*(\"[^\"]*\"|'[^']*')" \
    | head -1 \
    | sed -E "s/^-m[[:space:]]*//; s/^[\"']//; s/[\"']$//")

if [[ -z "$subject" ]]; then
    echo "[WF-1] -m 뒤의 메시지를 따옴표로 감싼다." >&2
    exit 2
fi

types='feat|fix|test|refactor|perf|docs|build|ci|chore'
errors=()

printf '%s' "$subject" | grep -qE "^($types)(\([a-z0-9-]+\))?: .+" \
    || errors+=("형식이 맞지 않는다: '<type>(<scope>): <subject>' / 허용 type: $types")

# 글자 수가 아니라 **표시 폭**으로 센다. 한글은 터미널에서 두 칸을 차지하므로
# 50자로 재면 100칸이 되어 git log 가 줄바꿈된다. 50칸은 git 의 관례다.
chars=$(printf '%s' "$subject" | wc -m)
wide=$(printf '%s' "$subject" | grep -oP '[\x{AC00}-\x{D7A3}\x{3130}-\x{318F}]' | wc -l)
width=$((chars + wide))
((width > 50)) && errors+=("제목이 ${width}칸이다 (한글은 두 칸). 50칸 이내로 줄인다")

printf '%s' "$subject" | grep -qE '\.$' \
    && errors+=("제목 끝에 마침표를 쓰지 않는다")

# 제목은 요약이지 문장이 아니다. 한글 종결어미로 끝나면 서술문이다 —
# 명사형은 '다' 로 끝나지 않으므로 이 한 글자로 갈린다.
printf '%s' "$subject" | grep -qP '다$' \
    && errors+=("제목을 명사형으로 끝낸다. '~했다/한다' 는 요약이 아니라 문장이다")

# 제목은 한글로 쓴다. type·scope 만 영문이다.
printf '%s' "$subject" | grep -qP '[\x{AC00}-\x{D7A3}]' \
    || errors+=("제목을 한글로 쓴다 (type·scope 는 영문)")

printf '%s' "$subject" | grep -qE '\bCY-[0-9]+' \
    && errors+=("Jira 키는 제목이 아니라 'Refs: CY-###' 푸터에 둔다")

if ((${#errors[@]} > 0)); then
    {
        echo "커밋 메시지 규약 위반"
        echo "  제목: $subject"
        echo
        printf '  - %s\n' "${errors[@]}"
        echo
        echo "예시:"
        echo "  test(admission): 한산한 쿠폰의 무대기 통과 검증"
        echo "  feat(admission): 전역 크레딧 기반 통과 상한 산출"
        echo
        echo "푸터에 'Refs: CY-###' 를 남긴다."
        echo "규칙 전문: ai/rules/60-workflow.md"
    } >&2
    exit 2
fi
exit 0

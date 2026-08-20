#!/usr/bin/env bash
# 커밋 제목 규약 — 검증 규칙의 유일한 출처. ai/rules/60-workflow.md (WF-1)
#
# 이 파일을 두 곳이 쓴다.
#   - .claude/hooks/check-commit-msg.sh   Claude Code 가 부르는 PreToolUse 훅
#   - .githooks/commit-msg                git 이 부르는 훅 (터미널 직접 커밋)
#
# **규칙을 양쪽에 복사하지 않는다.** 사본이 생기면 한쪽만 고쳐지고, 그때부터
# 어느 쪽이 맞는지 알 수 없다. 도구 훅만 두면 터미널 커밋이 우회하고,
# 한쪽만 막으면 막지 않은 것과 같다.
#
# 사용: check_commit_subject "<제목>"  → 위반 메시지를 stdout 으로, 있으면 1

check_commit_subject() {
    local subject="$1"
    local types='feat|fix|test|refactor|perf|docs|build|ci|chore'
    local errors=()

    printf '%s' "$subject" | grep -qE "^($types)(\([a-z0-9-]+\))?: .+" \
        || errors+=("형식이 맞지 않는다: '<type>(<scope>): <subject>' / 허용 type: $types")

    # 글자 수가 아니라 **표시 폭**으로 센다. 한글은 터미널에서 두 칸을 차지하므로
    # 50자로 재면 100칸이 되어 git log 가 줄바꿈된다. 50칸은 git 의 관례다.
    local chars wide width
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

    ((${#errors[@]} == 0)) && return 0

    printf '  - %s\n' "${errors[@]}"
    return 1
}

# 위반 시 함께 보여줄 안내. 두 훅이 같은 문구를 쓴다.
commit_rule_help() {
    cat <<'EOF'

예시:
  test(admission): 한산한 쿠폰의 무대기 통과 검증
  feat(admission): 전역 크레딧 기반 통과 상한 산출

푸터에 'Refs: CY-###' 를 남긴다.
규칙 전문: ai/rules/60-workflow.md
EOF
}

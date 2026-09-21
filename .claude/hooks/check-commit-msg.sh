#!/usr/bin/env bash
# 커밋 메시지 규약 검사. ai/rules/60-workflow.md (WF-1, WF-2)
#
# PreToolUse(Bash) 훅.
#
# **두 낱말을 따로 찾으면 양쪽으로 틀린다.** 같은 명령일 필요도 인접할 필요도 없어서
# 읽기 전용 명령이 막히고, 경로를 붙여 부르면 검사가 통째로 지나간다. 낱말을 세지
# 않고 세그먼트마다 실제 부명령을 찾는다. 검사를 통과시키는 훅은 없는 훅보다 나쁘고,
# 오탐이 잦은 훅은 우회되어 결국 같은 곳에 도달한다.
set -uo pipefail

input=$(cat)
cmd=$(printf '%s' "$input" | jq -r '.tool_input.command // empty')

[[ -z "$cmd" ]] && exit 0

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

fail() {
    echo "$1" >&2
    exit 2
}

# 한 세그먼트가 커밋인지 보고, 맞으면 제목을 검사한다.
check_segment() {
    local seg=$1

    # 주석은 뗀다. 본문에 낱말이 들어 있다고 커밋으로 읽으면 안 된다.
    seg=${seg%%#*}
    [[ -z "${seg// /}" ]] && return 0

    local -a toks=()
    local t
    while IFS= read -r t; do
        [[ -n "$t" ]] && toks+=("$t")
    done < <(printf '%s\n' "$seg" | tr -s ' \t' '\n\n')
    ((${#toks[@]})) || return 0

    # 앞의 환경변수 대입은 건너뛴다. 그 뒤가 실제 도구다.
    local i=0
    while ((i < ${#toks[@]})) && [[ "${toks[i]}" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]]; do
        i=$((i + 1))
    done
    ((i < ${#toks[@]})) || return 0

    # **경로를 뗀다.** 붙여 부르면 이름 앞이 슬래시라, 낱말로 찾으면 안 걸린다.
    local tool=${toks[i]##*/}
    [[ "$tool" == "git" ]] || return 0

    # `-c 키=값`·`-C 경로` 는 부명령이 아니다. 건너뛰고 첫 부명령을 찾는다.
    local sub=""
    i=$((i + 1))
    while ((i < ${#toks[@]})); do
        case "${toks[i]}" in
            -c|-C|--git-dir|--work-tree|--namespace) i=$((i + 2)); continue ;;
            -*) i=$((i + 1)); continue ;;
            *) sub=${toks[i]}; break ;;
        esac
    done
    [[ "$sub" == "commit" ]] || return 0

    # **메시지를 안 싣는 형태는 통과시킨다** (에디터·파일·재사용).
    local has_msg=0 j
    for ((j = i; j < ${#toks[@]}; j++)); do
        # **glob 은 정규식이 아니다.** `[a-zA-Z]*` 는 한 글자 이상을 요구해서 `-m` 이
        # 안 걸린다. 짧은 플래그는 묶여 오므로 m 이 들어 있는지로 본다.
        case "${toks[j]}" in
            --message|--message=*) has_msg=1 ;;
            --*) ;;
            -*m*) has_msg=1 ;;
        esac
    done
    if ((!has_msg)); then
        for ((j = i; j < ${#toks[@]}; j++)); do
            case "${toks[j]}" in
                --no-edit|--amend|-F|--file|-C|--reuse-message|--fixup|--squash) return 0 ;;
            esac
        done
        fail "[WF-1] 커밋 메시지를 -m 으로 전달한다. 에디터 커밋은 규약 검사를 우회한다."
    fi

    # 첫 메시지 값(제목)만 뽑는다. 여럿이면 두 번째부터는 본문이다.
    local subject
    subject=$(printf '%s' "$seg" \
        | grep -oE -- "(-[a-zA-Z]*m|--message)[[:space:]=]*(\"[^\"]*\"|'[^']*')" \
        | head -1 \
        | sed -E "s/^(-[a-zA-Z]*m|--message)[[:space:]=]*//; s/^[\"']//; s/[\"']\$//")

    if [[ -z "$subject" ]]; then
        fail "[WF-1] -m 뒤의 메시지를 따옴표로 감싼다."
    fi

    # 규칙은 .githooks/lib/ 하나에만 둔다. 여기에 복사하면 git 훅과 갈라지고,
    # 그때부터 어느 쪽이 맞는지 알 수 없다.
    # shellcheck source=../../.githooks/lib/commit-subject-rules.sh
    source "$here/../../.githooks/lib/commit-subject-rules.sh"

    local violations
    if ! violations=$(check_commit_subject "$subject"); then
        {
            echo "커밋 메시지 규약 위반"
            echo "  제목: $subject"
            echo
            echo "$violations"
            commit_rule_help
        } >&2
        exit 2
    fi
}

# **세그먼트마다 본다.** 한 호출에 커밋이 둘이면 뒤엣것도 검사해야 한다.
# 끝에 개행을 두는 것은 없으면 마지막 줄에서 `read` 가 거짓을 내서다.
while IFS= read -r segment; do
    check_segment "$segment"
done < <(printf '%s\n' "$cmd" | sed 's/&&/\n/g; s/||/\n/g; s/[;|]/\n/g')

exit 0

#!/usr/bin/env bash
# PR 을 올리기 전에 로컬 리뷰를 강제한다.
#
# **왜 차단인가.** "올리기 전에 돌려라" 는 규범은 잊힌다. 실제로 잊었고,
# CodeRabbit 이 두 라운드에 걸쳐 14건을 지적했는데 그중 셋은 우리 자신의
# MUST 규칙 위반이었다 — 로컬에서 1초면 잡히는 것들이다.
#
# PreToolUse(Bash) 훅. `gh pr create` 를 만나면 기계 검사를 돌리고
# 위반이 있으면 exit 2 로 막는다.

set -uo pipefail

input=$(cat)
cmd=$(printf '%s' "$input" | jq -r '.tool_input.command // empty')

# PR 생성이 아니면 통과
[[ "$cmd" != *"gh pr create"* ]] && exit 0

# **검사를 못 돌리면 막는다.** 통과시키면 게이트가 인프라 오류 한 번에
# 조용히 사라진다 — 가드는 fail closed 여야 한다.
if ! ROOT=$(git rev-parse --show-toplevel 2>/dev/null); then
    echo "git 저장소가 아니라 로컬 리뷰를 돌릴 수 없다. PR 은 저장소 안에서 연다." >&2
    exit 2
fi
RUNNER="$ROOT/.claude/hooks/review-branch.sh"
if [[ ! -x "$RUNNER" ]]; then
    echo "로컬 리뷰 러너를 실행할 수 없다: $RUNNER" >&2
    echo "  chmod +x .claude/hooks/*.sh" >&2
    exit 2
fi

# base 를 명령에서 뽑는다. 없으면 develop
# **명령 문자열 전체를 훑지 않는다.** `--title "--base release"` 처럼 인용부호
# 안에 들어간 값을 옵션으로 착각한다. 인자를 토큰으로 쪼갠 뒤 옵션 자리만 본다.
#
# 실행하지 않고 쪼갠다 — `xargs` 는 셸 인용 규칙을 그대로 따르면서 명령을
# 부르지 않는다.
base=""
mapfile -t args < <(printf '%s' "$cmd" | xargs -n1 printf '%s\n' 2>/dev/null)
for ((i = 0; i < ${#args[@]}; i++)); do
    case "${args[i]}" in
        --base=*) base="${args[i]#--base=}"; break ;;
        -B=*)     base="${args[i]#-B=}";     break ;;
        --base|-B)
            base="${args[i + 1]:-}"
            break ;;
    esac
done
base="${base:-develop}"
# 이미 접두가 붙어 있으면 겹치지 않게 둔다. origin/origin/develop 이 되면
# 러너가 폴백을 타고, 폴백마저 없으면 브랜치 커밋을 하나도 안 보고 통과한다.
[[ "$base" != origin/* ]] && base="origin/$base"

out=$("$RUNNER" "$base" 2>&1)
status=$?

if ((status != 0)); then
    {
        echo "PR 을 올리기 전에 로컬 리뷰가 통과해야 한다."
        echo
        # 전체를 보여 준다. 걸러내면 정작 필요한 줄이 빠진다.
        printf '%s\n' "$out"
        echo
        echo "고친 뒤 다시 시도한다.  수동 실행: .claude/hooks/review-branch.sh $base"
        echo "전체 절차: /review"
    } >&2
    exit 2
fi

# 통과했어도 기계가 못 보는 것이 남는다 — 막지는 않고 알린다.
{
    echo "로컬 기계 검사 통과. 아직 안 한 것이 있는지 본다:"
    echo "  · ./gradlew build jacocoTestCoverageVerification pitest"
    printf '%s\n' "$out" | sed -n '/사람·에이전트가 볼 것/,$p' | sed 's/^/  /'
} >&2
exit 0

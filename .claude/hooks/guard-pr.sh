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

# **기본은 막는 쪽이다.** 앞 토큰 허용 목록으로 "명령 자리" 를 가리려 했더니
# 개행·`sudo`·`$( )`·파이프가 전부 빠져나갔다. 그래서 반대로 센다 — 주석과
# 히어독 본문만 걷어내고, 남은 자리에 그 명령이 있으면 막는다.
CREATE_RE='(^|[^[:alnum:]_.-])gh[[:space:]]+pr[[:space:]]+create([^[:alnum:]_-]|$)'

# 줄 이음은 먼저 붙인다. 안 붙이면 `gh pr \` 다음 줄의 `create` 가 안 보인다.
# 따옴표로 묶어야 패턴이 글자 그대로 쓰인다 — 안 묶으면 역슬래시가 다음 글자를
# 벗기는 뜻이 되어 줄바꿈만 지운다.
join=$'\\'$'\n'
cmd_joined=${cmd//"$join"/ }

# 사전 걸러내기는 본 판별과 같은 관용도여야 한다. 더 엄하면 공백 하나로 게이트를
# 통째로 지나간다.
cmd_naked=${cmd_joined//[\'\"]/}
cmd_naked=${cmd_naked//\\/}
[[ ! "$cmd_joined" =~ $CREATE_RE && ! "$cmd_naked" =~ $CREATE_RE ]] && exit 0

mapfile -t lines <<< "$cmd_joined"

# 문자열 안의 `#` 는 주석이 아니다. 걷어내면 그 뒤에 붙은 진짜 명령이 숨는다.
strip_comment() {
    local line=$1 i prefix sq dq
    for ((i = 0; i < ${#line}; i++)); do
        [[ "${line:i:1}" == '#' ]] || continue
        ((i > 0)) && [[ ! "${line:i-1:1}" =~ [[:space:]] ]] && continue
        prefix=${line:0:i}
        sq=${prefix//[^\']/}
        dq=${prefix//[^\"]/}
        ((${#sq} % 2 == 0 && ${#dq} % 2 == 0)) || continue
        printf '%s' "$prefix"
        return
    done
    printf '%s' "$line"
}

# `<<<` 는 히어독이 아니고, 문자열 안의 `<<` 도 아니다. **구분자가 뒤에 다시
# 나올 때만** 히어독으로 친다 — 가짜 구분자는 다시 안 나오므로 저절로 걸러지고,
# 진짜로 안 닫힌 히어독은 차단 쪽으로 떨어진다.
HEREDOC_RE='(^|[^<])<<-?[[:space:]]*['\''"]?([^[:space:]'\''"<;&|]+)'
opens_heredoc() {
    local line=$1 from=$2 j
    [[ "$line" =~ $HEREDOC_RE ]] || return 1
    delim=${BASH_REMATCH[2]}
    delim=${delim%[\'\"]}
    # `<<\EOF` 도 인용이다. 역슬래시는 구분자에 안 들어가므로 종결선은 EOF 다 —
    # 안 떼면 그 히어독이 영영 안 닫혀 본문이 명령으로 읽힌다.
    delim=${delim#\\}
    for ((j = from; j < ${#lines[@]}; j++)); do
        [[ "${lines[j]}" =~ ^[[:space:]]*"$delim"[[:space:]]*$ ]] && return 0
    done
    return 1
}

creating=0
delim=""
skip_to=-1
for ((n = 0; n < ${#lines[@]}; n++)); do
    if ((n <= skip_to)); then
        continue
    fi
    line=${lines[n]}
    if opens_heredoc "$line" $((n + 1)); then
        for ((m = n + 1; m < ${#lines[@]}; m++)); do
            [[ "${lines[m]}" =~ ^[[:space:]]*"$delim"[[:space:]]*$ ]] && { skip_to=$m; break; }
        done
    fi
    # **인용과 이스케이프를 걷고 한 번 더 본다.** `gh'' pr create` 나
    # `g\h pr create` 는 셸이 벗겨 같은 명령이 되는데 글자 그대로는 안 걸린다.
    bare=$(strip_comment "$line")
    naked=${bare//[\'\"]/}
    naked=${naked//\\/}
    if [[ "$bare" =~ $CREATE_RE || "$naked" =~ $CREATE_RE ]]; then
        creating=1
        create_line=$line
        break
    fi
done
((creating)) || exit 0

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
# 안에 들어간 값을 옵션으로 착각하고, 히어독 본문의 낱말까지 옵션으로 센다.
# 명령이 있던 줄만 토큰으로 쪼갠다.
#
# 실행하지 않고 쪼갠다 — `xargs` 가 인용을 벗기되 명령을 부르지 않는다. 짝이
# 안 맞으면 하드 에러이므로 그때는 기본값으로 안 넘어가고 막는다.
base=""
if ! mapfile -t args < <(printf '%s' "$create_line" | xargs -n1 printf '%s\n' 2>/dev/null); then
    echo "명령을 못 쪼갰다 — 어느 기준으로 볼지 모르므로 막는다." >&2
    exit 2
fi
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

# **에이전트를 돌렸다는 증거를 요구한다.**
#
# 전에는 여기서 "돌려 보라" 고 알리기만 했다. 알림은 잊힌다 — 실제로 매 PR
# 마다 건너뛰었고, 뒤늦게 한 번 돌렸더니 불변식 위반 하나와 치명 둘이 나왔다.
# 기계 검사와 같은 이유로 이것도 차단이어야 한다.
#
# 증거는 이 파일이다. 에이전트를 돌린 뒤 손으로 남긴다 — 무엇을 돌렸는지
# 사람이 적게 하는 것이 목적이라 자동 생성하지 않는다.
STAMP="$ROOT/.claude/.agents-reviewed"
head=$(git -C "$ROOT" rev-parse HEAD 2>/dev/null)

# 첫 줄은 커밋, 그 뒤는 **무엇을 돌렸는지**다. 목록이 비면 증거가 아니다 —
# 해시만 적고 넘어갈 수 있으면 이 게이트는 한 줄로 우회된다.
stamp_head=$(head -1 "$STAMP" 2>/dev/null)
stamp_list=$(tail -n +2 "$STAMP" 2>/dev/null | grep -c '[^[:space:]]')

if [[ ! -f "$STAMP" ]] || [[ "$stamp_head" != "$head" ]] || ((stamp_list == 0)); then
    {
        echo "기계 검사는 통과했다. **에이전트 리뷰가 남았다.**"
        echo
        printf '%s\n' "$out" | sed -n '/사람·에이전트가 볼 것/,$p' | sed 's/^/  /'
        echo
        echo "  기계는 형태만 본다. 판정 순서의 타당성, 불변식이 실제로"
        echo "  지켜지는지, 픽스처가 도달 불가 상태를 만드는지는 기계가 못 본다."
        echo
        echo "  돌린 뒤 증거를 남긴다:"
        echo "    git rev-parse HEAD > .claude/.agents-reviewed"
        echo "    echo '돌린 에이전트: domain-guardian, resilience-auditor, ...' \\"
        echo "      >> .claude/.agents-reviewed"
        echo
        echo "  ./gradlew build 도 아직이면 같이 돌린다. 뮤테이션은 main 으로 PR 을 열 때만."
    } >&2
    exit 2
fi

{
    echo "로컬 기계 검사 통과 · 에이전트 리뷰 확인:"
    sed 's/^/  /' "$STAMP" | tail -n +2
} >&2
exit 0

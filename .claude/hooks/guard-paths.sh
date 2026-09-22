#!/usr/bin/env bash
# 보호 경로 쓰기 차단. ai/rules/60-workflow.md (WF-5)
#
# PreToolUse(Write|Edit|Bash) 훅. waiting-legacy/ 는 참조 전용이다.
# 파일 도구뿐 아니라 Bash 경로도 막는다 — 한쪽만 막으면 막지 않은 것과 같다.
set -uo pipefail

input=$(cat)
file=$(printf '%s' "$input" | jq -r '.tool_input.file_path // empty')
cmd=$(printf '%s' "$input" | jq -r '.tool_input.command // empty')

deny() {
    {
        echo "[WF-5] waiting-legacy/ 는 참조 전용이다. 수정할 수 없다."
        echo
        echo "  차단된 대상: $1"
        echo
        echo "이 디렉터리는 제로베이스 재작성 이전 구현이며, 설계 논거를 참조하기"
        echo "위해서만 보존한다. 읽는 것은 자유, 쓰는 것은 금지다."
        echo "새 구현은 이 저장소의 src/ 에서 한다."
    } >&2
    exit 2
}

[[ "$file" == *"waiting-legacy/"* ]] && deny "$file"

if [[ -n "$cmd" && "$cmd" == *"waiting-legacy"* ]]; then
    # **읽기 전용만 통과시킨다.** 도구 이름을 세는 거부목록은 우회가 너무 쉬웠다 —
    # 절대경로로 부르면 이름 앞이 슬래시라 안 걸리고, 목록에 없는 수단이 전부
    # 지나갔다. 막는 형태 하나만 재던 자기검증은 그동안 초록이었다.
    # 모르는 것은 막는 쪽으로 뒤집는다.
    # **이름만으로 읽기 전용이라 할 수 없는 것은 뺀다.** `sort` 는 `-o` 로 파일을 쓰고,
    # `awk` 는 `system()` 으로 아무 명령이나 부르며, `sed` 는 `w` 로 쓴다. 인자까지
    # 가려내는 것은 목록을 세는 것보다 어렵다 — 읽기는 다른 도구로 된다.
    allow_re='^(cat|bat|less|more|head|tail|wc|nl|od|xxd|strings|file|stat|du|df|ls|eza|tree|find|fd|grep|rg|egrep|fgrep|diff|cmp|comm|uniq|cut|tr|column|realpath|readlink|dirname|basename|md5sum|sha1sum|sha256sum|jq|yq|git|echo|printf)$'

    # **치환 안은 못 본다.** 세그먼트의 첫 도구만 보므로 `$( )` 나 역따옴표 안의 명령이
    # 통째로 빠지는데, 껍질은 그 안쪽을 **먼저** 실행한다. 안전하게 못 가르면 막는다.
    printf '%s' "$cmd" | grep -qE '[$]\(|`' && deny "$cmd"

    # **디렉터리에 들어가는 것부터 막는다.** Bash 도구는 호출 사이에 위치를 지키므로,
    # 들어간 뒤의 상대경로 쓰기는 명령 문자열에 이름이 없어 이 훅을 통째로 지나간다.
    printf '%s' "$cmd" | grep -qE '(^|[[:space:];&|(])cd[[:space:]]' && deny "$cmd"

    # 리다이렉션은 도구와 무관하게 쓴다.
    printf '%s' "$cmd" | grep -qE '>[[:space:]]*[^[:space:]]*waiting-legacy' && deny "$cmd"

    # 세그먼트마다 첫 낱말을 본다. 경로를 떼고 이름만 남겨 절대경로 호출을 같이 잡는다.
    while IFS= read -r seg; do
        [[ -z "${seg// /}" ]] && continue
        # 앞의 환경변수 대입은 건너뛴다. 대입 뒤에 오는 것이 실제 도구다.
        tool=$(printf '%s' "$seg" | tr -s ' \t' '\n\n' | grep -v '^$' \
            | grep -vE '^[A-Za-z_][A-Za-z0-9_]*=' | head -1)
        [[ -z "$tool" ]] && continue
        tool=${tool##*/}
        tool=${tool#\\}
        printf '%s' "$tool" | grep -qE "$allow_re" || deny "$cmd"
        # **읽기 도구여도 쓰는 갈래가 있다.** 그 갈래는 이름으로 못 가른다.
        case "$tool" in
            find)
                printf '%s' "$seg" \
                    | grep -qE -- '-(delete|exec|execdir|ok|okdir|fprintf|fprint0|fprint|fls)' \
                    && deny "$cmd" ;;
            git)
                # `-C 경로`·`-c 키=값` 뒤의 낱말은 부명령이 아니다. 그것을 건너뛰고 찾는다.
                sub=$(printf '%s' "$seg" | tr -s ' \t' '\n\n' | grep -v '^$' \
                    | awk 'NR > 1 {
                        if (skip) { skip = 0; next }
                        if ($0 == "-C" || $0 == "-c" || $0 == "--git-dir" || $0 == "--work-tree") {
                            skip = 1; next
                        }
                        if ($0 ~ /^-/) next
                        print; exit
                    }')
                case "$sub" in
                    log|show|diff|status|blame|cat-file|ls-files|ls-tree|rev-parse) ;;
                    grep|describe|shortlog|for-each-ref|branch|tag|remote|reflog|'') ;;
                    *) deny "$cmd" ;;
                esac ;;
        esac
    # **끝에 개행을 둔다.** 없으면 `read` 가 마지막 줄에서 거짓을 내, 세그먼트가
    # 하나뿐인 명령에서 루프가 한 번도 안 돈다 — 가드가 통째로 지나간다.
    done < <(printf '%s\n' "$cmd" | sed 's/&&/\n/g; s/||/\n/g; s/[;|]/\n/g')
fi

exit 0

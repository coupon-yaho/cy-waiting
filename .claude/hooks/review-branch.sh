#!/usr/bin/env bash
# 브랜치 전체 로컬 리뷰. PR 을 올리기 전에 돌린다.
#
# **왜 필요한가.** check-java.sh · check-lua.sh 는 PostToolUse(Write|Edit) 훅이라
# 그 도구로 쓴 파일만 본다. 힙독이나 스크립트로 쓴 파일은 훅을 통째로 지나간다 —
# 실제로 그렇게 들어간 JS-6·JS-12·JS-13 위반이 CodeRabbit 까지 갔다.
#
# 이 스크립트는 **파일을 어떻게 만들었든** 브랜치의 변경 전체를 같은 검사에 태운다.
# 검사 내용을 여기 복사하지 않고 기존 훅을 그대로 호출한다 — 사본이 생기면 갈라진다.
#
# 사용:  .claude/hooks/review-branch.sh [base]      기본 base 는 origin/develop
#        .claude/hooks/review-branch.sh --self-test 검사가 실제로 무는지 확인

set -uo pipefail
ROOT_SCRIPT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")
cd "$(git rev-parse --show-toplevel)" || exit 1

HOOKS=".claude/hooks"
# 자기검증이 갈아 끼운다 — 저장소의 진짜 색인 상태에 결과가 흔들리면 안 된다.
JOURNAL_INDEX="ai/journal/index.md"
findings=0

say()  { printf '%s\n' "$*"; }
head2() { printf '\n\033[1m%s\033[0m\n' "$*"; }

# 기존 훅을 그 훅의 입력 형식으로 호출한다.
run_hook() {
    local hook=$1 file=$2 out
    # **절대경로로 넘긴다.** 훅은 `*/src/test/*` 로 테스트 여부를 가르는데
    # git 은 선행 슬래시 없는 상대경로를 준다 — 그대로 넘기면 테스트가
    # 프로덕션 규칙으로 검사되어 오탐이 나고, 동시에 테스트 전용 검사는
    # 아예 안 돈다. 이미 절대경로면 그대로 둔다.
    [[ "$file" != /* ]] && file="$PWD/$file"
    out=$(jq -nc --arg f "$file" '{tool_input:{file_path:$f}}' \
          | "$HOOKS/$hook" 2>&1)
    [[ -n "$out" ]] && { printf '%s\n' "$out"; return 1; }
    return 0
}

# ── .coderabbit.yaml 의 path_instructions 중 기계로 볼 수 있는 것 ─────────────
# 나머지(설계 의도·판정 순서의 타당성)는 .claude/agents/ 가 본다.

# JS-12 · 값/상태 객체의 public 생성자 (class 한정. record 는 언어 제약)
check_js12() {
    local file=$1
    # **주석을 걷어낸 뒤 실제 선언만 본다.** 파일 전문을 훑으면 `// record Service(`
    # 같은 주석 한 줄이 진짜 위반을 면제해 버린다.
    local code
    code=$(awk '{
        line = $0
        sub(/\/\/.*/, "", line)
        if (line ~ /^[[:space:]]*\*/)  line = ""
        if (line ~ /^[[:space:]]*\/\*/) line = ""
        printf "%d:%s\n", NR, line
    }' "$file")

    printf '%s\n' "$code" \
        | grep -E '^[0-9]+:[[:space:]]*public [A-Z][A-Za-z0-9_]*\(' \
        | while IFS=: read -r n rest; do
            local name
            name=$(printf '%s' "$rest" | sed -E 's/^[[:space:]]*public[[:space:]]+([A-Za-z0-9_]+).*/\1/')
            # 그 이름의 타입이 record 로 선언됐으면 정규 생성자라 막을 수 없다
            if printf '%s\n' "$code" \
               | grep -qE "^[0-9]+:.*(^|[[:space:]])record[[:space:]]+$name([[:space:]]|\()"; then
                continue
            fi
            printf '  JS-12 %s:%s public 생성자 — 정적 팩토리를 쓴다\n' "$file" "$n"
          done
}

# TS-11 · 약한 단언
check_ts11() {
    local file=$1
    grep -nE '\.(isNotEmpty|isNotNull|isNotZero)\(\)\s*;' "$file" 2>/dev/null \
        | sed "s|^|  TS-11 $file:|;s|:\s*| |" \
        | sed 's/$/  ← 무엇이 들어 있는지까지 단언한다/'
}

# TS-4 · 실제 시간 의존 / TS-7 · 비활성 테스트
check_test_misc() {
    local file=$1
    grep -nE 'Thread\.sleep|Instant\.now\(\)|System\.currentTimeMillis' "$file" 2>/dev/null \
        | sed "s|^|  TS-4 $file:|" | sed 's/$/  ← 시각·대기를 주입한다/'
    grep -nE '@Disabled|@RepeatedTest\(.*\)\s*//.*(불안정|flaky)' "$file" 2>/dev/null \
        | sed "s|^|  TS-7 $file:|" | sed 's/$/  ← 불안정 테스트를 덮지 않는다/'
}

# EX-1 · 정상 실패를 예외로
check_ex1() {
    local file=$1
    grep -nE 'throw new .*(SoldOut|QueueFull|Overload).*Exception' "$file" 2>/dev/null \
        | sed "s|^|  EX-1 $file:|" | sed 's/$/  ← 매진·큐 상한은 판정값이지 예외가 아니다/'
}

# RX-2 · Flux.interval
check_rx2() {
    local file=$1
    grep -nE 'Flux\.interval\(' "$file" 2>/dev/null \
        | sed "s|^|  RX-2 $file:|" | sed 's/$/  ← repeatWhen 을 쓴다. 회복 시 몰아서 터진다/'
}

# 작업 로그 형식·색인 동기화. CI(_verify-conventions.yml)와 **같은 것**을 본다.
# 여기서 안 보면 프론트매터를 통째로 빠뜨린 글이 푸시된 뒤에야 드러난다.
#
# 색인 경로를 인자로 받는다 — 자기검증이 저장소의 진짜 색인을 읽으면 누가
# 그 ID 를 실제로 등록하는 순간 검사가 아니라 상태가 결과를 바꾼다.
check_journal() {
    local file=$1 index=${2:-ai/journal/index.md} out="" k id front
    # **프론트매터 안만 본다.** 파일 전체를 훑으면 본문에 'date:' 한 줄만
    # 있어도 프론트매터가 있는 것으로 쳐서 검사가 통과한다.
    #
    # **닫는 --- 까지 확인한다.** 여는 줄만 보고 끝까지 읽으면 닫히지 않은
    # 파일에서 본문 전체가 프론트매터 행세를 해 같은 구멍이 다시 열린다.
    local status
    front=$(awk '
        NR == 1 { if ($0 != "---") exit 1; next }
        $0 == "---" { closed = 1; exit 0 }
        { print }
        END { if (!closed) exit 1 }
    ' "$file")
    status=$?
    if ((status != 0)); then
        printf '%s' "  JN-1 $file:1  ← 프론트매터가 없거나 --- 로 닫히지 않았다"$'\n'
        return
    fi
    for k in id date kind confidence; do
        printf '%s\n' "$front" | grep -qE "^$k:" \
            || out+="  JN-1 $file:1  ← 프론트매터에 '$k:' 없음"$'\n'
    done
    # 값 **전체**가 AIJ-<숫자> 여야 한다. 끝 경계가 없으면 'AIJ-123-extra' 가
    # 'AIJ-123' 으로 잘려 엉뚱한 ID 로 색인을 찾는다.
    id=$(printf '%s\n' "$front" | sed -nE 's/^id:[[:space:]]*(AIJ-[0-9]+)[[:space:]]*$/\1/p')
    if [[ -z "$id" ]]; then
        out+="  JN-1 $file:1  ← id 가 'AIJ-<숫자>' 형식이 아니다"$'\n'
    # 부분 일치를 막는다. 색인에 AIJ-1234 만 있어도 AIJ-123 이 통과했다.
    elif ! grep -qE "(^|[^0-9A-Za-z-])$id([^0-9]|$)" "$index" 2>/dev/null; then
        out+="  JN-2 $file:1  ← $id 가 색인에 없다 — 색인에서 빠지면 없는 것과 같다"$'\n'
    fi
    printf '%s' "$out"
}

review_files() {
    local -a files=("$@")
    local java=() lua=() journal=()
    for f in "${files[@]}"; do
        [[ -f "$f" ]] || continue
        case "$f" in
            *.java)                 java+=("$f") ;;
            *.lua)                  lua+=("$f") ;;
            */ai/journal/*/*/AIJ-*.md|ai/journal/*/*/AIJ-*.md) journal+=("$f") ;;
        esac
    done

    if ((${#java[@]})); then
        head2 "Java — 기존 훅 (JS-1·2·4·6·9·13·14 · DS-1 · RX-1 · LG-5·6)"
        local clean=1
        for f in "${java[@]}"; do run_hook check-java.sh "$f" || clean=0; done
        ((clean)) && say "  위반 없음"
        ((clean)) || findings=$((findings + 1))

        head2 "Java — 훅이 안 보는 것 (.coderabbit.yaml path_instructions)"
        local out=""
        for f in "${java[@]}"; do
            case "$f" in
                src/test/*|src/testFixtures/*|*/src/test/*|*/src/testFixtures/*)
                    out+=$(check_ts11 "$f")$'\n'
                    out+=$(check_test_misc "$f")$'\n' ;;
                *)
                    out+=$(check_js12 "$f")$'\n'
                    out+=$(check_ex1 "$f")$'\n'
                    out+=$(check_rx2 "$f")$'\n' ;;
            esac
        done
        out=$(printf '%s' "$out" | grep -v '^[[:space:]]*$')
        if [[ -n "$out" ]]; then say "$out"; findings=$((findings + 1)); else say "  위반 없음"; fi
    fi

    if ((${#lua[@]})); then
        head2 "Lua — RD-1·2·10"
        local clean=1
        for f in "${lua[@]}"; do run_hook check-lua.sh "$f" || clean=0; done
        ((clean)) && say "  위반 없음" || findings=$((findings + 1))
    fi

    if ((${#journal[@]})); then
        head2 "작업 로그 — 프론트매터·색인"
        local jout="" one
        for f in "${journal[@]}"; do
            # 명령 치환이 후행 줄바꿈을 먹는다. 그냥 이으면 앞 파일의 마지막
            # 지적과 다음 파일의 첫 지적이 한 줄에 붙는다.
            one=$(check_journal "$f" "$JOURNAL_INDEX")
            [[ -n "$one" ]] && jout+="$one"$'\n'
        done
        jout=$(printf '%s' "$jout" | grep -v '^[[:space:]]*$')
        if [[ -n "$jout" ]]; then say "$jout"; findings=$((findings + 1))
        else say "  위반 없음"; fi
    fi
}

# ── 자기검증 — 통과만 하는 검사는 검사가 아니다 ──────────────────────────────
self_test() {
    local tmp; tmp=$(mktemp -d); local fail=0
    probe() {  # 이름, 경로, 내용, 기대규칙
        local name=$1 path=$2 body=$3 want=$4
        mkdir -p "$(dirname "$tmp/$path")"; printf '%s' "$body" > "$tmp/$path"
        local got; got=$(review_files "$tmp/$path" 2>&1)
        if printf '%s' "$got" | grep -q "$want"; then
            printf '  ✓ %s\n' "$name"
        else
            printf '  ✗ %s — %s 를 못 잡았다\n' "$name" "$want"; fail=1
        fi
    }
    probe "public 생성자" "src/main/java/A.java" \
        $'class A {\n    public A(int x) {}\n}\n' "JS-12"
    probe "Javadoc 6줄 (check-java.sh 위임)" "src/main/java/B.java" \
        $'/**\n * 1\n * 2\n * 3\n * 4\n * 5\n * 6\n */\nclass B {}\n' "JS-6"
    probe "약한 단언" "src/test/java/CTest.java" \
        $'class CTest { void t() { assertThat(x).isNotEmpty(); } }\n' "TS-11"
    probe "실제 시간" "src/test/java/DTest.java" \
        $'class DTest { void t() { Thread.sleep(10); } }\n' "TS-4"
    probe "정상 실패를 예외로" "src/main/java/E.java" \
        $'class E { void f() { throw new SoldOutException(); } }\n' "EX-1"
    probe "Flux.interval" "src/main/java/F.java" \
        $'class F { void f() { Flux.interval(d).subscribe(); } }\n' "RX-2"
    # 주석 한 줄이 진짜 위반을 면제하던 회귀
    probe "주석 속 record 는 면제가 아니다" "src/main/java/G.java" \
        $'// record G(int x)\nclass G {\n    public G() {}\n}\n' "JS-12"
    # record 와 클래스가 한 파일에 섞여도 클래스만 잡는다
    probe "record 가 있어도 다른 클래스는 검사한다" "src/main/java/H.java" \
        $'public record Marker(int x) {}\n\nclass H {\n    public H() {}\n}\n' "JS-12"
    # **색인을 갈아 끼운다.** 저장소의 진짜 색인을 읽으면 누가 이 ID 를
    # 실제로 등록하는 순간 검사가 아니라 상태가 결과를 바꾼다.
    JOURNAL_INDEX="$tmp/index.md"
    printf '| [AIJ-9990](x.md) | 2026-08-20 | implement | 있음 | high | — |\n' \
        > "$JOURNAL_INDEX"

    # 프론트매터를 통째로 빠뜨린 글이 CI 까지 갔다. 검사가 실제로 무는지 본다.
    probe "저널 프론트매터 누락" "ai/journal/2026/08/AIJ-9998-probe.md" \
        $'# 제목\n\n- **날짜** 2026-08-20\n' "JN-1"
    probe "저널이 색인에 없음" "ai/journal/2026/08/AIJ-9999-probe.md" \
        $'---\nid: AIJ-9999\ndate: 2026-08-20\nkind: implement\nconfidence: high\n---\n\n# 제목\n' "JN-2"
    # 본문의 'date:' 한 줄이 프론트매터 행세를 하던 구멍
    probe "본문 키는 프론트매터가 아니다" "ai/journal/2026/08/AIJ-9997-probe.md" \
        $'# 제목\n\nid: AIJ-9997\ndate: 2026-08-20\nkind: implement\nconfidence: high\n' "JN-1"
    # 끝 경계가 없어 'AIJ-9990-extra' 가 'AIJ-9990' 으로 잘리던 구멍
    probe "id 는 값 전체가 맞아야 한다" "ai/journal/2026/08/AIJ-9996-probe.md" \
        $'---\nid: AIJ-9990-extra\ndate: 2026-08-20\nkind: implement\nconfidence: high\n---\n' "JN-1"
    # 여는 --- 만 있고 안 닫힌 파일이 본문까지 프론트매터로 치던 구멍
    probe "닫는 구분자가 없으면 프론트매터가 아니다" "ai/journal/2026/08/AIJ-9995-probe.md" \
        $'---\nid: AIJ-9990\ndate: 2026-08-20\nkind: implement\nconfidence: high\n\n# 제목\n' "JN-1"
    # 색인의 AIJ-9990 이 AIJ-999 를 통과시키던 구멍
    probe "색인 부분 일치는 등록이 아니다" "ai/journal/2026/08/AIJ-999-probe.md" \
        $'---\nid: AIJ-999\ndate: 2026-08-20\nkind: implement\nconfidence: high\n---\n' "JN-2"
    JOURNAL_INDEX="ai/journal/index.md"

    # **상대경로 회귀.** git 은 선행 슬래시 없는 경로를 준다. 절대경로로
    # 안 바꾸면 테스트 전용 검사가 아예 안 돌고(미탐), 동시에 테스트가
    # 프로덕션 규칙으로 검사된다(오탐). 둘 다 실제로 났다.
    #
    # 저장소 안의 빌드 경로에 둔다 — 러너가 git 루트에서 도는 것을 전제하므로
    # 저장소 밖으로 나가면 재현이 안 된다.
    local relprobe="build/review-selftest/src/test/java/RelTest.java"
    mkdir -p "$(dirname "$relprobe")"
    printf 'class RelTest {\n    void t() throws Exception {\n        Thread.sleep(10);\n    }\n}\n' \
        > "$relprobe"
    local rel; rel=$(review_files "$relprobe" 2>&1)
    rm -rf build/review-selftest
    if printf '%s' "$rel" | grep -q "TS-4"; then
        printf '  ✓ 상대경로에서도 테스트 검사가 돈다\n'
    else
        printf '  ✗ 상대경로에서 테스트 검사가 안 돈다 (미탐)\n'; fail=1
    fi
    # 헤더 줄에도 규칙 ID 가 적혀 있다. 지적 형식([RX-1])으로만 본다.
    if printf '%s' "$rel" | grep -q "\[RX-1\]"; then
        printf '  ✗ 테스트를 프로덕션 규칙으로 검사한다 (오탐)\n'; fail=1
    else
        printf '  ✓ 테스트를 프로덕션 규칙으로 검사하지 않는다\n'
    fi

    # **티켓 혼입 회귀.** `git add -A` 가 브랜치를 옮겨도 따라온 미추적 파일을
    # 쓸어 담아 다른 티켓의 코드가 섞였고 CI 가 깨졌다. 검사가 실제로 무는지 본다.
    local repo; repo="$tmp/mixed"
    mkdir -p "$repo" && (
        cd "$repo" || exit 1
        git init -q . && git config user.email t@t && git config user.name t
        : > seed.txt && git add seed.txt && git commit -q -m 'chore: 씨앗'
        git branch -q base
        : > a.txt && git add a.txt
        git commit -q -m 'feat(a): 하나' -m 'Refs: CY-1'
        : > b.txt && git add b.txt
        git commit -q -m 'feat(b): 둘' -m 'Refs: CY-2'
    ) >/dev/null 2>&1
    local mixed mixed_status
    mixed=$(cd "$repo" && "$ROOT_SCRIPT" base 2>&1); mixed_status=$?
    # 문구만 보면 findings 를 안 올리는 회귀를 놓친다. 종료 상태까지 본다.
    if printf '%s' "$mixed" | grep -q '티켓이 둘 이상' && ((mixed_status != 0)); then
        printf '  ✓ 티켓이 섞이면 알리고 실패로 끝난다\n'
    else
        printf '  ✗ 티켓이 섞였는데 통과시켰다 (exit %d)\n' "$mixed_status"; fail=1
    fi

    rm -rf "$tmp"
    findings=0
    return $fail
}

if [[ "${1:-}" == "--self-test" ]]; then
    head2 "자기검증 — 각 검사가 실제로 무는가"
    self_test && { say $'\n자기검증 통과'; exit 0; } || { say $'\n자기검증 실패'; exit 1; }
fi

BASE="${1:-origin/develop}"
# **다른 ref 로 대체하지 않는다.** 요청한 기준이 아닌 것을 보면 검사 결과가
# 실제 PR 과 어긋나고, 어긋난 통과는 통과가 아니다.
if ! git rev-parse --verify "$BASE^{commit}" >/dev/null 2>&1 \
   || ! git merge-base "$BASE" HEAD >/dev/null 2>&1; then
    say "기준을 해석할 수 없다: $BASE"
    say "  git fetch 하거나 기준을 인자로 준다 — 이 상태로는 무엇이 바뀌었는지 알 수 없다"
    exit 1
fi

# 커밋된 것만 보면 **아직 안 커밋한 위반을 놓친다.** 개발 중에 돌릴 때가
# 오히려 더 중요하므로 작업 트리까지 합친다.
mapfile -t CHANGED < <({
    git diff --name-only "$BASE"...HEAD
    git diff --name-only HEAD
    git diff --name-only --cached
    git ls-files --others --exclude-standard
} | sort -u)

if ((${#CHANGED[@]} == 0)); then
    say "$BASE 대비 변경 없음"; exit 0
fi

head2 "$BASE 대비 변경 ${#CHANGED[@]}건 (작업 트리 포함)"
printf '  %s\n' "${CHANGED[@]}"

review_files "${CHANGED[@]}"

# ── 브랜치에 섞여 든 파일 ────────────────────────────────────────────────────
# `git add -A` 는 **브랜치를 옮겨도 따라온 미추적 파일**까지 쓸어 담는다.
# 실제로 다른 티켓의 Lua 와 테스트가 워크플로 브랜치에 딸려 가 CI 가 깨졌다.
# 커밋 푸터의 티켓과 변경 경로가 어긋나면 알린다.
# 기준을 못 읽으면 0 이 나와 조용히 통과한다. 위에서 이미 막았지만
# 여기서 다시 확인한다 — 이 검사만 따로 불릴 수도 있다.
if ! git rev-parse --verify "$BASE^{commit}" >/dev/null 2>&1; then
    head2 "기준을 해석할 수 없어 티켓 혼입을 못 본다: $BASE"
    findings=$((findings + 1))
fi
tickets=$(git log --format=%B "$BASE"..HEAD | grep -oE 'Refs: CY-[0-9]+' | sort -u | wc -l)
if ((tickets > 1)); then
    head2 "브랜치에 티켓이 둘 이상 섞였다"
    git log --format='  %h %s%n     %(trailers:key=Refs,valueonly)' "$BASE"..HEAD 2>/dev/null \
        | grep -v '^[[:space:]]*$'
    say ""
    say "  브랜치 하나에 티켓 하나다 (WF-3). CI 는 브랜치명에서 키를 뽑으므로"
    say "  섞이면 엉뚱한 티켓으로 전이된다. 쪼개거나 잘못 담긴 커밋을 뺀다."
    findings=$((findings + 1))
fi

head2 "사람·에이전트가 볼 것"
say "  기계 검사는 형태만 본다. 판정 순서의 타당성, 불변식이 실제로 지켜지는지,"
say "  픽스처가 도달 불가능한 상태를 만들 수 있는지는 .claude/agents/ 가 본다."
printf '\n'
# 경로 판정은 선행 슬래시를 요구하지 않는다 — git 이 주는 형식이 상대경로다.
all=" ${CHANGED[*]} "
[[ "$all" == *"/domain/"* ]]                          && say "  → domain-guardian"
[[ "$all" == *".lua"* || "$all" == *"/redis/"* ]]     && say "  → redis-cluster-checker"
[[ "$all" == *"src/test/"* || "$all" == *"src/testFixtures/"* ]] && say "  → test-quality-reviewer"
# 장애·회복 경로는 파일명으로 안 드러난다. 이름에 단서가 있을 때만 권한다.
[[ "$all" == *"esilience"* || "$all" == *"ircuit"* || "$all" == *"etry"* \
   || "$all" == *"ailover"* || "$all" == *"eader"* || "$all" == *"haos"* ]] \
    && say "  → resilience-auditor"
say "  → style-enforcer (항상)"

printf '\n'
((findings)) && { say "위반 있음 — 고치고 다시 돌린다"; exit 1; }
say "기계 검사 통과"

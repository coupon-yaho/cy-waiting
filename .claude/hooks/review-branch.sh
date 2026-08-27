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
    # **주석 줄은 보지 않는다.** "Instant.now() 를 부르지 않는다" 라고 적은
    # 문서가 위반으로 잡히면, 규칙을 설명한 것이 규칙 위반이 된다.
    grep -nE 'Thread\.sleep|Instant\.now\(\)|System\.currentTimeMillis' "$file" 2>/dev/null \
        | grep -vE '^[0-9]+:[[:space:]]*(//|\*|/\*)' \
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

# 작업 로그 프론트매터. CI(_verify-conventions.yml)와 **같은 스크립트**를 쓴다 —
# 여기서 규칙을 다시 구현하면 사본이 생기고 둘이 갈라진다.
#
# **색인 등록은 안 본다.** 색인이 생성물이라 저장소에 없다 (CY-303).
check_journal() {
    local out status
    out=$(.github/scripts/journal-index.sh --check 2>&1 >/dev/null)
    status=$?
    if [[ -n "$out" ]]; then
        printf '%s\n' "$out" | sed 's|^::error::|  JN-1 |'
    elif ((status != 0)); then
        # **종료 상태도 본다.** 출력 없이 실패하면 조용히 통과한다 —
        # 검사가 죽은 것을 "이상 없음" 으로 세는 자리다.
        printf '%s\n' "  JN-1 작업 로그 검사가 출력 없이 실패했다 (exit $status)"
    fi
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
        # 스크립트가 저장소 전체를 한 번에 본다 — 파일마다 부를 필요가 없다.
        local jout
        jout=$(check_journal)
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
    # 저널 검사는 생성 스크립트에 위임했다 — 여기서 다시 구현하면 사본이
    # 생긴다. 그 스크립트가 실제로 무는지를 **저장소 안**에서 본다.
    #
    # **임시 저장소로 옮기지 않는다.** 스크립트를 복사해 가면 못 찾고 실패한
    # 것을 "잡았다" 로 세게 된다 — 실제로 그렇게 통과하던 프로브가 있었다.
    # **고정 경로를 쓰지 않는다.** 같은 이름의 작업 파일이 있으면 덮어쓰고
    # 지운다 — 자기검증이 사람의 작업을 없애면 안 된다.
    #
    # 날짜 경로도 박지 않는다. 없는 달이면 만들다 실패하는데, 그때 프로브는
    # 안 생기고 검사는 계속 돌아 **없는 프로브로 통과**한다.
    # **템플릿은 X 로 끝나야 한다.** BSD/macOS mktemp 는 접미사가 붙은
    # 템플릿을 거부한다 — 거기서는 프로브가 안 생기고 검사는 계속 돈다.
    local jdir jtmp jprobe
    jdir=$(ls -d ai/journal/[0-9]*/[0-9]* 2>/dev/null | tail -1)
    #
    # mv -n 은 대상이 이미 있으면 **성공을 내면서 원본을 남긴다.** 옮겨졌는지
    # 원본이 사라진 것으로 확인한다 — 반환값만 믿으면 남은 원본이 저장소에
    # 굴러다니고, 지우는 쪽은 있지도 않은 .md 를 지운다.
    if [[ -z "$jdir" ]] \
        || ! jtmp=$(mktemp "$jdir/AIJ-9999-probe.XXXXXX" 2>/dev/null) \
        || { mv -n "$jtmp" "$jtmp.md" 2>/dev/null; [[ -e "$jtmp" ]]; }; then
        [[ -n "${jtmp:-}" ]] && rm -f "$jtmp"
        printf '  ✗ 저널 프로브를 못 만들었다 — 검사가 실제로 무는지 확인 못 함\n'
        fail=1
        jprobe=""
    else
        jprobe="$jtmp.md"
    fi
    [[ -n "$jprobe" ]] && printf '# 프론트매터 없음\n' > "$jprobe"
    if [[ -n "$jprobe" ]]; then
        if .github/scripts/journal-index.sh --check >/dev/null 2>&1; then
            printf '  ✗ 프론트매터 없는 저널을 통과시켰다\n'; fail=1
        else
            printf '  ✓ 프론트매터 없는 저널을 잡는다\n'
        fi
        rm -f "$jprobe"
    fi
    # 지우고 나면 다시 통과해야 한다. 안 그러면 프로브가 아니라 고장이다.
    if .github/scripts/journal-index.sh --check >/dev/null 2>&1; then
        printf '  ✓ 프로브를 지우면 다시 통과한다\n'
    else
        printf '  ✗ 프로브를 지웠는데도 실패한다 — 검사가 고장났다\n'; fail=1
    fi

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

    # 규칙을 설명한 주석이 규칙 위반으로 잡히던 오탐
    probe "주석 속 시각 표현은 위반이 아니다" "src/test/java/ITest.java" \
        $'class ITest {\n    // Instant.now() 를 부르지 않는다\n    void t() {}\n}\n' "위반 없음"

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
# **릴리스·핫픽스·통합은 예외다.** 여러 티켓을 모아 올리는 것이 이 브랜치들의
# 목적이라(WF-3), 여기서 막으면 규범이 규범을 막는다.
#
# `integration/*` 은 리뷰 도구의 사용량이 시간당 한 건이라 생긴 자리다. 브랜치는
# 하위 작업 단위로 잘게 두되, 논리적으로 한 문장인 것들을 여기서 묶어 PR 을 연다.
current_branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null)
tickets=$(git log --format=%B "$BASE"..HEAD | grep -oE 'Refs: CY-[0-9]+' | sort -u | wc -l)
case "$current_branch" in
    release/*|hotfix/*|integration/*) tickets=1 ;;
esac
if ((tickets > 1)); then
    head2 "브랜치에 티켓이 둘 이상 섞였다"
    git log --format='  %h %s%n     %(trailers:key=Refs,valueonly)' "$BASE"..HEAD 2>/dev/null \
        | grep -v '^[[:space:]]*$'
    say ""
    say "  브랜치 하나에 티켓 하나다 (WF-3). CI 는 브랜치명에서 키를 뽑으므로"
    say "  섞이면 엉뚱한 티켓으로 전이된다. 쪼개거나 잘못 담긴 커밋을 뺀다."
    findings=$((findings + 1))
fi

# ── CI 가 도는 규범 검사 ────────────────────────────────────────────────────
# **CI 와 같은 스크립트를 부른다.** 규칙을 여기 다시 구현하면 사본이 생기고,
# 사본은 갈라진다. 실제로 문서 링크와 훅 자기검증이 러너에 없어서, 파일을
# 지우고 링크를 안 고친 PR 이 CI 에서야 걸린 일이 반복됐다.
head2 "CI 규범 검사 — 같은 스크립트"
# **self-test.sh 는 여기서 안 부른다.** 그것이 guard-pr.sh 를 부르고,
# guard-pr.sh 가 이 러너를 부른다 — 넣었더니 무한 재귀로 멈췄다.
# 훅 자기검증은 `--self-test` 로 따로 돌린다.
for check in \
    "문서 링크:.github/scripts/doc-links.sh" \
    "작업 로그:.github/scripts/journal-index.sh --check" \
    "워크플로 셸:.github/scripts/workflow-shell.sh" \
    "Loki 라벨:.github/scripts/loki-labels.sh" \
    "대시보드 지표:.github/scripts/dashboard-queries.sh" \
    "액션 핀:.github/scripts/action-pins.sh"
do
    name="${check%%:*}"
    cmd="${check#*:}"
    # 스크립트가 없으면 통과시키지 않는다 — 없는 검사를 통과로 세면
    # 게이트가 파일 하나 지우는 것으로 사라진다.
    script="${cmd%% *}"
    if [[ ! -x "$script" ]]; then
        say "  $name — 실행할 수 없다: $script"
        findings=$((findings + 1))
        continue
    fi
    if out=$($cmd 2>&1); then
        say "  $name 통과"
    else
        say "  $name 위반"
        printf '%s\n' "$out" | sed 's|^::error[^:]*::|    |; s|^|    |' | head -20
        findings=$((findings + 1))
    fi
done

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

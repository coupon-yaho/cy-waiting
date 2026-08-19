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
    grep -nE '^\s*public [A-Z][A-Za-z0-9_]*\(' "$file" 2>/dev/null \
        | grep -vE 'record|interface' \
        | while IFS=: read -r n rest; do
            # 같은 파일이 record 면 정규 생성자라 막을 수 없다
            grep -qE '^\s*public record ' "$file" && continue
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

review_files() {
    local -a files=("$@")
    local java=() lua=()
    for f in "${files[@]}"; do
        [[ -f "$f" ]] || continue
        case "$f" in
            *.java) java+=("$f") ;;
            *.lua)  lua+=("$f") ;;
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

    rm -rf "$tmp"
    findings=0
    return $fail
}

if [[ "${1:-}" == "--self-test" ]]; then
    head2 "자기검증 — 각 검사가 실제로 무는가"
    self_test && { say $'\n자기검증 통과'; exit 0; } || { say $'\n자기검증 실패'; exit 1; }
fi

BASE="${1:-origin/develop}"
if ! git rev-parse --verify "$BASE" >/dev/null 2>&1; then
    # 폴백을 조용히 타면 브랜치 커밋을 하나도 안 보고 "통과" 를 낸다.
    # 게이트가 조용히 무력화되는 것이라, 못 찾으면 알리고 막는다.
    if git rev-parse --verify develop >/dev/null 2>&1; then
        say "기준 $BASE 을 못 찾아 develop 으로 본다"
        BASE=develop
    else
        say "기준 브랜치를 못 찾았다: $BASE"
        say "  fetch 하거나 기준을 인자로 준다 — 이 상태로는 무엇이 바뀌었는지 알 수 없다"
        exit 1
    fi
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

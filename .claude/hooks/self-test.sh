#!/usr/bin/env bash
# 훅 자기검증. TS-9 — 위반을 잡지 못하는 검사는 모든 코드를 통과시킨다.
#
# 통과(allow)뿐 아니라 **정상 코드를 잘못 막지 않는지**도 검증한다.
# 오탐이 잦은 훅은 우회되고, 우회된 훅은 없는 것과 같다.
#
# 실행: .claude/hooks/self-test.sh
set -uo pipefail

HOOKS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HOOKS/../.." && pwd)"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

pass=0
fail=0

# **로케일을 바꿔 같은 검사를 다시 돌린다.** 대괄호 범위는 콜레이션 순서를 따라서
# 개발자 로케일(ko_KR.UTF-8)에서만 물고 CI(C.UTF-8)에서는 조용히 새는 일이 실제로
# 있었다. 로케일에 기대는 판정은 로컬 통과가 아무 뜻이 없다.
locale_case() {   # 로케일 hook 내용 파일명 기대 설명
    local loc=$1; shift
    LC_ALL="$loc" file_case "$@"
}

file_case() {   # hook 내용 파일명 기대(block|allow) 설명
    local hook=$1 content=$2 name=$3 expect=$4 label=$5
    local path="$tmp/$name"
    mkdir -p "$(dirname "$path")"
    printf '%s\n' "$content" > "$path"

    printf '%s' "$(jq -nc --arg p "$path" '{tool_input: {file_path: $p}}')" \
        | "$HOOKS/$hook" >/dev/null 2>&1
    verdict $? "$expect" "$label"
}

bash_case() {   # hook 명령 기대 설명
    local hook=$1 cmd=$2 expect=$3 label=$4
    printf '%s' "$(jq -nc --arg c "$cmd" '{tool_input: {command: $c}}')" \
        | "$HOOKS/$hook" >/dev/null 2>&1
    verdict $? "$expect" "$label"
}

verdict() {
    local code=$1 expect=$2 label=$3
    local actual=allow
    ((code == 2)) && actual=block
    if [[ "$actual" == "$expect" ]]; then
        printf '  ok   %s\n' "$label"; pass=$((pass + 1))
    else
        printf '  FAIL %s (기대 %s, 실제 %s)\n' "$label" "$expect" "$actual"; fail=$((fail + 1))
    fi
}

echo "check-java.sh — 위반 검출"
file_case check-java.sh 'class A {
    private java.util.List<String> x;
}' 'src/main/java/A.java' block 'JS-1 FQDN'
file_case check-java.sh 'import java.util.*;' 'src/main/java/C.java' block 'JS-2 와일드카드'
file_case check-java.sh '@Data
class A {
}' 'src/main/java/D.java' block 'JS-9 @Data'
file_case check-java.sh '@AllArgsConstructor
class A {
}' 'src/main/java/D2.java' block 'JS-4 @AllArgsConstructor'
file_case check-java.sh 'class A {
    private static long eta(long a) {
        return a;
    }
}' 'src/main/java/E.java' block 'JS-13 private static 메서드'
file_case check-java.sh 'class A {
    boolean 낡았나() {
        return true;
    }
}' 'src/main/java/H.java' block 'JS-11 한글 메서드명'
file_case check-java.sh 'class A {
    private record 상태(int x) {
    }
}' 'src/main/java/H2.java' block 'JS-11 한글 레코드명'
file_case check-java.sh 'class A {
    // 한글 주석은 통과해야 한다
    void log() {
        System.out.println("한글 로그 메시지");
    }
}' 'src/main/java/H3.java' allow 'JS-11 주석·문자열은 오탐 아님'
file_case check-java.sh 'class A {
    void log() {
        System.out.println("한글 // 메시지");
    }
}' 'src/main/java/H7.java' allow 'JS-11 문자열 안의 // 가 마스킹을 안 깬다 (회귀)'
file_case check-java.sh 'class A {
    // 그는 "말했다
    void ok() {
    }
}' 'src/main/java/H8.java' allow 'JS-11 주석 안의 따옴표가 마스킹을 안 깬다 (회귀)'
file_case check-java.sh 'class A {
    char c = '"'"'가'"'"';
    void 안녕() {
    }
}' 'src/main/java/H9.java' block 'JS-11 문자열 뒤의 한글 식별자를 놓치지 않는다 (회귀)'
file_case check-java.sh 'class A {
    /* 한글 블록 주석 */ void ok2() {
    }
}' 'src/main/java/HA.java' allow 'JS-11 같은 줄 블록 주석 (회귀)'
file_case check-java.sh 'class A {
    /*
     * 여러 줄 한글 주석
     */
    void ok3() {
    }
}' 'src/main/java/HB.java' allow 'JS-11 여러 줄 블록 주석 (회귀)'

locale_case C.UTF-8 check-java.sh 'class A {
    boolean 낡았나() {
        return true;
    }
}' 'src/main/java/H4.java' block 'JS-11 로케일이 달라도 문다 (회귀)'
locale_case en_US.UTF-8 check-java.sh 'class A {
    private record 상태(int x) {
    }
}' 'src/main/java/H5.java' block 'JS-11 영어 로케일에서도 문다 (회귀)'
locale_case C.UTF-8 check-java.sh 'class A {
    // 한글 주석
    void log() {
        System.out.println("한글 로그");
    }
}' 'src/main/java/H6.java' allow 'JS-11 로케일 바뀌어도 주석·문자열은 오탐 아님'

file_case check-java.sh 'class A {
    private class Inner {
    }
}' 'src/main/java/G.java' block 'JS-14 non-static 중첩'
file_case check-java.sh 'class A {
    class Inner {
    }
}' 'src/main/java/G2.java' block 'JS-14 수식어 없는 중첩 (회귀)'

file_case check-java.sh '/** 한 줄 javadoc */
class Z {
    void a() {}
    void b() {}
    void c() {}
    void d() {}
    void e() {}
    void f() {}
}' 'src/main/java/Z.java' allow 'JS-6 한 줄 Javadoc 은 본문 0줄 (회귀) — 사본이 여기서 갈렸다'

file_case check-java.sh 'class N {
    @Nested
    class Inner {
    }
}' 'src/test/java/NTest.java' allow 'JS-14 @Nested 는 면제 (회귀) — static 이면 실행되지 않는다'

file_case check-java.sh 'class N2 {
    @Nested
    @DisplayName("설명")
    @Tag("slow")
    class Inner {
    }
}' 'src/test/java/N2Test.java' allow 'JS-14 @Nested 어노테이션 3개 (회귀) — 개수를 못 박지 않는다'

file_case check-java.sh 'class N3 {
    @Nested class Inner {
    }

    class Leaked {
    }
}' 'src/test/java/N3Test.java' block 'JS-14 @Nested 가 같은 줄이면 다음 중첩이 새지 않는다 (회귀)'

# 상대경로로 넘어오면 테스트가 프로덕션 규칙으로 검사된다. 러너가 절대경로로
# 넘기는지를 여기서 고정한다 — 이 회귀가 실제로 났다.
file_case check-java.sh 'class R {
    void t() throws Exception {
        Thread.sleep(10);
    }
}' 'src/test/java/RTest.java' allow 'RX-1 은 테스트 소스셋에 적용되지 않는다 (회귀)'
file_case check-java.sh 'class A {
    void f() {
        mono.block();
    }
}' 'src/main/java/I.java' block 'RX-1 블로킹'
file_case check-java.sh 'import org.springframework.stereotype.Component;' \
    'src/main/java/domain/K.java' block 'DS-1 도메인 순수성'
file_case check-java.sh 'class A {
    void f() {
        var t = Instant.now();
    }
}' 'src/main/java/domain/L.java' block 'DS-1 도메인 시계'
file_case check-java.sh '/**
 * 1
 * 2
 * 3
 * 4
 * 5
 * 6
 */
class A {
}' 'src/main/java/M.java' block 'JS-6 Javadoc 6줄'
file_case check-java.sh '/** 한 줄 */
class U {
    /**
     * 1
     * 2
     * 3
     * 4
     * 5
     * 6
     */
    void f() {
    }
}' 'src/main/java/U.java' block 'JS-6 한 줄 javadoc 이후도 검출 (회귀)'
file_case check-java.sh 'class A {
    void f() {
        log.info("token={}", token);
    }
}' 'src/main/java/N.java' block 'LG-6 토큰 로깅'
file_case check-java.sh 'class A {
    void f() {
        MDC.put("k", "v");
    }
}' 'src/main/java/O.java' block 'LG-5 MDC'

echo "check-java.sh — 정상 코드 오탐 방지"
file_case check-java.sh 'import java.util.List;
class A {
    private List<String> x;
}' 'src/main/java/B.java' allow 'JS-1 정상 import'
file_case check-java.sh 'class A {
    private int a;   // java.util.List 대신 배열을 쓴다
}' 'src/main/java/V.java' allow 'JS-1 줄 끝 주석 (회귀)'
file_case check-java.sh '/**
 * java.util.List 를 쓰지 않는 이유
 */
class A {
}' 'src/main/java/V2.java' allow 'JS-1 javadoc 안 (회귀)'
file_case check-java.sh 'class A {
    private static final int X = 1;
}' 'src/main/java/F.java' allow 'JS-13 상수'
file_case check-java.sh 'class A {
    private static Logger log = LoggerFactory.getLogger(A.class);
}' 'src/main/java/W.java' allow 'JS-13 static 필드 (회귀)'
file_case check-java.sh 'class A {
    public static A of(int x) {
        return new A();
    }
}' 'src/main/java/P.java' allow 'JS-12 정적 팩토리는 허용'
file_case check-java.sh 'class A {
    private static final class Inner {
    }
}' 'src/main/java/H.java' allow 'JS-14 static 중첩'
file_case check-java.sh '/** 한 줄 javadoc */
class S {
}' 'src/main/java/S.java' allow 'JS-6 한 줄 javadoc'
file_case check-java.sh 'class A {
    // RULE-EXCEPTION(RX-1): 종료 경로. 타임아웃 있음
    void f() {
        mono.block();
    }
}' 'src/main/java/J.java' allow 'RULE-EXCEPTION 윗줄'
file_case check-java.sh 'class A {
    void f() {
        mono.block();   // RULE-EXCEPTION(RX-1): 종료 경로
    }
}' 'src/main/java/J2.java' allow 'RULE-EXCEPTION 같은 줄'
file_case check-java.sh 'class A {
    void f() {
        mono.block();
    }
}' 'src/test/java/AT.java' allow 'RX-1 테스트는 예외'

file_case check-java.sh 'import org.springframework.stereotype.Component;
class CouponStates {
    void f() { mono.block(); }
}' 'src/testFixtures/java/com/kafkick/waiting/domain/coupon/CouponStates.java' allow \
    'testFixtures 도 테스트 소스셋 (회귀)'
file_case check-java.sh 'import org.springframework.stereotype.Component;
@Component
class A {
}' 'src/main/java/admission/Q.java' allow 'DS-1 도메인 밖은 허용'

echo "check-lua.sh"
file_case check-lua.sh "-- KEYS[1] queue:{couponId}
local n = redis.call('ZCARD', 'queue:{' .. cid .. '}')
return tostring(n)" 'a.lua' block 'RD-1 리터럴 키'
file_case check-lua.sh "-- KEYS[1] queue:{couponId}
local k = 'queue:{' .. cid .. '}'
local n = redis.call('ZCARD', k)
return tostring(n)" 'b.lua' block 'RD-1 변수 우회'
file_case check-lua.sh "local n = redis.call('ZCARD', KEYS[1])
return tostring(n)" 'c.lua' block 'RD-10 계약 주석 없음'
file_case check-lua.sh "-- KEYS[1] queue:{couponId}
local floor = redis.call('GET', 'maxscore:{' .. cid .. '}')
return tostring(floor)" 'e.lua' block 'RD-1 maxscore 리터럴 (A-9 신규 키)'
file_case check-lua.sh "-- KEYS[1] queue:{couponId}
-- KEYS[4] alive:{couponId}:{memberId}
redis.call('SET', KEYS[4], '1', 'EX', ARGV[2])
local n = redis.call('ZCARD', KEYS[1])
return tostring(n)" 'd.lua' allow 'RD-1 다중 인자 정상 (회귀)'

echo "guard-paths.sh"
file_case guard-paths.sh 'x' 'waiting-legacy/src/A.java' block 'WF-5 파일 쓰기'
file_case guard-paths.sh 'x' 'src/main/java/A.java' allow 'WF-5 일반 경로'
bash_case guard-paths.sh 'rm -rf ../waiting-legacy/src' block 'WF-5 Bash 삭제 (회귀)'
bash_case guard-paths.sh 'sed -i s/a/b/ ../waiting-legacy/x.java' block 'WF-5 Bash 수정 (회귀)'
bash_case guard-paths.sh 'rg AdmissionDecider ../waiting-legacy/src' allow 'WF-5 Bash 읽기는 허용'

echo "check-commit-msg.sh"
bash_case check-commit-msg.sh "git commit -m 'feat(admission): 상한 계산 추가'" allow '정상'
bash_case check-commit-msg.sh "git -c user.name=X -c user.email=Y commit -m '잘못된 메시지'" block 'git -c ... commit 탐지 (회귀)'
bash_case check-commit-msg.sh "git commit -m 'feat(x): 정상' -m '본문은 길어도 된다. 제목만 50자를 지킨다'" allow '-m 두 개 (회귀)'
bash_case check-commit-msg.sh "git commit -m '그냥 이것저것 고침'" block '형식 위반'
bash_case check-commit-msg.sh "git commit -m 'feat(admission): add idle cap'" block '영문 제목 (한글 강제)'
bash_case check-commit-msg.sh "git commit -m 'feat(admission): 상한 계산 추가.'" block '마침표'
bash_case check-commit-msg.sh "git commit -m 'fix(ci): 스모크가 아무것도 안 봤다'" block '종결어미 (요약이 아니라 문장)'
bash_case check-commit-msg.sh "git commit -m 'feat(admission): 전역 크레딧에서 상한을 구한다'" block '종결어미 ~한다'
bash_case check-commit-msg.sh "git commit -m 'feat(admission): 전역 크레딧에서 상한 산출'" allow '명사형 종결'
bash_case check-commit-msg.sh "git commit -m 'feat(admission): CY-123 상한 추가'" block '제목에 Jira 키'
bash_case check-commit-msg.sh "git commit -m 'feat(admission): 한산한 쿠폰의 통과 상한을 전역 크레딧에서 구한다'" block '50칸 초과 (한글 두 칸)'
bash_case check-commit-msg.sh "git commit -m 'feat(admission): 전역 크레딧으로 상한 계산'" allow '50칸 이내'
bash_case check-commit-msg.sh "git commit --amend --no-edit" allow '--amend --no-edit'
bash_case check-commit-msg.sh "git status" allow 'commit 아닌 명령'
bash_case check-commit-msg.sh "echo 'nothing to do with version control'" allow '무관한 명령'

# ── git commit-msg 훅 ────────────────────────────────────────────────────────
# 도구 훅만 검증하면 터미널 직접 커밋 경로가 비어 있다.
echo
echo ".githooks/commit-msg"

# git 훅은 0 통과 / 1 차단이다. Claude 훅의 exit 2 규약과 달라서
# verdict 를 그대로 쓰면 차단을 전부 통과로 읽는다.
git_case() {   # 메시지 기대 설명
    local msg=$1 expect=$2 label=$3
    local f; f=$(mktemp)
    printf '%s\n' "$msg" > "$f"
    "$ROOT/.githooks/commit-msg" "$f" >/dev/null 2>&1
    local rc=$?
    rm -f "$f"
    local actual=allow
    ((rc != 0)) && actual=block
    if [[ "$actual" == "$expect" ]]; then
        printf '  ok   %s\n' "$label"; pass=$((pass + 1))
    else
        printf '  FAIL %s (기대 %s, 실제 %s)\n' "$label" "$expect" "$actual"; fail=$((fail + 1))
    fi
}

git_case 'feat(app): 진입점 추가
Refs: CY-18' allow '정상'
git_case '진입점 추가
Refs: CY-18' block '형식 위반'
git_case 'feat(app): add entrypoint
Refs: CY-18' block '영문 제목'
git_case 'feat(app): 진입점을 추가했다
Refs: CY-18' block '종결어미'
git_case 'feat(app): 진입점 추가' block 'Refs 푸터 없음'
git_case 'feat(app): 진입점 추가
Refs: CY-18
Plan: 1.2.1' block '계획서 ID (커밋에 남기지 않는다)'
git_case 'feat(app): CY-18 진입점 추가
Refs: CY-18' block '제목에 Jira 키'
git_case 'Merge branch develop' allow '병합 커밋은 대상 아님'

# ── 브랜치 리뷰 러너 ─────────────────────────────────────────────────────────
# 이 러너가 없으면 힙독·스크립트로 쓴 파일은 어떤 검사도 안 받는다.
# 러너 자신의 자기검증을 여기서 함께 돌린다.
echo
echo '[review-branch.sh]'
if "$ROOT/.claude/hooks/review-branch.sh" --self-test >/dev/null 2>&1; then
    printf '  ok   러너 자기검증 (JS-12·JS-6·TS-11·TS-4·EX-1·RX-2)\n'; pass=$((pass + 1))
else
    printf '  FAIL 러너 자기검증 — 검사 중 하나가 위반을 못 잡는다\n'; fail=$((fail + 1))
fi

# ── PR 가드 ──────────────────────────────────────────────────────────────────
echo
echo '[guard-pr.sh]'
# **프로덕션 소스 트리에 쓰지 않는다.** 스크립트가 중간에 죽으면 public 생성자를
# 가진 JS-12 위반 파일이 도메인 패키지에 남는다. 저장소 루트의 임시 디렉터리에
# 두고 trap 을 건다 — gitignore 에 걸리면 러너의 변경 목록에 안 잡혀 무의미하다.
probe_dir="$ROOT/.selftest-probe"
probe="$probe_dir/Probe.java"
mkdir -p "$probe_dir"
# EXIT 트랩은 하나뿐이라 앞의 것을 덮는다. 둘 다 지우게 합친다.
trap 'rm -rf "$tmp" "$probe_dir"' EXIT
cat > "$probe" <<'PROBE'
class Probe {
    public Probe() {
    }
}
PROBE

# 러너가 이 파일을 실제로 본다는 것부터 확인한다. 안 보면 아래 차단 검증이
# 통과해도 그건 다른 이유로 막힌 것이다.
# 러너는 위반이 있으면 1 을 낸다. pipefail 아래서 파이프로 바로 받으면
# grep 이 맞아도 파이프라인이 실패로 읽힌다 — 출력을 먼저 담는다.
runner_out=$("$ROOT/.claude/hooks/review-branch.sh" 2>&1 || true)
probe_seen=0
printf '%s' "$runner_out" | grep -q '.selftest-probe/Probe.java' && probe_seen=1

printf '{"tool_input":{"command":"gh pr create --base develop"}}' \
    | "$ROOT/.claude/hooks/guard-pr.sh" >/dev/null 2>&1
blocked=$?

rm -rf "$probe_dir"

# **저장소 상태에 기대지 않는다.** 개발자의 작업 트리가 더럽다고 이 케이스를
# 건너뛰면 로컬에서만 조용히 통과하고 CI 에서 처음 드러난다 — 실제로 그렇게
# 났다. stash 로 씻는 것도 답이 아니다. 스크립트가 중간에 죽으면 남의 작업이
# stash 로 숨는다. **깨끗한 임시 저장소를 만들어 거기서 시험한다.**
clean_repo="$tmp/clean"
mkdir -p "$clean_repo/.claude/hooks" "$clean_repo/.github/scripts" \
         "$clean_repo/ai/journal/2026/08"
# 러너가 CI 규범 검사도 부른다. 스크립트가 없으면 "실행할 수 없다" 로 막는데
# (fail closed, 옳다) 그러면 깨끗한 케이스가 그 이유로 막혀 시험이 헛돈다.
# **픽스처가 조용히 실패하면 그 뒤 검사가 헛돈다.** 스크립트가 안 복사되면
# 러너가 "실행할 수 없다" 로 막고, 그건 우리가 재려던 것이 아니다.
if ! cp "$ROOT/.github/scripts/"*.sh "$clean_repo/.github/scripts/" 2>/dev/null \
    || ! chmod +x "$clean_repo/.github/scripts/"*.sh 2>/dev/null \
    || [[ ! -x "$clean_repo/.github/scripts/doc-links.sh" ]]; then
    printf '  FAIL 임시 저장소에 CI 스크립트를 못 넣었다 — 아래 검사가 무의미하다\n'
    fail=$((fail + 1))
fi
cp "$HOOKS/review-branch.sh" "$HOOKS/check-java.sh" "$HOOKS/check-lua.sh" \
   "$clean_repo/.claude/hooks/" 2>/dev/null
chmod +x "$clean_repo/.claude/hooks/"*.sh 2>/dev/null
(
    cd "$clean_repo" || exit 1
    # **초기 브랜치를 develop 과 분리한다.** init.defaultBranch=develop 인
    # 환경이면 `git branch develop` 이 실패하고, 그러면 기준과 HEAD 가 같아져
    # 빈 diff 를 검사하고 조용히 통과한다.
    git init -q -b selftest-base .
    git config user.email t@t
    git config user.name t
    : > seed.txt
    git add -A
    git commit -q -m 'chore: 씨앗'
    git branch -q develop
    git switch -q -c selftest-work
    printf 'class Ok {\n    private Ok() {\n    }\n}\n' > Ok.java
    git add -A
    git commit -q -m 'feat(a): 깨끗한 변경' -m 'Refs: CY-1'
) >/dev/null 2>&1

if ! cp "$HOOKS/guard-pr.sh" "$clean_repo/.claude/hooks/" \
    || ! chmod +x "$clean_repo/.claude/hooks/guard-pr.sh"; then
    printf '  FAIL 임시 저장소에 가드를 못 넣었다\n'; fail=$((fail + 1))
fi

# 가드는 에이전트 리뷰 증거도 요구한다. 여기서 보는 것은 **기계 검사 경로**라
# 증거를 만들어 두고 그 부분만 시험한다 — 증거 요구 자체는 아래에서 따로 본다.
if ! git -C "$clean_repo" rev-parse HEAD > "$clean_repo/.claude/.agents-reviewed" \
        2>/dev/null \
    || ! echo '돌린 에이전트: 자기검증' >> "$clean_repo/.claude/.agents-reviewed" \
    || [[ ! -s "$clean_repo/.claude/.agents-reviewed" ]]; then
    printf '  FAIL 증거 픽스처를 못 만들었다 — 깨끗한 케이스가 그 이유로 막힌다\n'
    fail=$((fail + 1))
fi

# 낡은 증거와 목록 없는 증거도 막아야 한다. 안 막으면 한 줄로 우회된다.
printf '%s\n돌린 에이전트: 옛 커밋\n' 0000000000000000000000000000000000000000 \
    > "$clean_repo/.claude/.agents-reviewed.stale"
stale=$(cd "$clean_repo" \
    && cp .claude/.agents-reviewed .claude/.agents-reviewed.bak \
    && cp .claude/.agents-reviewed.stale .claude/.agents-reviewed \
    && printf '{"tool_input":{"command":"gh pr create --base develop"}}' \
    | ./.claude/hooks/guard-pr.sh >/dev/null 2>&1; echo $?)
listless=$(cd "$clean_repo" \
    && git rev-parse HEAD > .claude/.agents-reviewed \
    && printf '{"tool_input":{"command":"gh pr create --base develop"}}' \
    | ./.claude/hooks/guard-pr.sh >/dev/null 2>&1; echo $?)
cp "$clean_repo/.claude/.agents-reviewed.bak" "$clean_repo/.claude/.agents-reviewed"

# 원격이 없는 임시 저장소라 origin/develop 이 없다. 원격 이름을 붙여 두어
# 가드가 실제와 같은 경로(origin/<base>)를 타게 한다.
git -C "$clean_repo" remote add origin "$clean_repo" >/dev/null 2>&1
git -C "$clean_repo" update-ref refs/remotes/origin/develop refs/heads/develop

# 기준과 HEAD 가 같으면 빈 diff 를 보는 것이라 시험이 무의미하다. 먼저 확인한다.
diff_ok=0
[[ -n "$(git -C "$clean_repo" diff --name-only origin/develop...HEAD 2>/dev/null)" ]] && diff_ok=1

clean=$(cd "$clean_repo" && printf '{"tool_input":{"command":"gh pr create --base develop"}}' \
    | "$clean_repo/.claude/hooks/guard-pr.sh" >/dev/null 2>&1; echo $?)

printf '{"tool_input":{"command":"git status"}}' \
    | "$ROOT/.claude/hooks/guard-pr.sh" >/dev/null 2>&1
unrelated=$?

# **증거가 없으면 막아야 한다.** 알리기만 하던 시절에 매 PR 마다 건너뛰었고,
# 뒤늦게 돌렸더니 불변식 위반 하나와 치명 둘이 나왔다.
rm -f "$clean_repo/.claude/.agents-reviewed"
nostamp=$(cd "$clean_repo" && printf '{"tool_input":{"command":"gh pr create --base develop"}}' \
    | "$clean_repo/.claude/hooks/guard-pr.sh" >/dev/null 2>&1; echo $?)
git -C "$clean_repo" rev-parse HEAD > "$clean_repo/.claude/.agents-reviewed" 2>/dev/null
echo '돌린 에이전트: 자기검증' >> "$clean_repo/.claude/.agents-reviewed"

# 검사를 못 돌리는 상황에서 통과시키면 게이트가 조용히 사라진다 (fail closed)
failclosed=$(cd /tmp && printf '{"tool_input":{"command":"gh pr create"}}' \
    | "$ROOT/.claude/hooks/guard-pr.sh" >/dev/null 2>&1; echo $?)

if ((probe_seen)); then
    printf '  ok   러너가 변경된 프로브 파일을 본다\n'; pass=$((pass + 1))
else
    printf '  FAIL 러너가 프로브를 못 본다 — 차단 검증이 무의미하다\n'; fail=$((fail + 1))
fi
if ((failclosed == 2)); then
    printf '  ok   저장소 밖에서는 막는다 (fail closed)\n'; pass=$((pass + 1))
else
    printf '  FAIL 저장소 밖인데 통과시켰다 (exit %d)\n' "$failclosed"; fail=$((fail + 1))
fi
if ((stale == 2)); then
    printf '  ok   낡은 증거는 막는다\n'; pass=$((pass + 1))
else
    printf '  FAIL 낡은 증거인데 통과시켰다 (exit %d)\n' "$stale"; fail=$((fail + 1))
fi
if ((listless == 2)); then
    printf '  ok   목록 없는 증거는 막는다\n'; pass=$((pass + 1))
else
    printf '  FAIL 목록이 없는데 통과시켰다 (exit %d)\n' "$listless"; fail=$((fail + 1))
fi
if ((nostamp == 2)); then
    printf '  ok   에이전트 리뷰 증거가 없으면 막는다\n'; pass=$((pass + 1))
else
    printf '  FAIL 증거가 없는데 통과시켰다 (exit %d)\n' "$nostamp"; fail=$((fail + 1))
fi
if ((blocked == 2)); then
    printf '  ok   실제 위반에서 PR 생성을 막는다\n'; pass=$((pass + 1))
else
    printf '  FAIL 위반이 있는데 PR 생성을 통과시켰다 (exit %d)\n' "$blocked"; fail=$((fail + 1))
fi
if ((diff_ok == 0)); then
    printf '  FAIL 임시 저장소의 기준과 HEAD 가 같다 — 빈 diff 를 검사한다\n'; fail=$((fail + 1))
elif ((clean == 0)); then
    printf '  ok   깨끗하면 막지 않는다\n'; pass=$((pass + 1))
else
    printf '  FAIL 깨끗한데 막았다 (exit %d)\n' "$clean"; fail=$((fail + 1))
fi
if ((unrelated == 0)); then
    printf '  ok   PR 생성이 아닌 명령은 건드리지 않는다\n'; pass=$((pass + 1))
else
    printf '  FAIL 무관한 명령을 막았다 (exit %d)\n' "$unrelated"; fail=$((fail + 1))
fi

echo
printf '통과 %d · 실패 %d\n' "$pass" "$fail"
((fail == 0))

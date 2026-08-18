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
    private class Inner {
    }
}' 'src/main/java/G.java' block 'JS-14 non-static 중첩'
file_case check-java.sh 'class A {
    class Inner {
    }
}' 'src/main/java/G2.java' block 'JS-14 수식어 없는 중첩 (회귀)'
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

echo
printf '통과 %d · 실패 %d\n' "$pass" "$fail"
((fail == 0))

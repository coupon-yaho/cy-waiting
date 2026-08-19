#!/usr/bin/env bash
# Java 규칙 검사. ai/rules/10-java-style.md · 20-design.md · 30-reactive.md · 80-logging.md
#
# PostToolUse(Write|Edit) 훅. 위반이 있으면 exit 2 로 차단하고 사유를 stderr 로 알린다.
#
# 검사는 **주석을 걷어낸 코드 뷰**에 대해 돈다. 주석 안의 예시 코드가 위반으로
# 잡히면 훅을 신뢰할 수 없게 되고, 신뢰할 수 없는 훅은 곧 우회된다.
# 예외 주석(RULE-EXCEPTION)은 원본 줄에서 확인한다 — 코드 뷰에는 남아 있지 않다.
set -uo pipefail

input=$(cat)
file=$(printf '%s' "$input" | jq -r '.tool_input.file_path // empty')

[[ -z "$file" || "$file" != *.java || ! -f "$file" ]] && exit 0

is_test=false
# testFixtures 도 테스트 소스셋이다 (T1.2.4). `*/src/test/*` 는 여기에 안 걸린다 —
# 그대로 두면 픽스처가 프로덕션 규칙으로 검사되어 오탐이 난다.
[[ "$file" == */src/test/* || "$file" == */src/testFixtures/* ]] && is_test=true
is_domain=false
[[ "$file" == */domain/* ]] && is_domain=true

# 주석을 지운 코드 뷰. "행번호:내용" 형식
code=$(awk '{
    line = $0
    sub(/\/\/.*/, "", line)                       # 줄 끝 주석
    if (line ~ /^[[:space:]]*\*/) line = ""       # 블록 주석 본문
    if (line ~ /^[[:space:]]*\/\*/) line = ""     # 블록 주석 시작
    printf "%d:%s\n", NR, line
}' "$file")

violations=()

# 코드 뷰에서 패턴을 찾고, 원본의 예외 주석을 존중한다.
#
# 예외 주석 인정 범위는 **같은 줄 또는 바로 위 3줄**이다. 위반 문장 바로 위에
# 다는 것도, 메서드 시그니처 위에 다는 것도 자연스러운 위치라 둘 다 받는다.
# 좁게 잡으면 정당한 예외가 막히고, 그때 사람은 훅을 고치는 대신 우회한다.
EXCEPTION_LOOKBACK=3

scan() {
    local rule=$1 pattern=$2 exclude=${3:-}
    local matches
    matches=$(printf '%s\n' "$code" | grep -E "$pattern" || true)
    [[ -n "$exclude" ]] && matches=$(printf '%s\n' "$matches" | grep -vE "$exclude" || true)

    local out="" m n from context
    while IFS= read -r m; do
        [[ -z "$m" ]] && continue
        n=${m%%:*}
        from=$((n > EXCEPTION_LOOKBACK ? n - EXCEPTION_LOOKBACK : 1))
        context=$(sed -n "${from},${n}p" "$file")
        [[ "$context" == *"RULE-EXCEPTION($rule)"* ]] && continue
        out+="  $n: $(sed -n "${n}p" "$file" | sed 's/^[[:space:]]*//')"$'\n'
    done <<< "$matches"
    printf '%s' "$out"
}

report() {
    local rule=$1 desc=$2 hits=$3
    [[ -z "$hits" ]] && return
    violations+=("[$rule] $desc"$'\n'"$hits")
}

# ── JS-1 FQDN 금지 ────────────────────────────────────────────────────────────
report "JS-1" "FQDN 대신 import 해서 짧은 이름으로 쓴다" \
    "$(scan 'JS-1' \
        '^[0-9]+:.*(^|[^[:alnum:]_."])(java|javax|jakarta)\.[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*\.[A-Z]' \
        '^[0-9]+:[[:space:]]*(import|package)')"

# ── JS-2 와일드카드 import ────────────────────────────────────────────────────
report "JS-2" "와일드카드 import 금지 — 무엇에 의존하는지 사라진다" \
    "$(scan 'JS-2' '^[0-9]+:import .*\.\*;')"

# ── JS-4 / JS-9 위험한 Lombok ─────────────────────────────────────────────────
report "JS-9" "@Data 금지 — 무엇이 생성되는지 알 수 없다. 필요한 것만 명시" \
    "$(scan 'JS-9' '^[0-9]+:[[:space:]]*@Data([[:space:]]|$)')"

report "JS-4" "@AllArgsConstructor/@SneakyThrows 금지 — 인자 뒤바뀜·예외 은닉" \
    "$(scan 'JS-4' '^[0-9]+:[[:space:]]*@(AllArgsConstructor|SneakyThrows)([[:space:](]|$)')"

# ── JS-13 private static 메서드 금지 ──────────────────────────────────────────
# 메서드 판별: '=' 없이 '(' 가 오는 선언. 상수·필드·중첩타입은 제외한다.
if [[ "$is_test" == false ]]; then
    report "JS-13" "private static 메서드 금지 — 인스턴스 메서드로 두거나 밖으로 꺼낸다" \
        "$(scan 'JS-13' \
            '^[0-9]+:[[:space:]]*private[[:space:]]+static[[:space:]]+[^=]*\(' \
            'private[[:space:]]+static[[:space:]]+(final[[:space:]]+)?(class|record|enum|interface)([[:space:]]|$)')"
fi

# ── JS-14 중첩 클래스는 static ────────────────────────────────────────────────
# 들여쓰기된 class 선언 = 중첩. 수식어가 없는 경우도 잡는다.
#
# **JUnit 5 의 @Nested 는 제외한다.** 그쪽은 static 이면 아예 실행되지 않는다 —
# 규칙과 프레임워크가 충돌하는 자리라 규칙이 진다. 여기서 오탐을 내면 사람은
# 훅을 고치는 대신 우회하고, 그러면 진짜 위반도 같이 지나간다.
# @Nested 이후 **선언까지의 연속 어노테이션 줄**을 전부 건너뛴다. 개수를 못
# 박으면 @Tag 하나 붙는 순간 오탐이 되고, 오탐이 나면 훅이 우회된다.
nested_class_lines=$(awk '
    /^[[:space:]]*@Nested([[:space:]]|\(|$)/ { pending=1; next }
    pending && /^[[:space:]]*@/               { next }
    pending                                   { print NR; pending=0 }
' "$file")
js14=$(scan 'JS-14' \
    '^[0-9]+:[[:space:]]+((public|protected|private|final|abstract)[[:space:]]+)*class[[:space:]]' \
    'static')
for n in $nested_class_lines; do
    js14=$(printf '%s\n' "$js14" | grep -vE "^[[:space:]]*$n:")
done
js14=$(printf '%s' "$js14" | grep -v '^[[:space:]]*$')
report "JS-14" "중첩 클래스는 static — 바깥 인스턴스를 붙들어 누수를 만든다" "$js14"

# ── JS-6 Javadoc 5줄 초과 (원본에서 검사한다) ─────────────────────────────────
hits=$(awk '
    {
        line = $0
        if (!in_doc && line ~ /\/\*\*/) {
            if (line ~ /\*\//) next          # 한 줄짜리 javadoc — 열고 닫는다
            in_doc = 1; count = 0; start = NR; next
        }
        if (in_doc && line ~ /\*\//) {
            if (count > 5) printf "  %d: javadoc 본문 %d줄\n", start, count
            in_doc = 0; next
        }
        if (in_doc) {
            sub(/^[[:space:]]*\*[[:space:]]?/, "", line)
            if (line !~ /^[[:space:]]*$/ && line !~ /^@/) count++
        }
    }
' "$file")
report "JS-6" "Javadoc 본문은 최대 5줄 — 넘으면 대개 설계가 문제다" "$hits"

# ── RX-1 블로킹 호출 ──────────────────────────────────────────────────────────
if [[ "$is_test" == false ]]; then
    report "RX-1" "블로킹 호출 금지 — 예외는 @PreDestroy 종료 경로뿐(타임아웃 필수)" \
        "$(scan 'RX-1' '^[0-9]+:.*(\.(block|blockFirst|blockLast)\(|Thread\.sleep\(|CountDownLatch)')"
fi

# ── LG-5 리액티브에서 MDC 금지 ────────────────────────────────────────────────
report "LG-5" "MDC/ThreadLocal 금지 — 스레드를 넘나들며 깨지거나 다른 요청 값이 섞인다" \
    "$(scan 'LG-5' '^[0-9]+:.*(\bMDC\b|ThreadLocal<)')"

# ── LG-6 비밀·개인정보 로깅 ───────────────────────────────────────────────────
report "LG-6" "토큰·비밀을 로그에 남기지 않는다 — 한 번 새면 회수할 수 없다" \
    "$(scan 'LG-6' \
        '^[0-9]+:.*log\.(trace|debug|info|warn|error)\(.*(token|secret|password|credential|authorization)' \
        '(tokenHeader|hasToken|tokenService|TokenService)')"

# ── DS-1 도메인 순수성 ────────────────────────────────────────────────────────
if [[ "$is_domain" == true && "$is_test" == false ]]; then
    report "DS-1" "도메인은 Spring·Reactor·Redis를 참조하지 않는다" \
        "$(scan 'DS-1' '^[0-9]+:import (org\.springframework|reactor\.|org\.redisson|io\.lettuce)')"

    report "DS-1" "도메인은 시계·난수를 직접 읽지 않는다 — Clock/난수원을 주입한다" \
        "$(scan 'DS-1' '^[0-9]+:.*(Instant\.now\(|System\.currentTimeMillis\(|LocalDateTime\.now\(|Math\.random\(|ThreadLocalRandom)')"
fi

# ── 결과 ──────────────────────────────────────────────────────────────────────
if ((${#violations[@]} > 0)); then
    {
        echo "규칙 위반: $file"
        echo
        printf '%s\n' "${violations[@]}"
        echo "규칙 전문: ai/rules/00-index.md"
        echo "정당한 예외라면 해당 줄 또는 바로 위 3줄 이내에"
        echo "  // RULE-EXCEPTION(<규칙ID>): <이유>"
        echo "를 달고 ai/journal/ 에 근거를 남긴다."
    } >&2
    exit 2
fi
exit 0

#!/usr/bin/env bash
# 빌드 스크립트의 식별자가 ASCII 영문인가 (JS-11 의 Gradle 판).
#
# **check-java.sh 는 .java 만 받는다.** 그래서 빌드 스크립트는 어떤 검사도 안 보고
# 있었고, 실제로 `def 어댑터클래스` 가 세 곳에서 쓰이며 그대로 지나갔다.
#
# 주석과 문자열을 지운 뒤 **선언 자리의 이름만** 본다 — `def`·`task`·타입 있는
# 선언 뒤의 이름, `ext.이름`, `ext { }` 블록 안의 대입 좌변,
# `tasks.register('이름')`. 문자열·주석의 한글은 정상이라(LG-9) 안 건드린다.
#
# Kotlin DSL(`.gradle.kts`) 도 같이 본다 — `val`·`var`·`fun` 과
# `tasks.register<Copy>("이름")` 의 타입 인자. `find` 에만 넣고 문법을 안 보면
# kts 파일이 "검사했다" 로 세어져 fail-closed 도 안 걸린다.
#
# **못 보는 것**: 클로저 파라미터(`{ 한글 -> }`), 동적 이름
# (`def n = "한글"; ext[n] = 1`). Groovy 를 제대로 파싱할 값어치가 아직 없다.
set -euo pipefail

root=${1:-.}
violations=0
scanned=0

while IFS= read -r -d '' file; do
    scanned=$((scanned + 1))
        # **while 을 파이프 뒤로 옮기지 않는다.** 파이프는 서브셸이라 카운터가
    # 안 늘고, 그러면 위반을 찾고도 조용히 통과한다.
    while IFS= read -r hit; do
        printf '  %s:%s\n' "$file" "$hit"
        violations=$((violations + 1))
    done < <(LC_ALL=C awk '
        function nonascii(w,   z) {
            for (z = 1; z <= length(w); z++) if (substr(w, z, 1) > "\177") return 1
            return 0
        }
        function report(name) { if (nonascii(name)) printf "%d: %s\n", NR, name }
        BEGIN { blk = 0; ext = 0; depth = 0 }
        {
            # 주석·문자열을 지운다. Groovy 는 삼중따옴표도 쓴다.
            out = ""; i = 1; n = length($0)
            while (i <= n) {
                c = substr($0, i, 1); two = substr($0, i, 2); three = substr($0, i, 3)
                if (tq != "") { if (three == tq) { tq = ""; i += 3 } else { i++ } ; continue }
                if (blk) { if (two == "*/") { blk = 0; i += 2 } else { i++ } ; continue }
                if (two == "/*") { blk = 1; i += 2; continue }
                if (three == "'"'"''"'"''"'"'" || three == "\"\"\"") { tq = three; i += 3; continue }
                if (two == "//") { break }
                if (c == "\"" || c == "'"'"'") {
                    q = c; i++; lit = ""
                    while (i <= n) {
                        d = substr($0, i, 1)
                        if (d == "\\") { lit = lit substr($0, i, 2); i += 2; continue }
                        i++
                        if (d == q) break
                        lit = lit d
                    }
                    # **태스크 이름은 문자열이지만 식별자다** — 사람이 ./gradlew 로
                    # 친다. 다만 여기서 봐야 한다. 마스킹 전에 원본을 훑으면
                    # 주석 속 예시(// tasks.register("한글"))와 문자열 안에 든
                    # 코드까지 위반으로 보고한다 — 실제로 그랬다.
                    if (out ~ /(register|create|task)([[:space:]]*<[^>]*>)?[[:space:]]*\([[:space:]]*$/)
                        report(lit)
                    out = out "\"\""            # 이름 자리를 지키되 내용은 지운다
                    continue
                }
                out = out c; i++
            }

            # ext { } 블록 안의 대입 좌변. 블록형이 Gradle 에서 제일 흔하다.
            if (ext) {
                if (match(out, /^[[:space:]]*[A-Za-z0-9_$\200-\377]+[[:space:]]*=/)) {
                    nm = substr(out, RSTART, RLENGTH)
                    gsub(/[[:space:]=]/, "", nm)
                    report(nm)
                }
                depth += gsub(/{/, "{", out) - gsub(/}/, "}", out)
                if (depth <= 0) ext = 0
                next
            }
            if (match(out, /(^|[^A-Za-z0-9_$.])ext[[:space:]]*{/)) {
                # **여는 줄도 본다.** next 로 빠지면 한 줄짜리 ext { 한글 = 1 } 을
                # 통째로 놓친다. 여는 중괄호 뒤부터 이어서 훑는다.
                rest = substr(out, RSTART + RLENGTH)
                while (match(rest, /[A-Za-z0-9_$\200-\377]+[[:space:]]*=/)) {
                    nm = substr(rest, RSTART, RLENGTH)
                    gsub(/[[:space:]=]/, "", nm)
                    report(nm)
                    rest = substr(rest, RSTART + RLENGTH)
                }
                depth = 1 + gsub(/{/, "{", rest) - gsub(/}/, "}", rest)
                ext = (depth > 0) ? 1 : 0
                next
            }

            # def·task·타입 있는 선언. 줄 맨 앞이 아니어도 잡는다.
            line = out
            while (match(line, /(^|[^A-Za-z0-9_$.])(def|task|val|var|fun)[[:space:]]+[A-Za-z0-9_$\200-\377]+/)) {
                nm = substr(line, RSTART, RLENGTH)
                sub(/^[^A-Za-z0-9_$]*(def|task|val|var|fun)[[:space:]]+/, "", nm)
                report(nm)
                line = substr(line, RSTART + RLENGTH)
            }
            # **제네릭 타입 인자도 타입 이름이다.** register<한글타입>("x") 처럼
            # 소비만 하면 그 자리로 샌다.
            line = out
            while (match(line, /<[^<>]*>/)) {
                arg = substr(line, RSTART + 1, RLENGTH - 2)
                m = split(arg, parts, /[^A-Za-z0-9_$\200-\377]+/)
                for (t = 1; t <= m; t++) if (parts[t] != "") report(parts[t])
                line = substr(line, RSTART + RLENGTH)
            }

            # 타입 있는 선언 — Groovy 는 def 없이도 된다. 줄 맨 앞만 본다
            # (메서드 호출과 헷갈리지 않게).
            if (match(out, /^[[:space:]]*((final|static)[[:space:]]+)*([A-Z][A-Za-z0-9_$]*|int|long|short|byte|char|float|double|boolean)(<[^>]*>)?(\[\])?[[:space:]]+[A-Za-z0-9_$\200-\377]+[[:space:]]*=/)) {
                nm = substr(out, RSTART, RLENGTH)
                sub(/[[:space:]]*=[[:space:]]*$/, "", nm)
                sub(/^.*[[:space:]]/, "", nm)
                report(nm)
            }

            # ext.이름  ·  project.ext.이름
            line = out
            while (match(line, /ext\.[A-Za-z0-9_$\200-\377]+/)) {
                nm = substr(line, RSTART + 4, RLENGTH - 4)
                report(nm)
                line = substr(line, RSTART + RLENGTH)
            }
        }' "$file")
done < <(find "$root" -type f \( -name '*.gradle' -o -name '*.gradle.kts' \) \
              -not -path '*/build/*' -not -path '*/.git/*' -print0)

# **검사할 것이 하나도 없으면 실패다.** 경로 오인이나 확장자 전환으로 대상이
# 사라져도 "통과" 를 찍으면, 그때부터 이 게이트는 영원히 초록이다.
if ((scanned == 0)); then
    echo "빌드 스크립트를 하나도 못 찾았다: $root" >&2
    exit 1
fi

if ((violations > 0)); then
    echo "빌드 스크립트 식별자는 ASCII 영문이다 (JS-11). 한글은 주석·문자열에만." >&2
    exit 1
fi
echo "빌드 스크립트 식별자 통과 ($scanned 파일)"

#!/usr/bin/env bash
# 작업 로그 색인을 **프론트매터에서 만든다.**
#
# 손으로 유지하면 동시에 도는 브랜치마다 같은 자리에서 충돌한다. 그런데 색인
# 한 줄에 들어가는 값은 이미 저널 파일 안에 다 있다 — 사본이 생기는 순간
# 둘이 갈라지고, 그때부터 CI 가 그 갈라짐을 막느라 일한다.
#
# 저장소에 커밋하지 않는다. 파일이 git 에 없으면 충돌 자체가 안 생긴다.
#
# 사용:  .github/scripts/journal-index.sh          표준출력으로 낸다
#        .github/scripts/journal-index.sh --check  프론트매터만 검사한다

set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

shopt -s nullglob
files=(ai/journal/*/*/AIJ-*.md)
fail=0

# 프론트매터 한 덩어리를 떼어 낸다. 첫 --- 로 열고 닫는 --- 까지만이다.
frontmatter() {
    awk '
        NR == 1 { if ($0 != "---") exit 1; next }
        $0 == "---" { closed = 1; exit 0 }
        { print }
        END { if (!closed) exit 1 }
    ' "$1"
}

field() {
    printf '%s\n' "$2" | sed -nE "s/^$1:[[:space:]]*(.*[^[:space:]])[[:space:]]*$/\1/p" | head -1
}

# 표 셀에 그대로 넣으면 값 안의 | 가 열 경계가 되어 색인이 어긋난다.
cell() {
    printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/|/\\|/g'
}

rows=""
for f in "${files[@]}"; do
    front=$(frontmatter "$f") || {
        echo "::error::$f 에 프론트매터가 없거나 --- 로 닫히지 않았다" >&2
        fail=1; continue
    }
    id=$(printf '%s\n' "$front" | sed -nE 's/^id:[[:space:]]*(AIJ-[0-9]+)[[:space:]]*$/\1/p' | head -1)
    if [[ -z "$id" ]]; then
        echo "::error::$f 의 id 가 'AIJ-<숫자>' 형식이 아니다" >&2
        fail=1; continue
    fi
    missing=""
    for k in date kind confidence; do
        [[ -n "$(field "$k" "$front")" ]] || missing+="$k "
    done
    if [[ -n "$missing" ]]; then
        echo "::error::$f 에 없는 항목: $missing" >&2
        fail=1; continue
    fi
    # 제목은 첫 h1 이다. 본문이 곧 제목이라 프론트매터에 또 적지 않는다.
    title=$(grep -m1 '^# ' "$f" | sed 's/^# //')
    promoted=$(field 'promoted-to' "$front")
    rows+="| [$id]($(printf '%s' "$f" | sed 's|^ai/journal/||')) | $(cell "$(field date "$front")") | $(cell "$(field kind "$front")") | $(cell "${title:-제목 없음}") | $(cell "$(field confidence "$front")") | $(cell "${promoted:-—}") |"$'\n'
done

if [[ "${1:-}" == "--check" ]]; then
    ((fail == 0)) && echo "작업 로그 ${#files[@]}건 — 프론트매터 이상 없음"
    exit $fail
fi

((fail == 0)) || exit $fail

printf '%s\n' \
    '# 작업 로그 색인' \
    '' \
    '> **이 문서는 생성물이다.** 손으로 고치지 않는다 —' \
    '> `.github/scripts/journal-index.sh` 가 프론트매터에서 만든다.' \
    '' \
    '| ID | 날짜 | 종류 | 제목 | 확신 | 승격 |' \
    '|---|---|---|---|---|---|'
printf '%s' "$rows" | sort -t'[' -k2 -r

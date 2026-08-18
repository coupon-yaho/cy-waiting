#!/usr/bin/env bash
# Lua 규칙 검사. ai/rules/50-redis.md
#
# 핵심: KEYS[] 에 선언되지 않은 키를 만지면 Redis Cluster 에서 거부된다.
# 클러스터 전환 시점에 발견하면 스크립트를 전부 다시 써야 하므로 지금부터 막는다.
set -uo pipefail

input=$(cat)
file=$(printf '%s' "$input" | jq -r '.tool_input.file_path // empty')

[[ -z "$file" || "$file" != *.lua || ! -f "$file" ]] && exit 0

# 주석을 걷어낸 코드 뷰. "행번호:내용"
code=$(awk '{
    line = $0
    sub(/--.*/, "", line)
    printf "%d:%s\n", NR, line
}' "$file")

violations=()

# 예외 주석 인정 범위는 같은 줄 또는 바로 위 3줄 (check-java.sh 와 동일)
EXCEPTION_LOOKBACK=3

scan() {
    local rule=$1 pattern=$2
    local matches out="" m n from context
    matches=$(printf '%s\n' "$code" | grep -E "$pattern" || true)
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

# ── RD-1 (a) 명령 바로 뒤의 키 인자가 리터럴인가 ──────────────────────────────
# redis.call('ZCARD', 'queue:...')  → 위반
# redis.call('SET', KEYS[4], '1', 'EX', ARGV[2])  → 정상
#   두 번째 인자만 본다. 세 번째 이후의 리터럴은 값이지 키가 아니다.
report "RD-1" "명령 뒤의 키 인자가 리터럴이다 — KEYS[n] 을 쓴다. 클러스터에서 거부된다" \
    "$(scan 'RD-1' "^[0-9]+:.*redis\.(call|pcall)\([[:space:]]*['\"][A-Za-z]+['\"][[:space:]]*,[[:space:]]*['\"]")"

# ── RD-1 (b) 키 접두사 리터럴이 파일 어디에든 있는가 ──────────────────────────
# 변수에 담아 우회하는 형태를 잡는다:
#   local k = 'queue:{' .. cid .. '}'
report "RD-1" "키 접두사를 스크립트 안에서 조립했다 — 키는 전부 KEYS[] 로 받는다" \
    "$(scan 'RD-1' "^[0-9]+:.*['\"](queue|maxscore|admitted|alive|grace|stock|capacity|gw):")"

# ── RD-10 KEYS/ARGV 계약 주석 ─────────────────────────────────────────────────
if grep -qE 'KEYS\[[0-9]+\]' "$file" && ! grep -qE '^--[[:space:]]*KEYS\[1\]' "$file"; then
    violations+=("[RD-10] 파일 상단에 KEYS/ARGV 계약을 주석으로 적는다"$'\n'"  예: -- KEYS[1] queue:{couponId}   Sorted Set. score = Redis TIME 의 us")
fi

if ((${#violations[@]} > 0)); then
    {
        echo "규칙 위반: $file"
        echo
        printf '%s\n' "${violations[@]}"
        echo "규칙 전문: ai/rules/50-redis.md"
        echo "정당한 예외라면 해당 줄 또는 바로 위 3줄 이내에"
        echo "  -- RULE-EXCEPTION(<규칙ID>): <이유>"
        echo "를 달고 ai/journal/ 에 근거를 남긴다."
    } >&2
    exit 2
fi
exit 0

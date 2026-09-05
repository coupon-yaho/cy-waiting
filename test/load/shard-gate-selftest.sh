#!/usr/bin/env bash
# 착수 판정의 자기검증 (TS-9).
#
# **이 판정 하나가 Phase 10 을 열고 닫는다.** 여기가 조용히 늘 "보류" 를 내면
# 샤딩이 필요한 상황에도 안 열리고, 늘 "착수" 를 내면 필요 없는 것을 연다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

judge=$PWD/test/load/evaluate-shard-gate.sh
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

fail=0
# **셸 지역변수는 영문이어야 한다.** bash 의 local 이 한글 식별자를 안 받는다.
check() {
    local name=$1 samples=$2 want_rc=$3 want_word=$4
    printf '%s\n' $samples > "$work/s.txt"
    local out rc
    out=$(REDIS_OPS_LIMIT=80000 "$judge" "$work/s.txt" 2>&1)
    rc=$?
    if [ "$rc" -eq "$want_rc" ] && printf '%s' "$out" | grep -q "$want_word"; then
        echo "  ✓ $name"
    else
        echo "  ✗ $name — 종료 $rc (기대 $want_rc), '$want_word' 없음"
        fail=1
    fi
}

echo "착수 판정 자기검증"
check "한계의 60% 를 넘으면 착수"   "1000 52000 900"  0 "착수"
check "60% 안이면 보류"             "1000 40000 900"  0 "보류"
check "경계(정확히 60%)는 보류"     "48000"           0 "보류"
check "경계 바로 위는 착수"         "48001"           0 "착수"
check "봉우리를 표본에서 고른다"    "80000 100 100"   0 "착수"
# **숫자가 아니면 막는다.** 안 막으면 셸 산술이 0 으로 읽어 "보류" 가 나온다 —
# 못 잰 것이 "여유 있다" 로 기록된다.
check "숫자가 아닌 표본은 막는다"  "oops"            1 "숫자가 아니다"

# **p99 를 어디서 읽는지도 본다.** 요약의 모양이 두 가지라 한쪽만 보면 늘 "없음"
# 이 찍히고, 그러면 기록이 있는 줄 알고 넘어간다. 없을 때 다른 값으로 채우지
# 않는 것도 같이 본다 — 채우면 그 줄을 보는 사람이 p99 라고 믿는다.
p99_check() {
    local name=$1 json=$2 want=$3
    printf '48000\n' > "$work/s.txt"
    printf '%s' "$json" > "$work/summary.json"
    local out
    out=$(REDIS_OPS_LIMIT=80000 "$judge" "$work/s.txt" "$work/summary.json" 2>&1)
    if printf '%s' "$out" | grep -q "응답 p99(ms) — 기록만 *$want"; then
        echo "  ✓ $name"
    else
        echo "  ✗ $name — '$want' 이 안 나왔다"
        fail=1
    fi
}

p99_check "values 아래의 p99 를 읽는다" \
    '{"metrics":{"http_req_duration":{"values":{"p(99)":123.4}}}}' "123.4"
p99_check "바로 아래의 p99 도 읽는다" \
    '{"metrics":{"http_req_duration":{"p(99)":222.5}}}' "222.5"
p99_check "없으면 다른 값으로 안 채운다" \
    '{"metrics":{"http_req_duration":{"values":{"p(95)":99.9}}}}' "없음"

# **표본이 비면 실패다.** 프로브가 안 돌았다는 뜻이고, 그걸 "보류" 로 읽으면
# 측정을 안 한 판이 착수 판정을 낸 것이 된다.
: > "$work/empty.txt"
if REDIS_OPS_LIMIT=80000 "$judge" "$work/empty.txt" >/dev/null 2>&1; then
    echo "  ✗ 표본이 비었는데 통과시켰다"
    fail=1
else
    echo "  ✓ 표본이 비면 막는다"
fi

if REDIS_OPS_LIMIT=80000 "$judge" "$work/없는파일.txt" >/dev/null 2>&1; then
    echo "  ✗ 표본 파일이 없는데 통과시켰다"
    fail=1
else
    echo "  ✓ 표본 파일이 없으면 막는다"
fi

# **계기 고장은 착수가 아니다.** 시계가 틀려 값이 부풀면 판정이 늘 "착수" 로
# 나오는데, 그것은 부하가 아니라 계기를 잰 것이다.
printf '%s\n' 8000000000 > "$work/broken.txt"
out=$(test/load/evaluate-shard-gate.sh "$work/broken.txt" 2>&1); rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "계기를 먼저 본다"; then
    echo "  ✓ 계기가 고장 나면 판정 불가"
else
    echo "  ✗ 계기가 고장 나면 판정 불가 — 종료 $rc"
    printf '%s\n' "$out" | sed 's/^/      /'
    fail=1
fi

[ "$fail" -eq 0 ] && echo "착수 판정 자기검증 통과" || echo "착수 판정 자기검증 실패"

exit "$fail"

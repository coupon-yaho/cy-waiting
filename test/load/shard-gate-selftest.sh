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
# 판정기가 요약을 필수로 받는다. 비율 계산만 보는 사례에는 줄이 충분히 선
# 요약을 쥐여 준다 — 그 조건은 아래에 제 사례가 따로 있다.
printf '{"metrics":{"queued_responses":{"values":{"count":5000}}}}' > "$work/ok.json"

check() {
    local name=$1 samples=$2 want_rc=$3 want_word=$4
    printf '%s\n' $samples > "$work/s.txt"
    local out rc
    # **여기서는 표본 수를 안 문다.** 이 사례들이 보는 것은 비율 계산이고,
    # 표본 수 조건은 아래에 제 사례가 따로 있다.
    out=$(REDIS_OPS_LIMIT=80000 MIN_SAMPLES=1 "$judge" "$work/s.txt" "$work/ok.json" 2>&1)
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
check "숫자가 아닌 표본은 막는다"  "oops"            1 "숫자가 아닌 줄"
# **한 줄만 깨져도 막는다.** `sort -n` 이 쓰레기 줄을 0 으로 읽으므로 봉우리만
# 보면 조용히 지나간다 — 그 회차는 프로브가 중간에 흔들렸다는 뜻이다.
check "한 줄만 깨져도 막는다"      "1000 oops 900"   1 "숫자가 아닌 줄"

# **p99 를 어디서 읽는지도 본다.** 요약의 모양이 두 가지라 한쪽만 보면 늘 "없음"
# 이 찍히고, 그러면 기록이 있는 줄 알고 넘어간다. 없을 때 다른 값으로 채우지
# 않는 것도 같이 본다 — 채우면 그 줄을 보는 사람이 p99 라고 믿는다.
p99_check() {
    local name=$1 json=$2 want=$3
    printf '48000\n' > "$work/s.txt"
    printf '%s' "$json" > "$work/summary.json"
    local out
    out=$(REDIS_OPS_LIMIT=80000 MIN_SAMPLES=1 "$judge" "$work/s.txt" "$work/summary.json" 2>&1)
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
if REDIS_OPS_LIMIT=80000 "$judge" "$work/empty.txt" "$work/ok.json" >/dev/null 2>&1; then
    echo "  ✗ 표본이 비었는데 통과시켰다"
    fail=1
else
    echo "  ✓ 표본이 비면 막는다"
fi

if REDIS_OPS_LIMIT=80000 "$judge" "$work/없는파일.txt" "$work/ok.json" >/dev/null 2>&1; then
    echo "  ✗ 표본 파일이 없는데 통과시켰다"
    fail=1
else
    echo "  ✓ 표본 파일이 없으면 막는다"
fi

# **계기 고장은 착수가 아니다.** 시계가 틀려 값이 부풀면 판정이 늘 "착수" 로
# 나오는데, 그것은 부하가 아니라 계기를 잰 것이다.
#
# **경계를 두 사례로 못 박는다.** 한쪽만 두면 가드를 두 배로 옮기든 천 배로
# 옮기든 시험이 초록이다 — 두 배로 좁히면 계획서가 든 스파이크 수요(800K)가
# 계기 고장으로 거절되고, 천 배로 넓히면 아무것도 안 막는다.
many() { local n=$1 v=$2 i; : > "$work/$3"; for i in $(seq 1 "$n"); do echo "$v 200" >> "$work/$3"; done; }

many 30 800000 edge_ok.txt
out=$(REDIS_OPS_LIMIT=80000 test/load/evaluate-shard-gate.sh "$work/edge_ok.txt" "$work/ok.json" 2>&1); rc=$?
if [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q "착수"; then
    echo "  ✓ 한계의 열 배까지는 잰 값으로 본다"
else
    echo "  ✗ 한계의 열 배까지는 잰 값으로 본다 — 종료 $rc"
    printf '%s\n' "$out" | sed 's/^/      /'
    fail=1
fi

many 30 800001 edge_bad.txt
out=$(REDIS_OPS_LIMIT=80000 test/load/evaluate-shard-gate.sh "$work/edge_bad.txt" "$work/ok.json" 2>&1); rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "계기를 먼저 본다"; then
    echo "  ✓ 열 배를 넘으면 판정 불가"
else
    echo "  ✗ 열 배를 넘으면 판정 불가 — 종료 $rc"
    printf '%s\n' "$out" | sed 's/^/      /'
    fail=1
fi

# **표본이 적으면 못 잰 것이다.** 한 개로도 판정이 나면 프로브가 거의 안 돈
# 회차가 그대로 착수를 낸다.
printf '%s\n' "70000 200" > "$work/few.txt"
out=$(REDIS_OPS_LIMIT=80000 test/load/evaluate-shard-gate.sh "$work/few.txt" "$work/ok.json" 2>&1); rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "표본이"; then
    echo "  ✓ 표본이 적으면 판정 불가"
else
    echo "  ✗ 표본이 적으면 판정 불가 — 종료 $rc"
    printf '%s\n' "$out" | sed 's/^/      /'
    fail=1
fi

# **유입도 남는지 본다.** 안 남으면 회차마다 값이 갈렸을 때 부하가 달랐던
# 것인지 배선이 달랐던 것인지 산출물만 보고 못 가른다. 없을 때 다른 값으로
# 채우지 않는 것도 같이 본다.
rate_check() {
    local name=$1 json=$2 want=$3
    printf '48001\n' > "$work/s.txt"
    printf '%s' "$json" > "$work/r.json"
    local out
    out=$(REDIS_OPS_LIMIT=80000 MIN_SAMPLES=1 MIN_QUEUED=0 \
        "$judge" "$work/s.txt" "$work/r.json" 2>&1)
    if printf '%s' "$out" | grep -q "유입 req/s — 기록만 *$want"; then
        echo "  ✓ $name"
    else
        echo "  ✗ $name — '$want' 이 안 나왔다"
        printf '%s\n' "$out" | sed 's/^/      /'
        fail=1
    fi
}

rate_check "values 아래의 유입을 읽는다" \
    '{"metrics":{"http_reqs":{"values":{"rate":1656.7}}}}' "1656.7"
rate_check "바로 아래의 유입도 읽는다" \
    '{"metrics":{"http_reqs":{"rate":856.9}}}' "856.9"
rate_check "없으면 다른 값으로 안 채운다" \
    '{"metrics":{"http_reqs":{"values":{"count":20000}}}}' "없음"

# **줄에 안 섰으면 잰 것이 없다.** 한산한 쿠폰은 레디스를 안 치므로(불변식 1),
# 게이트웨이가 앞에서 다 흘려보낸 회차는 제어 평면 몫만 찍힌다. 그 값으로
# "여유 있다" 를 적으면 등록 경로를 한 번도 안 밟은 회차가 게이트를 지난다.
queued_check() {
    # **경계 위를 쓴다.** 48000 은 정확히 60% 라 보류가 나오므로, 그 표본으로는
    # 통과 사례가 "착수" 를 못 낸다 — 여기서 보는 것은 문턱이 아니라 닿았는지다.
    local name=$1 json=$2 want_rc=$3 want_word=$4
    printf '48001\n' > "$work/s.txt"
    printf '%s' "$json" > "$work/q.json"
    local out rc
    out=$(REDIS_OPS_LIMIT=80000 MIN_SAMPLES=1 MIN_QUEUED=1000 \
        "$judge" "$work/s.txt" "$work/q.json" 2>&1)
    rc=$?
    if [ "$rc" -eq "$want_rc" ] && printf '%s' "$out" | grep -q "$want_word"; then
        echo "  ✓ $name"
    else
        echo "  ✗ $name — 종료 $rc (기대 $want_rc), '$want_word' 없음"
        printf '%s\n' "$out" | sed 's/^/      /'
        fail=1
    fi
}

# **기본값을 못 박는다.** 위의 사례들이 전부 `MIN_QUEUED` 를 주입하는데
# 러너는 안 준다 — 실제 판정은 기본값으로 돈다. 주입 없이 도는 사례가 없으면
# 기본값을 0 이나 1 로 되돌려도 전부 초록이고, 이 브랜치가 고친 결함이 그대로
# 돌아온다. `REDIS_OPS_LIMIT` 도 같은 이유로 하나를 주입 없이 둔다.
default_check() {
    local name=$1 json=$2 want_rc=$3 want_word=$4
    # 기본 한계 80,000 의 60% 바로 위. 주입을 안 하므로 이 수가 기본값을 박는다.
    printf '48001\n' > "$work/s.txt"
    printf '%s' "$json" > "$work/d.json"
    local out rc
    out=$(MIN_SAMPLES=1 "$judge" "$work/s.txt" "$work/d.json" 2>&1)
    rc=$?
    if [ "$rc" -eq "$want_rc" ] && printf '%s' "$out" | grep -q "$want_word"; then
        echo "  ✓ $name"
    else
        echo "  ✗ $name — 종료 $rc (기대 $want_rc), '$want_word' 없음"
        printf '%s\n' "$out" | sed 's/^/      /'
        fail=1
    fi
}

default_check "기본 하한 바로 아래는 판정 불가" \
    '{"metrics":{"queued_responses":{"values":{"count":999}}}}' 1 "최소 1000"
default_check "기본 하한에 닿으면 판정한다" \
    '{"metrics":{"queued_responses":{"values":{"count":1000}}}}' 0 "착수"
# **한계를 양쪽에서 박는다.** 위 사례가 48,001 에서 착수를 내므로 한계가 더
# 크면 죽는다. 아래는 47,999 에서 보류를 내므로 한계가 더 작으면 죽는다.
default_low() {
    printf '47999\n' > "$work/s.txt"
    printf '%s' '{"metrics":{"queued_responses":{"values":{"count":5000}}}}' > "$work/d.json"
    local out
    out=$(MIN_SAMPLES=1 "$judge" "$work/s.txt" "$work/d.json" 2>&1)
    if printf '%s' "$out" | grep -q "판정: 보류"; then
        echo "  ✓ 기본 한계로 경계 바로 아래는 보류"
    else
        echo "  ✗ 기본 한계로 경계 바로 아래는 보류"
        printf '%s\n' "$out" | sed 's/^/      /'
        fail=1
    fi
}
default_low

queued_check "줄에 선 것이 0 이면 판정 불가" \
    '{"metrics":{"queued_responses":{"values":{"count":0}}}}' 1 "안 닿았다"
queued_check "카운터가 아예 없어도 판정 불가" \
    '{"metrics":{"http_req_duration":{"values":{"p(99)":1.0}}}}' 1 "안 닿았다"
queued_check "세 자릿수도 판정 불가" \
    '{"metrics":{"queued_responses":{"values":{"count":135}}}}' 1 "안 닿았다"
queued_check "충분히 줄에 섰으면 판정한다" \
    '{"metrics":{"queued_responses":{"values":{"count":5000}}}}' 0 "착수"
queued_check "카운터가 바로 아래 있어도 읽는다" \
    '{"metrics":{"queued_responses":{"count":5000}}}' 0 "착수"

# **폴백이 다른 지표로 뻗는 것까지 본다.** 앞의 사례들은 반증 JSON 에 한
# 지표만 담아서, p99 가 없을 때 유입으로 채우는 결함은 안 잡힌다. 둘을 서로
# 다른 값으로 한 JSON 에 담고 각 줄이 제 값을 내는지 본다.
printf '%s' '{"metrics":{"http_req_duration":{"values":{"p(99)":111.1}},
    "http_reqs":{"values":{"rate":222.2}},
    "queued_responses":{"values":{"count":5000}}}}' > "$work/both.json"
printf '48001\n' > "$work/s.txt"
out=$(REDIS_OPS_LIMIT=80000 MIN_SAMPLES=1 "$judge" "$work/s.txt" "$work/both.json" 2>&1)
if printf '%s' "$out" | grep -q "응답 p99(ms) — 기록만 *111.1" \
    && printf '%s' "$out" | grep -q "유입 req/s — 기록만 *222.2"; then
    echo "  ✓ 두 지표가 서로를 안 채운다"
else
    echo "  ✗ 두 지표가 서로를 안 채운다"
    printf '%s\n' "$out" | sed 's/^/      /'
    fail=1
fi

# **요약이 없으면 못 잰 것이다.** 없으면 줄에 선 건수를 못 읽어 "닿았는지"
# 검사가 통째로 생략되고 판정만 나온다 — 가드를 넣어 놓고 우회로를 여는 셈이다.
printf '48001\n' > "$work/s.txt"
: > "$work/empty.json"
for case in "요약 인자가 없으면 막는다:" \
            "요약 파일이 없으면 막는다:$work/없는요약.json" \
            "요약이 비었으면 막는다:$work/empty.json"; do
    name=${case%%:*}; arg=${case#*:}
    if REDIS_OPS_LIMIT=80000 MIN_SAMPLES=1 "$judge" "$work/s.txt" $arg >/dev/null 2>&1; then
        echo "  ✗ $name — 통과시켰다"
        fail=1
    else
        echo "  ✓ $name"
    fi
done

[ "$fail" -eq 0 ] && echo "착수 판정 자기검증 통과" || echo "착수 판정 자기검증 실패"

exit "$fail"

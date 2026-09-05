#!/usr/bin/env bash
# 착수 판정의 자기검증 (TS-9).
#
# **이 판정 하나가 Phase 10 을 열고 닫는다.** 여기가 조용히 늘 "보류" 를 내면
# 샤딩이 필요한 상황에도 안 열리고, 늘 "착수" 를 내면 필요 없는 것을 연다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

. test/load/selftest-lib.sh || exit 2
SELFTEST_JUDGE=$PWD/test/load/evaluate-shard-gate.sh

# 실패를 안 보면 `work` 가 빈 문자열이 되어 이후 쓰기가 루트로 간다.
work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

# 판정기가 요약을 필수로 받는다. 요약을 안 보는 사례에는 줄이 충분히 선 것을
# 쥐여 준다 — 줄에 선 건수 조건은 아래에 제 사례가 따로 있다.
printf '{"metrics":{"queued_responses":{"values":{"count":5000}}}}' > "$work/ok.json"

# 표본 파일을 만들고 이름을 낸다. 한 칸만 주면 CPU 칸이고, 세 칸을 주면
# `<CPU×100> <창ms> <명령/초>` 그대로다. **셸 지역변수는 영문이어야 한다** — bash 의
# `local` 이 한글 식별자를 안 받는다.
samples() {
    local path=$work/$1
    shift
    printf '%s\n' "$@" > "$path"
    printf '%s' "$path"
}

# 같은 값을 여러 줄. 표본 수 조건에 걸리지 않게 채울 때 쓴다.
many() {
    local path=$work/$1 n=$2 v=$3 i
    : > "$path"
    for i in $(seq 1 "$n"); do echo "$v 200" >> "$path"; done
    printf '%s' "$path"
}

# 요약 JSON 을 만들고 이름을 낸다.
summary() {
    local path=$work/$1
    printf '%s' "$2" > "$path"
    printf '%s' "$path"
}

echo "착수 판정 자기검증"

# **문턱 계산을 본다.** 표본 수 조건은 아래에 제 사례가 있으므로 여기서는 푼다.
MIN_SAMPLES=1 run_case "봉우리가 60% 를 넘으면 착수" 0 "착수" \
    -- "$(samples s.txt 1000 6500 900)" "$work/ok.json"
MIN_SAMPLES=1 run_case "60% 안이면 보류" 0 "보류" \
    -- "$(samples s.txt 1000 5000 900)" "$work/ok.json"
MIN_SAMPLES=1 run_case "경계(정확히 60%)는 보류" 0 "보류" \
    -- "$(samples s.txt 6000)" "$work/ok.json"
MIN_SAMPLES=1 run_case "경계 바로 위는 착수" 0 "착수" \
    -- "$(samples s.txt 6001)" "$work/ok.json"
MIN_SAMPLES=1 run_case "봉우리를 표본에서 고른다" 0 "착수" \
    -- "$(samples s.txt 9000 100 100)" "$work/ok.json"

# **숫자가 아니면 막는다.** 안 막으면 셸 산술이 0 으로 읽어 "보류" 가 나온다 —
# 못 잰 것이 "여유 있다" 로 기록된다. **한 줄만 깨져도 막는다** — `sort -n` 이
# 쓰레기 줄을 0 으로 읽으므로 봉우리만 보면 조용히 지나가는데, 그 회차는
# 프로브가 중간에 흔들렸다는 뜻이다.
MIN_SAMPLES=1 run_case "숫자가 아닌 표본은 막는다" 1 "숫자가 아닌 줄" \
    -- "$(samples s.txt oops)" "$work/ok.json"
MIN_SAMPLES=1 run_case "한 줄만 깨져도 막는다" 1 "숫자가 아닌 줄" \
    -- "$(samples s.txt 1000 oops 900)" "$work/ok.json"

# **표본이 비면 실패다.** 프로브가 안 돌았다는 뜻이고, 그걸 "보류" 로 읽으면
# 측정을 안 한 회차가 착수 판정을 낸 것이 된다.
: > "$work/empty.txt"
run_case "표본이 비면 막는다" 1 "표본이 비었다" \
    -- "$work/empty.txt" "$work/ok.json"
run_case "표본 파일이 없으면 막는다" 1 "표본이 비었다" \
    -- "$work/없는파일.txt" "$work/ok.json"

# **표본이 적으면 못 잰 것이다.** 한 개로도 판정이 나면 프로브가 거의 안 돈
# 회차가 그대로 착수를 낸다.
run_case "표본이 적으면 판정 불가" 1 "표본이" \
    -- "$(samples few.txt "7000 200 500")" "$work/ok.json"

# **계기 고장은 착수가 아니다.** 시계가 틀려 값이 부풀면 판정이 늘 "착수" 로
# 나오는데, 그것은 부하가 아니라 계기를 잰 것이다.
#
# **경계를 두 사례로 못 박는다.** 한쪽만 두면 가드를 어디로 옮기든 시험이
# 초록이다. 레디스의 명령 처리는 한 스레드라 배경 스레드를 얹어도 코어 몇 개를
# 못 넘는다 — 코어 열 개어치는 계기가 틀린 것이다.
run_case "코어 열 개까지는 잰 값으로 본다" 0 "착수" \
    -- "$(many edge_ok.txt 30 100000)" "$work/ok.json"
run_case "그것을 넘으면 판정 불가" 1 "계기를 먼저 본다" \
    -- "$(many edge_bad.txt 30 100001)" "$work/ok.json"

# **p99 를 어디서 읽는지도 본다.** 요약의 모양이 두 가지라 한쪽만 보면 늘 "없음"
# 이 찍히고, 그러면 기록이 있는 줄 알고 넘어간다. 없을 때 다른 값으로 채우지
# 않는 것도 같이 본다 — 채우면 그 줄을 보는 사람이 p99 라고 믿는다.
#
# 이 요약들에는 줄에 선 건수가 없으므로 판정 불가(1)로 끝난다. 기록 줄은 그
# 앞에 찍히므로 대조에는 지장이 없다.
MIN_SAMPLES=1 run_case "values 아래의 p99 를 읽는다" 1 "기록만 *123.4" \
    -- "$(samples s.txt 6001)" \
       "$(summary p99.json '{"metrics":{"http_req_duration":{"values":{"p(99)":123.4}}}}')"
MIN_SAMPLES=1 run_case "바로 아래의 p99 도 읽는다" 1 "기록만 *222.5" \
    -- "$(samples s.txt 6001)" \
       "$(summary p99.json '{"metrics":{"http_req_duration":{"p(99)":222.5}}}')"
MIN_SAMPLES=1 run_case "p99 가 없으면 다른 값으로 안 채운다" 1 "p99(ms) — 기록만 *없음" \
    -- "$(samples s.txt 6001)" \
       "$(summary p99.json '{"metrics":{"http_req_duration":{"values":{"p(95)":99.9}}}}')"

# **유입도 남는지 본다.** 안 남으면 회차마다 값이 갈렸을 때 부하가 달랐던
# 것인지 배선이 달랐던 것인지 산출물만 보고 못 가른다.
MIN_SAMPLES=1 run_case "values 아래의 유입을 읽는다" 1 "기록만 *1656.7" \
    -- "$(samples s.txt 6001)" \
       "$(summary rate.json '{"metrics":{"http_reqs":{"values":{"rate":1656.7}}}}')"
MIN_SAMPLES=1 run_case "바로 아래의 유입도 읽는다" 1 "기록만 *856.9" \
    -- "$(samples s.txt 6001)" \
       "$(summary rate.json '{"metrics":{"http_reqs":{"rate":856.9}}}')"
MIN_SAMPLES=1 run_case "유입이 없으면 다른 값으로 안 채운다" 1 "req/s — 기록만 *없음" \
    -- "$(samples s.txt 6001)" \
       "$(summary rate.json '{"metrics":{"http_reqs":{"values":{"count":20000}}}}')"

# **폴백이 다른 지표로 뻗는 것까지 본다.** 위 사례들은 반증 JSON 에 한 지표만
# 담아서, p99 가 없을 때 유입으로 채우는 결함은 안 잡힌다. 둘을 서로 다른
# 값으로 한 JSON 에 담고 각 줄이 제 값을 내는지 따로 본다.
both=$(summary both.json '{"metrics":{"http_req_duration":{"values":{"p(99)":111.1}},
    "http_reqs":{"values":{"rate":222.2}},
    "queued_responses":{"values":{"count":5000}}}}')
MIN_SAMPLES=1 run_case "p99 자리에 유입이 안 온다" 0 "p99(ms) — 기록만 *111.1" \
    -- "$(samples s.txt 6001)" "$both"
MIN_SAMPLES=1 run_case "유입 자리에 p99 가 안 온다" 0 "req/s — 기록만 *222.2" \
    -- "$(samples s.txt 6001)" "$both"

# **줄에 안 섰으면 잰 것이 없다.** 한산한 쿠폰은 레디스를 안 치므로(불변식 1),
# 게이트웨이가 앞에서 다 흘려보낸 회차는 제어 평면 몫만 찍힌다. 그 값으로
# "여유 있다" 를 적으면 등록 경로를 한 번도 안 밟은 회차가 게이트를 지난다.
#
# **표본은 경계 위를 쓴다.** 48,000 은 정확히 60% 라 보류가 나오므로 그것으로는
# 통과 사례가 "착수" 를 못 낸다 — 여기서 보는 것은 문턱이 아니라 닿았는지다.
MIN_SAMPLES=1 MIN_QUEUED=1000 \
    run_case "줄에 선 것이 0 이면 판정 불가" 1 "안 닿았다" \
    -- "$(samples s.txt 6001)" \
       "$(summary q.json '{"metrics":{"queued_responses":{"values":{"count":0}}}}')"
MIN_SAMPLES=1 MIN_QUEUED=1000 \
    run_case "카운터가 아예 없어도 판정 불가" 1 "안 닿았다" \
    -- "$(samples s.txt 6001)" \
       "$(summary q.json '{"metrics":{"http_req_duration":{"values":{"p(99)":1.0}}}}')"
MIN_SAMPLES=1 MIN_QUEUED=1000 \
    run_case "세 자릿수도 판정 불가" 1 "안 닿았다" \
    -- "$(samples s.txt 6001)" \
       "$(summary q.json '{"metrics":{"queued_responses":{"values":{"count":135}}}}')"
MIN_SAMPLES=1 MIN_QUEUED=1000 \
    run_case "충분히 줄에 섰으면 판정한다" 0 "착수" \
    -- "$(samples s.txt 6001)" \
       "$(summary q.json '{"metrics":{"queued_responses":{"values":{"count":5000}}}}')"
MIN_SAMPLES=1 MIN_QUEUED=1000 \
    run_case "카운터가 바로 아래 있어도 읽는다" 0 "착수" \
    -- "$(samples s.txt 6001)" \
       "$(summary q.json '{"metrics":{"queued_responses":{"count":5000}}}')"

# **기본값을 못 박는다.** 위 사례들이 전부 하한과 한계를 주입하는데 러너는 안
# 준다 — 실제 판정은 기본값으로 돈다. 주입 없이 도는 사례가 없으면 기본값을
# 0 이나 1 로 되돌려도 전부 초록이고, 이 브랜치가 고친 결함이 그대로 돌아온다.
#
# 표본 6,001 은 기본 문턱 60% 바로 위고 5,999 는 바로 아래다. 둘을 같이 두어
# **문턱을 양쪽에서** 박는다 — 문턱이 커지면 위 사례가 죽고 작아지면 아래가 죽는다.
MIN_SAMPLES=1 run_case "기본 하한 바로 아래는 판정 불가" 1 "최소 1000" \
    -- "$(samples s.txt 6001)" \
       "$(summary d.json '{"metrics":{"queued_responses":{"values":{"count":999}}}}')"
MIN_SAMPLES=1 run_case "기본 하한에 닿으면 판정한다" 0 "착수" \
    -- "$(samples s.txt 6001)" \
       "$(summary d.json '{"metrics":{"queued_responses":{"values":{"count":1000}}}}')"
MIN_SAMPLES=1 run_case "기본 문턱으로 경계 바로 아래는 보류" 0 "판정: 보류" \
    -- "$(samples s.txt 5999)" "$work/ok.json"

# **요약이 없으면 못 잰 것이다.** 없으면 줄에 선 건수를 못 읽어 "닿았는지"
# 검사가 통째로 생략되고 판정만 나온다 — 가드를 넣어 놓고 우회로를 여는 셈이다.
: > "$work/empty.json"
MIN_SAMPLES=1 run_case "요약 인자가 없으면 막는다" 1 "요약 파일" \
    -- "$(samples s.txt 6001)"
MIN_SAMPLES=1 run_case "요약 파일이 없으면 막는다" 1 "요약이 없거나 비었다" \
    -- "$(samples s.txt 6001)" "$work/없는요약.json"
MIN_SAMPLES=1 run_case "요약이 비었으면 막는다" 1 "요약이 없거나 비었다" \
    -- "$(samples s.txt 6001)" "$work/empty.json"

[ "$selftest_failed" -eq 0 ] && echo "착수 판정 자기검증 통과" \
    || echo "착수 판정 자기검증 실패"

exit "$selftest_failed"

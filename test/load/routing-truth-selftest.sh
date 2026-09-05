#!/usr/bin/env bash
# 라우팅 비교 판정의 자기검증 (TS-9).
#
# **이 판정의 표가 문서 세 곳에 인용됐다.** 기본 전략 결정을 다시 여는 근거라
# 판정기가 조용히 틀리면 그 결정이 틀린 근거 위에 선다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

. test/load/selftest-lib.sh || exit 2
SELFTEST_JUDGE=$PWD/test/load/evaluate-routing-truth.sh

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

# 결과 파일을 만들고 이름을 낸다. 인자는 `<느린대> <정상1> <정상2>` 처리 건수다.
result() {
    local path=$work/$1
    printf '%s 0 느린대\n%s 0 정상1\n%s 0 정상2\n' "$2" "$3" "$4" > "$path"
    printf '%s' "$path"
}

# 밀어낸 건수를 실은 결과. 다섯째 인자가 느린 대의 밀어냄이다.
rejecting() {
    local path=$work/$1
    printf '%s %s 느린대\n%s 0 정상1\n%s 0 정상2\n' "$2" "$5" "$3" "$4" > "$path"
    printf '%s' "$path"
}

summary() {
    local path=$work/$1
    printf '%s' "$2" > "$path"
    printf '%s' "$path"
}

ok=$(summary ok.json '{"metrics":{"dropped_iterations":{"values":{"count":2520}}}}')

echo "라우팅 비교 자기검증"

# 균등하게 보내면 실제 능력을 못 본 것이다. 세 배 느린 대에 3분의 1 을 보낸다.
run_case "균등이면 능력을 못 본다" 0 "못 본다" \
    -- "$(result even.txt 2500 2500 2500)" "$ok"
# 능력비(1/7)에 가까우면 본 것이다.
run_case "능력비에 가까우면 본다" 0 "능력을 본다" \
    -- "$(result seen.txt 1071 3214 3215)" "$ok"

# **경계를 두 사례로 못 박는다.** 한쪽만 두면 중간값을 어디로 옮겨도 초록이다.
# 균등 33.3% 와 능력비 14.2% 의 중간은 23.7% 다. 처리 합 10,000 기준이라
# 23.69% 가 바로 아래고 23.80% 가 바로 위다.
run_case "중간값 바로 아래는 본다" 0 "능력을 본다" \
    -- "$(result below.txt 2369 3815 3816)" "$ok"
run_case "중간값 바로 위는 못 본다" 0 "못 본다" \
    -- "$(result above.txt 2380 3810 3810)" "$ok"

# **부하가 안 닿은 회차를 "완벽히 나눴다" 로 읽으면 안 된다.** 뒷단이 안 떴거나
# 게이트웨이가 앞에서 다 막은 회차가 그 모양이다.
run_case "처리가 적으면 판정 불가" 1 "부하가 안 닿았다" \
    -- "$(result few.txt 10 20 20)" "$ok"

# 계기가 흔들린 회차. 숫자가 아니면 셸 산술이 0 으로 읽어 균등으로 나온다.
printf '2500 0 느린대\noops 0 정상1\n2500 0 정상2\n' > "$work/bad.txt"
run_case "숫자가 아니면 막는다" 1 "숫자가 아닌" \
    -- "$work/bad.txt" "$ok"

printf '2500 0 느린대\n2500 0 정상1\n' > "$work/two.txt"
run_case "뒷단이 셋이 아니면 막는다" 1 "세 줄이어야" \
    -- "$work/two.txt" "$ok"

: > "$work/empty.txt"
run_case "결과가 비면 막는다" 1 "결과가 비었다" \
    -- "$work/empty.txt" "$ok"
run_case "결과 파일이 없으면 막는다" 1 "결과가 비었다" \
    -- "$work/없는파일.txt" "$ok"

: > "$work/empty.json"
run_case "요약이 비면 막는다" 1 "요약이 없거나" \
    -- "$(result even.txt 2500 2500 2500)" "$work/empty.json"

# **느린 배수가 판정에 실제로 쓰이는지 본다.** 안 쓰면 어떤 배수를 줘도 같은
# 답이 나오고, 그러면 이 판정기는 균등인지만 보는 것이다.
# 같은 표본(느린 대 21.0%)이 배수 3 에서는 중간값 23.7% 아래라 "본다" 이고,
# 배수 9 에서는 중간값 19.2% 위라 "못 본다" 다. 배수를 안 쓰면 둘이 같아진다.
run_case "배수 3 에서는 본다" 0 "능력을 본다" \
    -- "$(result f3.txt 2100 3950 3950)" "$ok"
SLOW_FACTOR=9 run_case "배수 9 에서는 못 본다" 0 "못 본다" \
    -- "$(result f9.txt 2100 3950 3950)" "$ok"

# **과부하 회차는 몫을 못 읽는다.** 약한 대에 잔뜩 보내 놓고 그 대가 동시
# 한도에서 밀어내면, 처리 건수만 세는 판정기는 "적게 보냈다" 로 읽는다 —
# 능력을 보고 피한 것과 정반대인데 답이 같아진다.
run_case "밀어낸 건이 있으면 판정 불가" 1 "밀어냈다" \
    -- "$(rejecting rej.txt 1071 3214 3215 400)" "$ok"

# **요약이 깨졌으면 0 으로 안 읽는다.** 잘린 요약으로도 판정이 나면, 두 방식이
# 같은 부하를 받았다는 증거가 없는 채로 몫만 비교하게 된다.
run_case "요약이 깨졌으면 판정 불가" 1 "못 읽었다" \
    -- "$(result seen2.txt 1071 3214 3215)" "$(summary broken.json '{"metrics":')"
run_case "흘린 회차 계수가 없으면 판정 불가" 1 "못 읽었다" \
    -- "$(result seen3.txt 1071 3214 3215)" "$(summary nodrop.json '{"metrics":{}}')"

[ "$selftest_failed" -eq 0 ] && echo "라우팅 비교 자기검증 통과" \
    || echo "라우팅 비교 자기검증 실패"
exit "$selftest_failed"

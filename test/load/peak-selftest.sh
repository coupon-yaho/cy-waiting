#!/usr/bin/env bash
# 최대치 판정의 자기검증 (TS-9).
#
# **이 판정이 Phase 10 착수 표에 적힐 수를 만든다.** 여기가 조용히 틀리면
# 하네스가 못 만든 부하를 게이트웨이가 견딘 것으로 적게 된다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

. test/load/selftest-lib.sh || exit 2
SELFTEST_JUDGE=${SELFTEST_JUDGE:-$PWD/test/load/evaluate-peak.sh}

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

# 회차 표를 만든다. 한 줄이 한 회차다 — 요청 유입·실측 유입·판정·응답 p99.
table() {
    local path=$work/$1
    shift
    printf '%s\n' "$@" > "$path"
    printf '%s' "$path"
}

echo "최대치 판정 자기검증"

# 하네스가 만든 회차 하나. 실측이 요청의 95% 이상이라 선다.
one=$(table one.tsv $'4000\t3950\tok\t2.1')
run_case "한 회차가 서면 그 실측이 최대치다" 0 "3950" -- "$one"

# **가장 낮은 회차조차 못 만들었으면 잰 것이 없다.** 0 을 최대치로 적으면
# 게이트웨이가 아무것도 못 한 것처럼 읽힌다 — 계기 문제를 제품 문제로 옮긴다.
run_case "가장 낮은 회차를 못 만들면 판정 불가" 2 "잰 것이 없다" \
    -- "$(table nofloor.tsv $'4000\t1200\tok\t2.1')"

# 실측이 모자란 회차에서 멈춘다. 그 위는 게이트웨이 얘기가 아니다.
ladder=$(table ladder.tsv $'2000\t1990\tok\t1.8' $'4000\t3950\tok\t2.4' $'8000\t5900\tok\t3.1')
run_case "도착이 모자라면 거기서 멈춘다" 0 "3950" -- "$ladder"
run_case "그 천장은 하네스 쪽이다" 0 "하네스" -- "$ladder"

# 판정 비율이 미달인 회차는 게이트웨이가 못 버틴 것이다 — 천장의 종류가 다르다.
product=$(table product.tsv $'2000\t1990\tok\t1.8' $'4000\t3950\tunder\t2.4')
run_case "판정이 미달이면 제품 천장이다" 0 "제품" -- "$product"
run_case "제품 천장 아래가 최대치다" 0 "1990" -- "$product"

# **응답 기준은 줄 때만 건다.** 끝에서 끝까지의 값이라 G10.10 의 오버헤드가
# 아니고, 근거 없는 수를 기본값으로 두면 그 수가 계획서로 새어 나간다.
slow=$(table slow.tsv $'2000\t1990\tok\t1.8' $'4000\t3950\tok\t7.2')
run_case "기준을 안 주면 느려도 선다" 0 "3950" -- "$slow"
PEAK_LATENCY_P99_MS=5 run_case "기준을 주면 느린 회차가 제품 천장이다" 0 "제품" -- "$slow"

# **천장을 못 본 회차를 최대치로 적지 않는다.** 사다리가 짧았을 뿐이다.
run_case "전 회차가 다 서면 천장을 못 본 것이다" 0 "천장을 아직 못 봤다" \
    -- "$(table all.tsv $'2000\t1990\tok\t1.8' $'4000\t3950\tok\t2.4')"

# **기준을 실제로 쓰는지 본다.** 안 쓰면 어떤 값을 줘도 같은 답이 나온다.
PEAK_ARRIVAL_TOLERANCE=0.99 run_case "허용 오차를 조이면 최대치가 내려간다" 0 "1990" \
    -- "$ladder"
PEAK_LATENCY_P99_MS=2.0 run_case "응답 기준을 조이면 최대치가 내려간다" 0 "1990" \
    -- "$ladder"

# 바닥을 주면 되돌아가는 것을 막는다. 안 주면 기록만 한다.
PEAK_FLOOR=3000 run_case "바닥 위는 충족" 0 "충족" -- "$ladder"
PEAK_FLOOR=3950 run_case "바닥과 정확히 같으면 충족" 0 "충족" -- "$ladder"
PEAK_FLOOR=오천 run_case "바닥이 숫자가 아니면 판정 불가" 2 "바닥" -- "$ladder"

# **바닥 아래인 이유를 가른다.** 하네스가 못 만들어 내려간 것을 제품 미달로
# 내면 고칠 곳을 반대로 가리킨다 — 게이트를 읽는 쪽은 종료 코드만 본다.
PEAK_FLOOR=5000 run_case "하네스 천장에서 바닥 아래면 판정 불가" 2 "제품 미달로 못 읽는다" \
    -- "$ladder"
PEAK_FLOOR=5000 run_case "제품 천장에서 바닥 아래면 미달" 1 "미달" -- "$product"

# **제품이 가장 낮은 회차부터 무너지면 미달이다.** 판정 불가로 내면 계기를
# 고치라는 뜻이 되어 다음에 할 일을 반대로 가리킨다.
run_case "가장 낮은 회차부터 안 서면 미달" 1 "미달" \
    -- "$(table lowfail.tsv $'2000\t1990\tunder\t1.8' $'4000\t3950\tunder\t2.4')"

# **판정 불가는 도착 미달보다 먼저 본다.** 요약을 못 읽은 회차는 실측을 0 으로
# 적으므로, 순서가 뒤집히면 계기 사고가 생성기 한계로 적힌다.
run_case "값을 못 읽은 회차는 판정 불가로 적는다" 0 "판정 불가" \
    -- "$(table zero.tsv $'2000\t1990\tok\t1.8' $'4000\t0\tunmeasurable\t0')"

# **기준을 검증하는지도 본다.** 쓰는지만 재면 검증 블록을 지운 판이 통과한다.
PEAK_ARRIVAL_TOLERANCE=0 run_case "허용 오차 0 은 판정 불가" 2 "허용 오차" -- "$ladder"
PEAK_ARRIVAL_TOLERANCE=2 run_case "허용 오차 1 초과는 판정 불가" 2 "허용 오차" -- "$ladder"
PEAK_ARRIVAL_TOLERANCE=영점구 run_case "허용 오차가 숫자가 아니면 판정 불가" 2 "허용 오차" \
    -- "$ladder"
PEAK_LATENCY_P99_MS=오초 run_case "응답 기준이 숫자가 아니면 판정 불가" 2 "응답 기준" \
    -- "$ladder"

# 경계는 통과하는 쪽이다. 부등호가 뒤집히면 여기서 걸린다.
run_case "정확히 허용 오차면 선다" 0 "3800" \
    -- "$(table edge_tol.tsv $'4000\t3800\tok\t2.1')"
PEAK_LATENCY_P99_MS=2.1 run_case "정확히 응답 기준이면 선다" 0 "3950" \
    -- "$(table edge_lat.tsv $'4000\t3950\tok\t2.1')"

# 계기가 틀어진 모양들. 전부 판정 불가여야 한다 — 미달로 내면 제품 탓이 된다.
run_case "인자가 없으면 판정 불가" 2 "회차 표" --
run_case "회차가 한 줄도 없으면 판정 불가" 2 "한 줄도 없다" -- "$(table empty.tsv '')"
run_case "칸이 모자라면 판정 불가" 2 "칸" -- "$(table short.tsv $'4000\t3950\tok')"
run_case "요청 유입이 숫자가 아니면 판정 불가" 2 "숫자" \
    -- "$(table nan.tsv $'사천\t3950\tok\t2.1')"
run_case "판정 칸이 셋 밖이면 판정 불가" 2 "판정 칸" \
    -- "$(table verdict.tsv $'4000\t3950\tmaybe\t2.1')"
run_case "요청 유입이 오름차순이 아니면 판정 불가" 2 "오름차순" \
    -- "$(table order.tsv $'4000\t3950\tok\t2.1' $'2000\t1990\tok\t1.8')"
# **실측이 요청을 크게 넘으면 계기가 틀어진 것이다.** 그 수를 최대치로 적으면
# 만든 적 없는 부하가 표에 남는다.
run_case "실측이 요청을 넘으면 판정 불가" 2 "계기" \
    -- "$(table over.tsv $'4000\t4300\tok\t2.1')"
# 반올림과 마지막 회차의 꼬리는 덮는다. 여유를 넓히면 만든 적 없는 부하가 남는다.
run_case "반올림만큼의 초과는 선다" 0 "4200" \
    -- "$(table over_edge.tsv $'4000\t4200\tok\t2.1')"
run_case "실측 유입이 숫자가 아니면 판정 불가" 2 "실측 유입" \
    -- "$(table nan2.tsv $'4000\t삼천\tok\t2.1')"
run_case "응답 p99 가 숫자가 아니면 판정 불가" 2 "응답 p99" \
    -- "$(table nan3.tsv $'4000\t3950\tok\t느림')"
run_case "칸이 넷을 넘으면 판정 불가" 2 "칸" \
    -- "$(table long.tsv $'4000\t3950\tok\t2.1\t여분')"
run_case "요청 유입이 같으면 판정 불가" 2 "오름차순" \
    -- "$(table tie.tsv $'4000\t3950\tok\t2.1' $'4000\t3900\tok\t2.2')"
# 판정을 못 한 회차는 미달과 다르다. 그 위를 안 쓰되 제품 탓으로 적지 않는다.
run_case "판정 불가 회차에서 멈추면 그렇게 적는다" 0 "판정 불가" \
    -- "$(table unmeas.tsv $'2000\t1990\tok\t1.8' $'4000\t3950\tunmeasurable\t2.4')"
# **실패 뒤에 선 회차는 안 쓴다.** 사다리는 아래에서부터 이어져야 한다.
gap=$(table gap.tsv $'2000\t1990\tok\t1.8' $'4000\t2100\tok\t2.4' $'8000\t7900\tok\t3.1')
run_case "실패 뒤에 선 회차가 있으면 경고한다" 0 "이어지지 않았다" -- "$gap"
# **경고만 보면 안 된다.** 문구는 그대로 두고 값만 위 회차로 바꾸는 판이 있다.
run_case "이어지지 않은 위 회차는 최대치가 아니다" 0 "최대치(실측 유입) 1990" -- "$gap"

# **표를 만드는 쪽도 잰다.** 위 사례는 전부 판정기만 부른다. 요약을 읽는 자리와
# 회차 길이를 푸는 자리와 종료 코드를 판정으로 옮기는 자리가 틀리면, 판정기가
# 아무리 옳아도 표가 틀린 채로 온다.
. test/load/peak-lib.sh || exit 2

lib_case() {
    local name=$1 want=$2 got=$3
    if [ "$got" = "$want" ]; then
        echo "  ✓ $name"
    else
        echo "  ✗ $name — '$got' (기대 '$want')"
        selftest_failed=1
    fi
}

echo "러너 부분 자기검증"

# 회차 길이. `${D%s}` 로 끝 글자만 떼면 1m 이 1 이 되어 가드가 사라진다.
lib_case "초는 그대로" 30 "$(peak_duration_sec 30s)"
lib_case "분을 초로" 60 "$(peak_duration_sec 1m)"
lib_case "분과 초를 더한다" 90 "$(peak_duration_sec 1m30s)"
lib_case "시간도 푼다" 3600 "$(peak_duration_sec 1h)"
lib_case "단위가 없으면 0" 0 "$(peak_duration_sec 30)"
lib_case "못 읽는 형식은 0" 0 "$(peak_duration_sec abc)"
lib_case "빈 값은 0" 0 "$(peak_duration_sec '')"

# 요약의 두 모양. 하나만 보면 k6 판이 바뀌는 순간 전 회차가 판정 불가가 된다.
flat=$work/flat.json
printf '%s' '{"metrics":{"http_reqs":{"rate":1234.5},"http_req_duration":{"p(99)":9.5}}}' \
    > "$flat"
nested=$work/nested.json
printf '%s' '{"metrics":{"http_reqs":{"values":{"rate":1234.5}},"http_req_duration":{"values":{"p(99)":9.5}}}}' \
    > "$nested"
lib_case "평면형 도착률" 1234.5000 "$(peak_summary_value "$flat" rate)"
lib_case "중첩형 도착률" 1234.5000 "$(peak_summary_value "$nested" rate)"
lib_case "중첩형 응답 p99" 9.5000 "$(peak_summary_value "$nested" p99)"
lib_case "없는 파일은 빈 값" "" "$(peak_summary_value "$work/none.json" rate)"

# 종료 코드 해석. 99 를 통째로 정상으로 읽으면 연결을 끊은 회차가 ok 로 남는다.
thr() { printf '{"metrics":{%s}}' "$1" > "$work/$2"; printf '%s' "$work/$2"; }
only_drop=$(thr '"dropped_iterations":{"thresholds":{"count==0":true}}' drop.json)
failed=$(thr '"dropped_iterations":{"thresholds":{"count==0":true}},"http_req_failed":{"thresholds":{"rate<0.01":true}}' failed.json)
offj=$(thr '"peak_off_judgement":{"thresholds":{"count==0":true}}' offj.json)
mix=$(thr '"peak_poll_bootstrap":{"thresholds":{"rate<0.1":true}}' mix.json)
none=$(thr '"dropped_iterations":{"thresholds":{"count==0":false}}' clean.json)
lib_case "종료 0 은 그대로 선다" ok "$(peak_verdict_from_k6 0 "$none")"
lib_case "드롭만 깨지면 선다" ok "$(peak_verdict_from_k6 99 "$only_drop")"
lib_case "연결 실패는 안 선다" under "$(peak_verdict_from_k6 99 "$failed")"
lib_case "판정 밖 응답은 안 선다" under "$(peak_verdict_from_k6 99 "$offj")"
lib_case "섞은 비율이 어긋나면 판정 불가" unmeasurable "$(peak_verdict_from_k6 99 "$mix")"
lib_case "99 인데 깨진 것이 없으면 판정 불가" unmeasurable "$(peak_verdict_from_k6 99 "$none")"
lib_case "다른 종료 코드는 판정 불가" unmeasurable "$(peak_verdict_from_k6 1 "$none")"

[ "$selftest_failed" -eq 0 ] && echo "최대치 자기검증 통과" || echo "최대치 자기검증 실패"
exit "$selftest_failed"

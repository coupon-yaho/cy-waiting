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

# **천장을 못 본 회차를 최대치로 적지 않는다.** 사다리가 짧았을 뿐이다 —
# 사다리를 줄이는 것으로 기록을 낮출 수 있으면 그 수를 못 믿는다.
allrungs=$(table all.tsv $'2000\t1990\tok\t1.8' $'4000\t3950\tok\t2.4')
run_case "전 회차가 다 서면 천장을 못 본 것이다" 0 "천장을 아직 못 봤다" -- "$allrungs"
run_case "천장을 못 봤으면 아래 경계로 적는다" 0 "아래 경계(실측 유입) 3950" -- "$allrungs"
run_case "천장을 봤으면 최대치로 적는다" 0 "최대치(실측 유입) 1990" -- "$product"
PEAK_REQUIRE_CEILING=1 run_case "천장을 요구하면 짧은 사다리는 판정 불가" 2 "사다리를 늘려야" \
    -- "$allrungs"

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
# 다섯째 칸은 끊긴 몫(%)이다 (CY-990). 증설 효율이 두 표의 섞임을 견주는 재료라 옛 네 칸 표도 받는다.
run_case "다섯째 칸의 끊긴 몫은 받는다" 0 "4000" \
    -- "$(table mix.tsv $'4000\t3950\tok\t2.1\t1.5')"
run_case "못 잰 칸의 끊긴 몫은 - 로 받는다" 0 "판정 불가" \
    -- "$(table mix_dash.tsv $'4000\t3950\tok\t2.1\t1.5' $'8000\t0\tunmeasurable\t0\t-')"
run_case "끊긴 몫이 숫자가 아니면 판정 불가" 2 "끊긴 몫" \
    -- "$(table mix_nan.tsv $'4000\t3950\tok\t2.1\t여분')"
run_case "칸이 다섯을 넘으면 판정 불가" 2 "칸" \
    -- "$(table long.tsv $'4000\t3950\tok\t2.1\t1.5\t여분')"
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

# VU 풀. 표가 VU 마다 하나라, VU 당 회차가 적으면 폴링 갈래가 표 없이 진입으로 돌아 섞은 비율이 깨진다.
# 1000/초·20초를 2000 VU 에 나누면 VU 당 10 회라 그 몫이 25% 였다.
lib_case "VU 당 100 회가 되게 나눈다" 200 "$(peak_vus 1000 20)"
lib_case "시간이 길면 풀이 커진다" 2400 "$(peak_vus 8000 30)"
lib_case "낮은 유입에도 하한 50" 50 "$(peak_vus 100 20)"
lib_case "유입이 정수가 아니면 0" 0 "$(peak_vus abc 20)"
lib_case "시간이 0 이면 0" 0 "$(peak_vus 1000 0)"
# bash 정수가 넘치면 곱이 음수가 되어 하한 50 으로 조용히 줄어든다. 표현 범위 안의 자릿수만 받는다.
lib_case "유입이 열 자리면 0" 0 "$(peak_vus 1000000000 30)"
lib_case "시간이 열 자리면 0" 0 "$(peak_vus 1000 1000000000)"
lib_case "아홉 자리는 받는다" 2999999997 "$(peak_vus 999999999 300)"
# 선행 0 은 10진수로 읽는다. bash 산술은 008 을 오류로, 010 을 8 로 읽는다.
lib_case "선행 0 이 있어도 10진수다" 100 "$(peak_vus 010 1000 2>/dev/null)"
lib_case "8진수로 못 읽는 선행 0 도 받는다" 50 "$(peak_vus 008 10 2>/dev/null)"

# 예열 수렴. 갓 뜬 한 대가 2 코어에서 500/초 예열을 p99 20초로 뒤집어써, 첫 칸이 예열 노릇을 했다.
warm() { printf '{"metrics":{"http_req_duration":{"p(99)":%s}}}' "$1" > "$work/$2"; printf '%s' "$work/$2"; }
peak_warm_converged "$(warm 42.0 warm-ok.json)" 100; lib_case "p99 가 선 아래면 수렴" 0 "$?"
peak_warm_converged "$(warm 100.0 warm-edge.json)" 100; lib_case "선 정확히는 수렴" 0 "$?"
peak_warm_converged "$(warm 20198.7 warm-cold.json)" 100; lib_case "선 위면 덜 됐다" 1 "$?"
peak_warm_converged "$work/none.json" 100; lib_case "요약이 없으면 못 읽는다" 2 "$?"
peak_warm_converged "$(warm 42.0 warm-bad.json)" abc; lib_case "선이 수가 아니면 못 읽는다" 2 "$?"
peak_warm_converged "$(warm 42.0 warm-zero.json)" 0; lib_case "선이 0 이면 못 읽는다" 2 "$?"

# **걸린 요청.** 흘린 회차 없이 보낸 건수는 찼는데 실측 유입만 모자라면, 끝나지 않은 요청이 회차를 늘린 것이다.
# k6 는 유입률을 전체 시간으로 나누므로 그 칸이 생성기 한계로 읽힌다. LB 파일 한도 결함이 이 모양이었다.
summ() { printf '%s' "$1" > "$work/$2"; printf '%s' "$work/$2"; }
peak_hung "$(summ '{"metrics":{"iterations":{"count":239629},"http_reqs":{"rate":3993.18},"dropped_iterations":{"count":0}}}' hung.json)" 8000 30 0.95
lib_case "건수는 찼는데 유입이 모자라면 걸렸다" 0 "$?"
peak_hung "$(summ '{"metrics":{"iterations":{"count":239629},"http_reqs":{"rate":3993.18}}}' hung_nodrop.json)" 8000 30 0.95
lib_case "드롭 계수가 없으면 0 으로 본다" 0 "$?"
peak_hung "$(summ '{"metrics":{"iterations":{"values":{"count":239629}},"http_reqs":{"values":{"rate":3993.18}}}}' hung_nested.json)" 8000 30 0.95
lib_case "중첩형 요약도 읽는다" 0 "$?"
peak_hung "$(summ '{"metrics":{"iterations":{"count":240001},"http_reqs":{"rate":7998.7},"dropped_iterations":{"count":0}}}' stood.json)" 8000 30 0.95
lib_case "유입이 찼으면 걸리지 않았다" 1 "$?"
peak_hung "$(summ '{"metrics":{"iterations":{"count":120000},"http_reqs":{"rate":4000.0},"dropped_iterations":{"count":120000}}}' short.json)" 8000 30 0.95
lib_case "흘린 회차가 있으면 걸린 것이 아니다" 1 "$?"
peak_hung "$(summ '{"metrics":{"iterations":{"count":150000},"http_reqs":{"rate":4000.0},"dropped_iterations":{"count":0}}}' few.json)" 8000 30 0.95
lib_case "건수가 모자라면 걸린 것이 아니다" 1 "$?"
peak_hung "$work/없는요약.json" 8000 30 0.95
lib_case "요약이 없으면 못 읽는다" 2 "$?"
# 건수가 찼어도 흘린 회차가 있으면 걸린 것이 아니다. 건수 조건에 먼저 걸리는 픽스처로는 이 조건을 못 잰다.
peak_hung "$(summ '{"metrics":{"iterations":{"count":239000},"http_reqs":{"rate":3990.0},"dropped_iterations":{"count":500}}}' full_drop.json)" 8000 30 0.95
lib_case "건수가 찼어도 흘린 회차가 있으면 걸린 것이 아니다" 1 "$?"

# **표집은 회차 길이까지만 한다.** k6 가 걸린 요청 때문에 gracefulStop 동안 더 돌면 한가한 표본이 쌓여, 붙었던
# 자리의 가운데 값이 선 아래로 내려간다. 기한이 지나면 정지 파일 없이도 그 바퀴를 마치고 멈춘다.
started=$(date +%s)
timeout 20 bash -c ". test/load/peak-lib.sh; peak_sample_cpu '$work/deadline.tsv' 없는프로젝트 $$ $((started + 2))"
lib_case "기한이 지나면 표집기가 멈춘다 (시한 20초에 안 걸린다)" 0 "$?"
lib_case "기한 뒤 표본이 붙지 않는다 (호스트 표본 셋 이하)" yes \
    "$([ "$(grep -c '^idle' "$work/deadline.tsv")" -le 3 ] && echo yes || echo no)"

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

# 끊긴 몫(%) — 발급 응답 중 판정이 끊은(429·503) 몫 (CY-990). 두 표의 섞임을 견주는 재료다. **폴링은 안 넣는다** —
# 발급만 세는 계수를 따로 읽는다. 폴링의 끊김이 섞이면 섞임의 차이가 희석된다.
counts=$work/counts.json
printf '%s' '{"metrics":{"peak_issue_total":{"count":100},"peak_issue_shed":{"count":5},"peak_shed":{"count":50},"peak_admitted":{"count":900}}}' \
    > "$counts"
nested_counts=$work/nested_counts.json
printf '%s' '{"metrics":{"peak_issue_total":{"values":{"count":80}},"peak_issue_shed":{"values":{"count":20}}}}' \
    > "$nested_counts"
lib_case "발급만 센 끊긴 몫 · 폴링 계수는 안 본다" 5.0 "$(peak_shed_pct "$counts")"
lib_case "중첩형" 25.0 "$(peak_shed_pct "$nested_counts")"
printf '%s' '{"metrics":{"peak_issue_shed":{"count":3}}}' > "$work/zero.json"
lib_case "발급이 없으면 -" - "$(peak_shed_pct "$work/zero.json")"
lib_case "없는 파일은 -" - "$(peak_shed_pct "$work/none.json")"

# docker stats 한 벌을 표본 줄로 옮긴다. 남의 프로젝트 컨테이너가 섞이면 그 CPU 가 천장 원인에
# 끼고, `%` 를 안 떼면 판정기가 숫자가 아닌 표본으로 읽어 매 회차 판정 불가가 된다.
stats=$(printf 'load-gateway-1\t95.30%%\nsearch-cache\t88.00%%\nload-redis-1\t12.05%%\n')
lib_case "우리 컨테이너만 · 백분율 기호를 뗀다" \
    "$(printf 'cpu\tload-gateway-1\t95.30\ncpu\tload-redis-1\t12.05')" \
    "$(printf '%s\n' "$stats" | peak_cpu_lines load)"
lib_case "빈 입력은 빈 출력" "" "$(printf '' | peak_cpu_lines load)"

# 레디스 명령 수와 네트워크 (CY-990). CPU 로 안 보이는 천장 둘이다.
info=$(printf '# Stats\r\ntotal_connections_received:12\r\ninstantaneous_ops_per_sec:48123\r\nrejected_connections:0\r\n')
lib_case "INFO 에서 초당 명령 수를 뽑는다 · 줄 끝 CR 을 뗀다" 48123 "$(printf '%s\n' "$info" | peak_ops_from_info)"
lib_case "그 줄이 없으면 빈 값" "" "$(printf 'uptime_in_seconds:3\n' | peak_ops_from_info)"
# 두 번 읽은 누적 바이트의 차분을 Mbit/s 로. 1초에 125,000,000 바이트는 1000 Mbit/s 다.
lib_case "차분을 Mbit/s 로" 1000.0 "$(peak_net_mbps 0 0 125000000 1000000000)"
lib_case "반 초면 두 배" 2000.0 "$(peak_net_mbps 0 0 125000000 500000000)"
lib_case "시간이 안 흘렀으면 빈 값" "" "$(peak_net_mbps 10 5 20 5)"
lib_case "누적이 줄었으면(재시작) 빈 값" "" "$(peak_net_mbps 500 0 100 1000000000)"
# /proc/<pid>/net/dev 에서 eth0 의 받은·보낸 바이트 합. 콜론 뒤에 공백이 없어도 읽는다.
netdev=$(printf 'Inter-|   Receive\n face |bytes\n    lo: 10 1 0 0 0 0 0 0 20 2 0 0 0 0 0 0\n  eth0:1500 9 0 0 0 0 0 0 2500 7 0 0 0 0 0 0\n')
lib_case "eth0 의 받은·보낸 합" 4000 "$(printf '%s\n' "$netdev" | peak_eth0_bytes)"

# 망 표집은 docker 와 /proc 을 거친다. 이름 칸을 잘못 읽어 표본이 한 줄도 안 쌓이던 적이 있어(CY-990) 가짜 둘로 잰다.
docker() {
    case "$1" in
        ps) printf 'zz-gateway-1\nzz-redis-1\nzz-backend-1\n' ;;
        inspect) case "$4" in zz-gateway-1) echo 101 ;; zz-redis-1) echo 102 ;; zz-backend-1) echo 103 ;; esac ;;
    esac
}
netdev() {
    mkdir -p "$work/proc/$1/net"
    printf 'Inter-|\n face |\n  eth0: %s 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n' "$2" > "$work/proc/$1/net/dev"
}
netdev 101 1000; netdev 102 5000; netdev 103 7
: > "$work/net.state"
lib_case "첫 바퀴는 앞 값이 없어 안 낸다" "" \
    "$(PEAK_PROC=$work/proc PEAK_NOW_NS=0 peak_net_lines zz "$work/net.state")"
netdev 101 1001000; netdev 102 100
lib_case "두 바퀴째는 Mbit/s · 재시작한 것과 표집 밖의 것은 뺀다" "$(printf 'net\tzz-gateway-1\t8.0')" \
    "$(PEAK_PROC=$work/proc PEAK_NOW_NS=1000000000 peak_net_lines zz "$work/net.state")"
unset -f docker netdev

# 호스트 유휴. **못 읽으면 0.0 을 내면 안 된다** — 천장 원인 판정의 첫 규칙이 유휴 바닥이라,
# 계기를 못 읽은 회차가 전부 호스트 탓으로 기록된다.
printf 'cpu  100 0 100 800 0 0 0 0\ncpu0 1 2 3 4\n' > "$work/stat.ok"
lib_case "정상 파일이면 수를 낸다" 100.0 "$(PEAK_STAT=$work/stat.ok PEAK_IDLE_WAIT=0 peak_host_idle_pct)"
lib_case "없는 파일이면 판정 불가를 낸다" NA "$(PEAK_STAT=$work/nope PEAK_IDLE_WAIT=0 peak_host_idle_pct)"
printf 'intr 1 2 3\n' > "$work/stat.bad"
lib_case "cpu 줄이 없으면 판정 불가를 낸다" NA "$(PEAK_STAT=$work/stat.bad PEAK_IDLE_WAIT=0 peak_host_idle_pct)"

# 줄 키 목록. 세 러너가 제각각이면 지우다 만 회차가 앞 회차 값을 들고 넘어간다.
lib_case "줄 키는 한 곳에서 온다" \
    "queue:{c1} admitted:{c1} maxscore:{c1} grace:{c1} alive:{c1} dropfence:{c1} applyfence:{c1}" \
    "$(queue_keys c1)"

# 대마다 낸 판정 비율을 모은다. **합산하지 않는다** — 한 대가 다시 떴거나 못 긁은 칸이 다른 대의 계수에 묻힌다.
# 가장 나쁜 것을 쓴다. 판정 불가가 미달보다 앞이다 — 한 대를 못 잰 칸은 나머지가 미달이어도 제품 탓으로 못 읽는다.
lib_case "모두 서면 ok" ok "$(peak_worst_verdict ok ok)"
lib_case "한 대만 미달이어도 미달" under "$(peak_worst_verdict ok under)"
lib_case "한 대만 못 쟀어도 판정 불가" unmeasurable "$(peak_worst_verdict ok unmeasurable)"
lib_case "판정 불가가 미달보다 앞" unmeasurable "$(peak_worst_verdict under unmeasurable ok)"
lib_case "모르는 판정은 판정 불가" unmeasurable "$(peak_worst_verdict ok 뭔가)"
lib_case "받은 것이 없으면 판정 불가" unmeasurable "$(peak_worst_verdict)"

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

# **임계 모양이 둘이다.** 비어 있지 않다는 것만 보면 통과한 중첩형도 깨진 것이
# 되어, 드롭 하나로 99 가 난 회차의 멀쩡한 임계까지 제품 미달로 읽힌다.
nest_ok=$(thr '"dropped_iterations":{"thresholds":{"count==0":{"ok":false}}},"http_req_failed":{"thresholds":{"rate<0.01":{"ok":true}}}' nest_ok.json)
nest_bad=$(thr '"http_req_failed":{"thresholds":{"rate<0.01":{"ok":false}}}' nest_bad.json)
lib_case "중첩형에서 통과한 임계는 안 깨진 것" ok "$(peak_verdict_from_k6 99 "$nest_ok")"
lib_case "중첩형에서 깨진 임계는 안 선다" under "$(peak_verdict_from_k6 99 "$nest_bad")"

[ "$selftest_failed" -eq 0 ] && echo "최대치 자기검증 통과" || echo "최대치 자기검증 실패"
exit "$selftest_failed"

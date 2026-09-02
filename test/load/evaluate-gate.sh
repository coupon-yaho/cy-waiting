#!/usr/bin/env bash
# 계획서 기준으로 판정한다.
#
# **k6 임계만 보면 기준이 둘이 된다.** 그쪽은 조기 중단용이고, 통과 여부는
# 여기서 정한다 — 계획서가 바뀌면 고칠 곳이 한 곳이어야 한다.
set -uo pipefail

scenario="${1:?시나리오 이름}"
summary="${2:?k6 요약 JSON}"

failed=0

# **두 형식을 다 받는다.** k6 실행에 따라 집계값이 `.value` 에도 `.values.*` 에도
# 온다. 한쪽만 읽으면 실행을 올리는 순간 전부 빈 값이 되고, 그러면 판정이 아니라
# "하네스가 안 돌았다" 로 읽힌다.
read_metric() {
  jq -r "$1 // $2 // empty" "$summary"
}

report() {
  printf '  %-28s %s\n' "$1" "$2"
}

violate() {
  printf '::error title=부하 기준 미달::%s\n' "$1"
  failed=1
}

# **숫자인지 먼저 본다.** awk 는 숫자꼴이 아닌 값을 문자열로 비교해서
# "HELLO" >= 0.99 가 참이 된다 — 깨진 요약이 통과로 읽힌다.
numeric() {
  [[ "${1:-}" =~ ^-?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$ ]]
}

at_least() {   # 값 하한 이름
  if ! numeric "${1:-}"; then
    violate "$3 가 숫자가 아니다: ${1:-없음}"
    return
  fi
  awk -v v="$1" -v m="$2" 'BEGIN { exit (v + 0 >= m + 0) ? 0 : 1 }' \
    || violate "$3 $1 < $2"
}

at_most() {    # 값 상한 이름
  if ! numeric "${1:-}"; then
    violate "$3 가 숫자가 아니다: ${1:-없음}"
    return
  fi
  awk -v v="$1" -v m="$2" 'BEGIN { exit (v + 0 <= m + 0) ? 0 : 1 }' \
    || violate "$3 $1 > $2"
}

at_below() {   # 값 상한 이름 — 경계를 통과로 안 센다
  if ! numeric "${1:-}"; then
    violate "$3 가 숫자가 아니다: ${1:-없음}"
    return
  fi
  awk -v v="$1" -v m="$2" 'BEGIN { exit (v + 0 < m + 0) ? 0 : 1 }' \
    || violate "$3 $1 >= $2"
}

# **없는 값을 0 으로 안 읽는다.** 무결성 카운터는 시나리오가 반드시 내보낸다.
# 안 나왔으면 0 건이었던 것이 아니라 요약이 깨진 것이고, 그 회차는 못 믿는다.
required() {   # 값 이름
  if [[ -z "${1:-}" ]]; then
    violate "$2 가 요약에 없다 — 시나리오가 내보내는 값이라 없으면 깨진 실행이다"
    return 1
  fi
  return 0
}

# **덜 실린 실행을 통과시키지 않는다.** 도착률 실행기는 VU 가 모자라면 반복을
# 조용히 버린다. 검사 통과율은 실행된 반복만 세므로 그 사실을 못 본다 —
# 하한이 1 이면 "거의 다 버려진 실행" 이 초록으로 남는다.
delivered() {   # 실측 최소 이름
  local dropped
  dropped=$(read_metric '.metrics.dropped_iterations.count' \
                        '.metrics.dropped_iterations.values.count')
  report "버려진 반복" "${dropped:-0}"
  at_most "${dropped:-0}" "$2" "버려진 반복"
  at_least "$1" "$3" "$4"
}

case "$scenario" in
  smoke)
    checks=$(read_metric '.metrics.checks.value' '.metrics.checks.values.rate')
    req_failed=$(read_metric '.metrics.http_req_failed.value' \
                             '.metrics.http_req_failed.values.rate')
    p95=$(read_metric '.metrics.http_req_duration["p(95)"]' \
                      '.metrics.http_req_duration.values["p(95)"]')
    reqs=$(read_metric '.metrics.http_reqs.count' '.metrics.http_reqs.values.count')

    report "검사 통과율" "${checks:-없음}"
    report "요청 실패율" "${req_failed:-없음}"
    # 판정에 안 쓴다. 게이트웨이 오버헤드 기준은 뒷단 지연을 뺀 값이라 별도다.
    report "p95(ms · 미판정)" "${p95:-없음}"
    report "요청 수" "${reqs:-없음}"

    # 요약이 비면 k6 가 안 돈 것이다. 그 상태를 통과로 세면 잡이 늘 초록이다.
    at_least "${reqs:-}" 1 "요청 수"
    at_least "${checks:-}" 0.99 "검사 통과율"
    # 계약에 없는 응답이 섞이면 그건 배선이 어긋난 것이다.
    at_most "${req_failed:-}" 0.01 "요청 실패율"
    ;;
  idle-coupon)
    checks=$(read_metric '.metrics.checks.value' '.metrics.checks.values.rate')
    queued=$(read_metric '.metrics.queued_responses.count' \
                         '.metrics.queued_responses.values.count')
    admitted=$(read_metric '.metrics.admitted_responses.count' \
                           '.metrics.admitted_responses.values.count')
    reqs=$(read_metric '.metrics.http_reqs.count' '.metrics.http_reqs.values.count')

    report "검사 통과율" "${checks:-없음}"
    report "줄 선 응답" "${queued:-없음}"
    report "뒷단까지 간 응답" "${admitted:-없음}"
    report "요청 수" "${reqs:-없음}"

    at_least "${reqs:-}" 1 "요청 수"
    # **하나라도 뒷단까지 가야 한다.** 전부 막혔는데 줄도 안 섰으면 0/0 이라
    # 아래 상한이 그냥 통과한다 — 아무것도 안 지나간 초록이다.
    at_least "${admitted:-}" 1 "뒷단까지 간 응답"
    at_least "${checks:-}" 0.99 "검사 통과율"
    # **R1 은 값으로 못 박는다.** 한산한 쿠폰이 줄을 서도 응답은 202 라 정상으로
    # 보이고, 사용자는 기다릴 뿐 오류를 안 본다 — 레거시가 뒤집힌 방식이다.
    at_most "${queued:-}" 0 "줄 선 응답"

    # **줄 키가 아예 없어야 한다.** 응답만 보면 게이트웨이가 등록해 놓고 통과
    # 응답을 내도 통과한다. 요청 경로에서 레디스를 안 친다는 것(불변식 1)의
    # 유일한 직접 증거다.
    if [[ -n "${REDIS_CLI:-}" ]]; then
      queue_len=$($REDIS_CLI --no-raw ZCARD 'queue:{c1}' 2>/dev/null | tr -dc '0-9')
      report "줄 키 길이" "${queue_len:-없음}"
      at_most "${queue_len:-}" 0 "줄 키 길이"
    else
      # 못 재는 것을 통과로 세지 않는다. 그러면 이 검사가 있으나 마나다.
      violate "REDIS_CLI 가 없어 줄 키를 못 봤다 — 이 시나리오의 핵심 증거다"
    fi
    ;;
  contended-coupon)
    queued=$(read_metric '.metrics.queued_responses.count' \
                         '.metrics.queued_responses.values.count')
    passed=$(read_metric '.metrics.passed_responses.count' \
                         '.metrics.passed_responses.values.count')
    checks=$(read_metric '.metrics.checks.value' '.metrics.checks.values.rate')
    reqs=$(read_metric '.metrics.http_reqs.count' '.metrics.http_reqs.values.count')

    report "줄 선 응답" "${queued:-없음}"
    report "지나간 응답" "${passed:-없음}"
    report "검사 통과율" "${checks:-없음}"
    report "요청 수" "${reqs:-없음}"

    at_least "${reqs:-}" 1 "요청 수"
    at_least "${checks:-}" 0.99 "검사 통과율"
    # **대조군의 전부다.** 줄이 한 번도 안 서면 idle 쪽 초록은 "대기열이 꺼져
    # 있다" 와 구별되지 않는다.
    at_least "${queued:-}" 1 "줄 선 응답"

    # **줄 키가 실제로 자라야 한다.** 응답만 보면 게이트웨이가 202 를 내면서
    # 레디스에 안 올려도 통과한다 — 그러면 순번이 아무 데도 없다.
    if [[ -n "${REDIS_CLI:-}" ]]; then
      queue_len=$($REDIS_CLI --no-raw ZCARD 'queue:{c2}' 2>/dev/null | tr -dc '0-9')
      report "줄 키 길이" "${queue_len:-없음}"
      at_least "${queue_len:-}" 1 "줄 키 길이"
    else
      violate "REDIS_CLI 가 없어 줄 키를 못 봤다 — 이 시나리오의 핵심 증거다"
    fi
    ;;
  read-burst)
    checks=$(read_metric '.metrics.checks.value' '.metrics.checks.values.rate')
    served=$(read_metric '.metrics.served_responses.count' \
                         '.metrics.served_responses.values.count')
    reqs=$(read_metric '.metrics.http_reqs.count' '.metrics.http_reqs.values.count')

    report "검사 통과율" "${checks:-없음}"
    report "온전한 응답" "${served:-없음}"
    report "요청 수" "${reqs:-없음}"

    # **목표 부하를 실제로 넣었는지 본다.** 하한이 1 이면 10건에 뒷단 1건도
    # 배수 10 을 만족해 통과한다. 시나리오는 초당 2,000 을 10초 보낸다.
    at_least "${reqs:-}" 18000 "요청 수"
    at_least "${checks:-}" 0.99 "검사 통과율"

    # **뒷단이 몇 번 받았는지가 이 시나리오의 전부다.** 응답만 보면 코얼레싱이
    # 통째로 꺼져 있어도 통과한다 — 재지 못하는 보호 장치는 없는 것과 같다.
    if [[ -n "${STUB_SERVED:-}" ]]; then
      report "뒷단 도달" "${STUB_SERVED}"
      report "병합 배수" "$(awk -v a="${reqs:-0}" -v b="${STUB_SERVED:-1}" \
          'BEGIN { printf "%.1f", (b + 0 == 0) ? 0 : (a + 0) / (b + 0) }')"
      # **0 을 통과로 세지 않는다.** 하한이 없으면 "완벽히 모았다" 와 "정규식이
      # 안 맞았다 / 요청이 뒷단에 하나도 안 갔다" 가 같은 초록이 된다.
      at_least "${STUB_SERVED}" 1 "뒷단 도달"
      # 수명이 300ms 이므로 10초 동안 뒷단은 많아야 수십 번 받아야 한다.
      # 요청 수만큼 받았으면 하나도 안 모인 것이다.
      at_most "${STUB_SERVED}" 200 "뒷단 도달"
      # 배수도 판정에 쓴다. 출력만 하면 그 값이 1 이어도 통과한다.
      at_least "$(awk -v a="${reqs:-0}" -v b="${STUB_SERVED:-1}" \
          'BEGIN { printf "%.1f", (b + 0 == 0) ? 0 : (a + 0) / (b + 0) }')" \
          10 "병합 배수"
    else
      violate "STUB_SERVED 가 없어 뒷단 도달 수를 못 봤다 — 이 시나리오의 핵심 증거다"
    fi
    ;;
  read-window)
    checks=$(read_metric '.metrics.checks.value' '.metrics.checks.values.rate')
    reqs=$(read_metric '.metrics.http_reqs.count' '.metrics.http_reqs.values.count')

    report "검사 통과율" "${checks:-없음}"
    report "요청 수" "${reqs:-없음}"

    at_least "${reqs:-}" 9000 "요청 수"
    at_least "${checks:-}" 0.99 "검사 통과율"

    # **"동시 1만" 을 하네스가 못 만든다.** 1만 VU 를 띄우는 데만 십수 초가
    # 걸려서 도착이 수명 창 수십 개에 흩어진다. 그래서 뒷단 도달 수를 절대값으로
    # 재는 대신 병합 배수로 잰다 — 재려던 것(같은 조회가 뒷단에 한 번만 간다)은
    # 그 값이 답한다.
    if [[ -n "${STUB_SERVED:-}" ]]; then
      report "뒷단 도달" "${STUB_SERVED}"
      report "병합 배수" "$(awk -v a="${reqs:-0}" -v b="${STUB_SERVED:-1}" \
          'BEGIN { printf "%.0f", (b + 0 == 0) ? 0 : (a + 0) / (b + 0) }')"
      at_least "${STUB_SERVED}" 1 "뒷단 도달"
      at_least "$(awk -v a="${reqs:-0}" -v b="${STUB_SERVED:-1}" \
          'BEGIN { printf "%.0f", (b + 0 == 0) ? 0 : (a + 0) / (b + 0) }')" \
          500 "병합 배수"
    else
      violate "STUB_SERVED 가 없어 뒷단 도달 수를 못 봤다 — 이 시나리오의 핵심 증거다"
    fi
    ;;
  open-spike)
    checks=$(read_metric '.metrics.checks.value' '.metrics.checks.values.rate')
    reqs=$(read_metric '.metrics.http_reqs.count' '.metrics.http_reqs.values.count')
    queued=$(read_metric '.metrics.queued_responses.count' \
                         '.metrics.queued_responses.values.count')
    shed=$(read_metric '.metrics.shed_responses.count' '.metrics.shed_responses.values.count')
    retry_min=$(read_metric '.metrics.retry_after_seconds.min' \
                            '.metrics.retry_after_seconds.values.min')
    retry_max=$(read_metric '.metrics.retry_after_seconds.max' \
                            '.metrics.retry_after_seconds.values.max')

    report "검사 통과율" "${checks:-없음}"
    report "요청 수" "${reqs:-없음}"
    report "줄 선 응답" "${queued:-없음}"
    report "막힌 응답" "${shed:-없음}"
    report "다시 올 시각(초)" "${retry_min:-없음} ~ ${retry_max:-없음}"

    # **정한 인원이 다 던져야 한다.** 하한이 1 이면 한 명이 보낸 실행도 통과하고,
    # 그 값으로 Phase 10 착수를 판정하게 된다.
    at_least "${reqs:-}" "${SPIKE_USERS:-20000}" "요청 수"
    at_least "${checks:-}" 0.99 "검사 통과율"

    # **도착이 짧은 창에 몰려야 스파이크다.** 늘어지면 같은 인원이라도 선착순
    # 오픈이 아니라 지속 부하를 잰 것이다 — 1초 균등은 실제 스파이크를 다섯 배
    # 과소평가한다.
    arrival_rate=$(read_metric '.metrics.http_reqs.rate' '.metrics.http_reqs.values.rate')
    report "실측 도착률(초당)" "${arrival_rate:-없음}"
    at_least "${arrival_rate:-}" "${SPIKE_MIN_RATE:-2000}" "실측 도착률"

    # **줄이 서야 한다.** 안 서면 스파이크가 유휴 몫을 못 넘긴 것이다.
    at_least "${queued:-}" 1 "줄 선 응답"

    # **다시 올 시각이 흩어져야 한다** (F7 · G6.20). 한 값으로 몰리면 그 초에
    # 같은 스파이크가 다시 오고, 회복이 곧 두 번째 사고가 된다.
    #
    # 폭만 보면 부족하다 — 만 건 중 9,999 건이 한 값이고 하나만 멀리 있어도
    # 폭은 넓다. 분위수가 서로 떨어져 있는지까지 본다.
    # **사분위를 촘촘히 본다.** 둘만 보면 덩어리 둘로 나뉜 분포가 통과한다 —
    # 절반이 24초, 40%가 30초, 나머지가 36초면 최소·중앙·p90 이 서로 다르지만
    # 89%가 두 덩어리로 돌아온다. 덩어리는 반드시 이웃한 분위수를 붙여 놓는다.
    retry_p10=$(read_metric '.metrics.retry_after_seconds["p(10)"]' \
                            '.metrics.retry_after_seconds.values["p(10)"]')
    retry_p25=$(read_metric '.metrics.retry_after_seconds["p(25)"]' \
                            '.metrics.retry_after_seconds.values["p(25)"]')
    retry_med=$(read_metric '.metrics.retry_after_seconds.med' \
                            '.metrics.retry_after_seconds.values.med')
    retry_p75=$(read_metric '.metrics.retry_after_seconds["p(75)"]' \
                            '.metrics.retry_after_seconds.values["p(75)"]')
    retry_p90=$(read_metric '.metrics.retry_after_seconds["p(90)"]' \
                            '.metrics.retry_after_seconds.values["p(90)"]')
    if [[ -n "${shed:-}" ]] && awk -v v="${shed:-0}" 'BEGIN { exit (v + 0 > 0) ? 0 : 1 }'; then
      report "다시 올 시각 분포" \
          "${retry_min:-?} / ${retry_p10:-?} / ${retry_p25:-?} / ${retry_med:-?} / ${retry_p75:-?} / ${retry_p90:-?} / ${retry_max:-?}"
      at_least "$(awk -v a="${retry_max:-0}" -v b="${retry_min:-0}" \
          'BEGIN { print a - b }')" 0.5 "다시 올 시각의 폭"
      # 이웃한 분위수가 붙어 있으면 그 사이에 덩어리가 있다는 뜻이다.
      prev="${retry_min:-}"
      for name in p10 p25 med p75 p90 max; do
        eval "next=\${retry_$name:-}"
        if ! numeric "${prev:-}" || ! numeric "${next:-}"; then
          violate "막힌 응답이 있는데 Retry-After 의 $name 을 못 읽었다"
          break
        fi
        at_least "$(awk -v a="$next" -v b="$prev" 'BEGIN { print a - b }')" 0.5 \
            "다시 올 시각의 $name 간격"
        prev="$next"
      done
    fi
    ;;
  mixed)
    checks=$(read_metric '.metrics.checks.value' '.metrics.checks.values.rate')
    hot=$(read_metric '.metrics.hot_queued.count' '.metrics.hot_queued.values.count')
    cold_q=$(read_metric '.metrics.cold_queued.count' '.metrics.cold_queued.values.count')
    cold_p=$(read_metric '.metrics.cold_passed.count' '.metrics.cold_passed.values.count')

    report "핫이 줄 선 수" "${hot:-없음}"
    report "콜드가 줄 선 수" "${cold_q:-없음}"
    report "콜드가 지나간 수" "${cold_p:-없음}"
    report "검사 통과율" "${checks:-없음}"

    # **핫이 몰려야 이 실행이 혼합이다.** 시나리오는 초당 800 을 20초 보낸다 —
    # 하한이 1 이면 거의 다 버려진 실행도 통과하고, 그때 격리를 안 잰 것이다.
    delivered "${hot:-}" 1000 14000 "핫이 줄 선 수"
    # 콜드는 초당 5 를 18초. 넉넉히 잡아도 여든은 넘어야 한다.
    at_least "${cold_p:-}" 80 "콜드가 지나간 수"
    # **콜드가 한 번이라도 줄을 서면 격리가 깨진 것이다** (R1).
    at_most "${cold_q:-}" 0 "콜드가 줄 선 수"
    at_least "${checks:-}" 0.99 "검사 통과율"
    ;;
  abandonment)
    checks=$(read_metric '.metrics.checks.value' '.metrics.checks.values.rate')
    joined=$(read_metric '.metrics.joined.count' '.metrics.joined.values.count')
    left=$(read_metric '.metrics.abandoned.count' '.metrics.abandoned.values.count')
    polled=$(read_metric '.metrics.polled.count' '.metrics.polled.values.count')

    report "줄 선 수" "${joined:-없음}"
    report "이탈한 수" "${left:-없음}"
    report "다시 온 수" "${polled:-없음}"
    report "검사 통과율" "${checks:-없음}"

    # 시나리오는 초당 300 을 20초 보낸다. 하한이 1 이면 거의 다 버려진 실행도
    # 통과하고, 그때 이탈률은 아무 뜻이 없다.
    delivered "${joined:-}" 600 5000 "줄 선 수"
    # **이탈자가 있어야 잴 수 있다.** 없으면 이 시나리오가 이탈을 안 만든 것이다.
    at_least "${left:-}" 1 "이탈한 수"
    # **안 이탈한 사람은 다시 와야 한다.** 안 오면 이탈률이 100% 로 보이고,
    # 그때는 이탈이 아니라 배선 오류를 재는 것이다.
    at_least "${polled:-}" 1 "다시 온 수"
    at_least "${checks:-}" 0.99 "검사 통과율"

    # **줄 선 사람은 이탈했거나 다시 왔거나 둘 중 하나다.** 합이 안 맞으면 그
    # 차이만큼이 어느 쪽으로도 안 세어진 것이고, 그때 이탈률은 틀린 값이다 —
    # 폴링이 일부만 실패해도 여기가 벌어진다.
    if numeric "${joined:-}" && numeric "${left:-}" && numeric "${polled:-}"; then
      report "합이 맞는가" "$(awk -v j="$joined" -v a="$left" -v p="$polled" \
          'BEGIN { printf "%d = %d + %d", j, a, p }')"
      awk -v j="$joined" -v a="$left" -v p="$polled" \
          'BEGIN { exit (j == a + p) ? 0 : 1 }' \
          || violate "줄 선 수가 이탈과 재방문의 합과 다르다 — 어느 쪽으로도 안 세어진 요청이 있다"
    fi

    # **크레딧 낭비는 아직 못 잰다.** 큐에서 나가는 경로가 Phase 7 이라, 지금은
    # 이탈자가 그냥 줄에 남는다 — 낭비율을 여기서 판정하면 없는 기능을 재는 것이다.
    report "크레딧 낭비 판정" "Phase 7 에서 (G7.5)"
    ;;
  no-shared-marker)
    # **계약이 안 선 상태를 잰다.** 스텁이 공유 선언을 안 하면 게이트웨이가
    # 안 모으고, 그때 뒷단 도달이 요청 수와 같아야 한다. 이 실행이 없으면
    # "붙이기 전까지 어떻게 되는가" 를 아무도 안 본다.
    checks=$(read_metric '.metrics.checks.value' '.metrics.checks.values.rate')
    reqs=$(read_metric '.metrics.http_reqs.count' '.metrics.http_reqs.values.count')

    report "요청 수" "${reqs:-없음}"
    report "검사 통과율" "${checks:-없음}"

    delivered "${reqs:-}" 100 2000 "요청 수"
    at_least "${checks:-}" 0.99 "검사 통과율"

    if [[ -n "${STUB_SERVED:-}" ]]; then
      # **반올림한 값으로 판정하지 않는다.** 1.054 가 1.05 가 되어 통과한다.
      ratio=$(awk -v a="${reqs:-0}" -v b="${STUB_SERVED:-1}" \
          'BEGIN { print (b + 0 == 0) ? 0 : (a + 0) / (b + 0) }')
      report "뒷단 도달" "${STUB_SERVED}"
      report "병합 배수" "$(awk -v r="$ratio" 'BEGIN { printf "%.2f", r }')"
      at_least "${STUB_SERVED}" 1 "뒷단 도달"
      # **1 이어야 한다.** 1 보다 크면 선언을 안 한 응답을 나눠 준 것이고,
      # 그것이 이 변경이 막으려던 것이다. 반대로 모으기가 통째로 꺼져 있어도
      # 1 이므로, 같은 하네스의 read-window 가 짝으로 500 배 이상을 요구한다.
      at_most "$ratio" 1.05 "병합 배수"
    else
      violate "STUB_SERVED 가 없어 뒷단 도달 수를 못 봤다 — 이 시나리오의 핵심 증거다"
    fi
    ;;
  overhead)
    checks=$(read_metric '.metrics.checks.value' '.metrics.checks.values.rate')
    reqs=$(read_metric '.metrics.http_reqs.count' '.metrics.http_reqs.values.count')
    # **판정하는 값.** 게이트웨이 왕복과 바닥을 같은 반복에서 재고 그 자리에서
    # 뺀 것이라, 한 표본이 품은 하네스 몫이 그 표본에서 빠진다. 서로 다른
    # 분포의 분위수를 빼면 그 값은 어느 표본의 것도 아니다.
    own99=$(read_metric '.metrics.gateway_overhead_ms["p(99)"]' \
                        '.metrics.gateway_overhead_ms.values["p(99)"]')
    own50=$(read_metric '.metrics.gateway_overhead_ms.med' \
                        '.metrics.gateway_overhead_ms.values.med')
    sold_own99=$(read_metric '.metrics.soldout_overhead_ms["p(99)"]' \
                             '.metrics.soldout_overhead_ms.values["p(99)"]')
    # 아래 셋은 판정에 안 쓴다. 무엇이 얼마였는지를 사람이 보는 값이다.
    gw99=$(read_metric '.metrics.gateway_own_ms["p(99)"]' \
                       '.metrics.gateway_own_ms.values["p(99)"]')
    base99=$(read_metric '.metrics.harness_baseline_ms["p(99)"]' \
                         '.metrics.harness_baseline_ms.values["p(99)"]')
    base50=$(read_metric '.metrics.harness_baseline_ms.med' \
                         '.metrics.harness_baseline_ms.values.med')

    gw_measured=$(read_metric '.metrics.gateway_measured.count' \
                              '.metrics.gateway_measured.values.count')
    sold_measured=$(read_metric '.metrics.soldout_measured.count' \
                                '.metrics.soldout_measured.values.count')
    unmeasured=$(read_metric '.metrics.overhead_unmeasured.count' \
                             '.metrics.overhead_unmeasured.values.count')
    clamped=$(read_metric '.metrics.overhead_clamped.count' \
                          '.metrics.overhead_clamped.values.count')

    report "요청 수" "${reqs:-없음}"
    report "갈래별 표본 수" "한산 ${gw_measured:-없음} / 매진 ${sold_measured:-없음}"
    report "게이트웨이 잔차 p99(ms)" "${gw99:-없음}"
    report "하네스 바닥 p50/p99(ms)" "${base50:-없음} / ${base99:-없음}"
    report "게이트웨이 오버헤드 p50/p99(ms)" "${own50:-없음} / ${own99:-없음}"
    report "매진 단락 오버헤드 p99(ms)" "${sold_own99:-없음}"
    report "검사 통과율" "${checks:-없음}"

    # 예열 3,000 · 한산 18,000 · 매진 3,600 = 24,600 을 기대한다 (반복마다
    # 요청 둘). 하한이 1 이면 한 건 보낸 실행도 통과한다.
    delivered "${reqs:-}" 300 20000 "요청 수"
    at_least "${checks:-}" 0.99 "검사 통과율"

    # **임계가 달린 Trend 는 표본이 없어도 요약에서 안 사라지고 전부 0 으로
    # 남는다.** 0 은 상한을 안 넘으므로, 표본 수를 따로 못 박지 않으면
    # 아무것도 안 잰 실행이 통과한다. 갈래마다 따로 못 박는다 — 하나로 합치면
    # 한쪽이 죽어도 나머지가 하한을 채울 수 있고, 그 보호는 도착률에 달린
    # 우연이다.
    at_least "${gw_measured:-0}" 6000 "한산 갈래 표본 수"
    at_least "${sold_measured:-0}" 1000 "매진 갈래 표본 수"

    # **없는 값을 0 으로 안 읽는다.** 이 둘은 시나리오가 반드시 내보내므로,
    # 안 나왔으면 0 건이었던 것이 아니라 요약이 깨진 것이다.
    #
    # 문턱이 비율이 아니라 0 인 것은 그 헤더가 붙거나 안 붙거나 둘 중
    # 하나라서다. 하나라도 빠졌으면 스텁이 바뀐 것이고, 그 회차의 숫자는 무엇을
    # 뺀 값인지 모르는 값이다. 허용치를 두면 그 사실이 허용치 안에 숨는다.
    if required "${unmeasured:-}" "못 잰 응답"; then
      at_most "$unmeasured" 0 "못 잰 응답"
    fi
    if required "${clamped:-}" "음수로 나온 응답"; then
      at_most "$clamped" 0 "음수로 나온 응답"
    fi

    # **바닥이 시끄러우면 짝지어 빼도 잡음이 커진다.** 상한을 안 두면 러너가
    # 느릴수록 꼬리가 넓어져 판정이 흔들린다. 이 값이 걸린 실행은 느린 실행이
    # 아니라 못 재는 실행이다.
    at_most "${base99:-}" 2.5 "하네스 바닥 p99(ms)"

    # **G6.11 은 `< 5ms` 다.** 경계를 통과로 세면 계획서가 금지한 값이 초록이 된다.
    at_below "${own99:-}" 5 "게이트웨이 오버헤드 p99(ms)"
    # 음수는 좋은 소식이 아니다. 게이트웨이를 지난 쪽이 스텁 직행보다 빨랐다는
    # 뜻이고, 그건 성능이 아니라 배선이 바뀐 것이다.
    at_least "${own99:-}" 0 "게이트웨이 오버헤드 p99(ms)"

    # 매진 단락은 뒷단도 Redis 도 안 거친다 (R3). 같은 방식으로 바닥을 뺀다.
    at_below "${sold_own99:-}" 5 "매진 단락 오버헤드 p99(ms)"
    at_least "${sold_own99:-}" 0 "매진 단락 오버헤드 p99(ms)"
    ;;
  *)
    # **모르는 시나리오를 통과로 안 센다.** 기본이 통과면 시나리오가 늘 때마다
    # 아무 기준 없는 잡이 하나씩 생기고, 그 초록은 아무 뜻이 없다.
    violate "'$scenario' 의 판정 기준이 없다 — 기준을 넣거나 시나리오를 지운다"
    ;;
esac

exit "$failed"

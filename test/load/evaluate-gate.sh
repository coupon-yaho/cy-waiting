#!/usr/bin/env bash
# 계획서 기준으로 판정한다.
#
# **k6 임계만 보면 기준이 둘이 된다.** 그쪽은 조기 중단용이고, 통과 여부는
# 여기서 정한다 — 계획서가 바뀌면 고칠 곳이 한 곳이어야 한다.
set -uo pipefail

scenario="${1:?시나리오 이름}"
summary="${2:?k6 요약 JSON}"

failed=0

# **두 형식을 다 받는다.** k6 판에 따라 집계값이 `.value` 에도 `.values.*` 에도
# 온다. 한쪽만 읽으면 판을 올리는 순간 전부 빈 값이 되고, 그러면 판정이 아니라
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

    # **정한 인원이 다 던져야 한다.** 하한이 1 이면 한 명이 보낸 판도 통과하고,
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
    if [[ -n "${shed:-}" ]] && awk -v v="${shed:-0}" 'BEGIN { exit (v + 0 > 0) ? 0 : 1 }'; then
      if numeric "${retry_min:-}" && numeric "${retry_max:-}"; then
        at_least "$(awk -v a="${retry_max}" -v b="${retry_min}" \
            'BEGIN { printf "%.2f", a - b }')" 0.5 "다시 올 시각의 폭"
      else
        violate "막힌 응답이 있는데 Retry-After 를 못 읽었다"
      fi
    fi
    ;;
  *)
    # **모르는 시나리오를 통과로 안 센다.** 기본이 통과면 시나리오가 늘 때마다
    # 아무 기준 없는 잡이 하나씩 생기고, 그 초록은 아무 뜻이 없다.
    violate "'$scenario' 의 판정 기준이 없다 — 기준을 넣거나 시나리오를 지운다"
    ;;
esac

exit "$failed"

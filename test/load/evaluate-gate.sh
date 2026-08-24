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
  *)
    # **모르는 시나리오를 통과로 안 센다.** 기본이 통과면 시나리오가 늘 때마다
    # 아무 기준 없는 잡이 하나씩 생기고, 그 초록은 아무 뜻이 없다.
    violate "'$scenario' 의 판정 기준이 없다 — 기준을 넣거나 시나리오를 지운다"
    ;;
esac

exit "$failed"

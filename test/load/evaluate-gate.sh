#!/usr/bin/env bash
# 계획서 기준으로 판정한다.
#
# **k6 임계만 보면 기준이 둘이 된다.** 그쪽은 조기 중단용이고, 통과 여부는
# 여기서 정한다 — 계획서가 바뀌면 고칠 곳이 한 곳이어야 한다.
set -euo pipefail

scenario="${1:?시나리오 이름}"
summary="${2:?k6 요약 JSON}"

failed=0

read_metric() {
  jq -r "$1 // empty" "$summary"
}

report() {
  printf '  %-28s %s\n' "$1" "$2"
}

violate() {
  printf '::error title=부하 기준 미달::%s\n' "$1"
  failed=1
}

case "$scenario" in
  smoke)
    checks=$(read_metric '.metrics.checks.value')
    req_failed=$(read_metric '.metrics.http_req_failed.value')
    p95=$(read_metric '.metrics.http_req_duration["p(95)"]')
    reqs=$(read_metric '.metrics.http_reqs.count')

    report "검사 통과율" "${checks:-없음}"
    report "요청 실패율" "${req_failed:-없음}"
    report "p95(ms)" "${p95:-없음}"
    report "요청 수" "${reqs:-없음}"

    # 요약이 비면 k6 가 안 돈 것이다. 그 상태를 통과로 세면 잡이 늘 초록이다.
    if [[ -z "${reqs:-}" ]] || (( ${reqs%.*} <= 0 )); then
      violate "요청이 하나도 안 나갔다 — 하네스가 안 돌았다"
    fi

    awk -v v="${checks:-0}" 'BEGIN { exit (v >= 0.99) ? 0 : 1 }' \
      || violate "검사 통과율 ${checks:-없음} < 0.99"

    # 계약에 없는 응답이 섞이면 그건 배선이 어긋난 것이다.
    awk -v v="${req_failed:-1}" 'BEGIN { exit (v <= 0.01) ? 0 : 1 }' \
      || violate "요청 실패율 ${req_failed:-없음} > 0.01"
    ;;
  *)
    report "판정 기준" "아직 없다"
    ;;
esac

exit "$failed"

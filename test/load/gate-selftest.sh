#!/usr/bin/env bash
# **판정 자체를 판정한다.**
#
# `evaluate-gate.sh` 는 하네스가 아니라 하네스의 심판이다. 그런데 심판이 틀리면
# 아무도 안 본다 — 실제로 이 저장소에서 게이트웨이를 아예 안 띄운 판이 통과했고,
# 표본 0 건인 판이 통과했다. 둘 다 초록이라 아무도 안 봤다.
#
# 스택도 k6 도 없이 몇 초에 돈다. 조작한 요약을 먹이고 종료 코드를 단언한다.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
gate="$here/evaluate-gate.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

failed=0

# 통과하는 판. 아래 사례들은 여기서 한 값씩만 비튼다 — 그래야 무엇이 잡혔는지가
# 값 하나로 좁혀진다.
base() {
  cat <<'JSON'
{
  "metrics": {
    "checks": { "value": 1 },
    "http_reqs": { "count": 21006 },
    "dropped_iterations": { "count": 0 },
    "gateway_measured": { "count": 9001 },
    "direct_measured": { "count": 9001 },
    "soldout_measured": { "count": 1801 },
    "overhead_unmeasured": { "count": 0 },
    "overhead_clamped": { "count": 0 },
    "gateway_own_ms": { "med": 2.08, "p(99)": 3.74 },
    "harness_baseline_ms": { "med": 0.78, "p(99)": 1.47 },
    "soldout_ms": { "med": 1.24, "p(99)": 2.23 }
  }
}
JSON
}

# 이름 기대 비틀기
case_is() {
  local name="$1" want="$2" tweak="$3"
  local file="$work/$RANDOM.json"
  if [[ -n "$tweak" ]]; then
    base | jq "$tweak" > "$file"
  else
    base > "$file"
  fi
  local got=0
  "$gate" overhead "$file" > "$work/out.txt" 2>&1 || got=$?
  if [[ "$got" == "$want" ]]; then
    printf '  \033[32m✓\033[0m %s\n' "$name"
  else
    printf '  \033[31m✗\033[0m %s — 기대 %s, 실제 %s\n' "$name" "$want" "$got"
    sed 's/^/      /' "$work/out.txt"
    failed=1
  fi
}

printf '\033[1m게이트 자기검사 — overhead\033[0m\n'

case_is "온전한 판은 통과한다" 0 ""

# **여기부터가 실제로 통과했던 상태들이다.**
case_is "게이트웨이 갈래가 통째로 죽으면 잡는다" 1 \
  '.metrics.gateway_own_ms = {"med":0,"p(99)":0} | .metrics.gateway_measured.count = 0'
case_is "대조군이 통째로 죽으면 잡는다" 1 \
  '.metrics.harness_baseline_ms = {"med":0,"p(99)":0} | .metrics.direct_measured.count = 0'
case_is "매진 갈래가 통째로 죽으면 잡는다" 1 \
  '.metrics.soldout_ms = {"med":0,"p(99)":0} | .metrics.soldout_measured.count = 0'

# 심판이 헐거워지는 방향
case_is "바닥이 시끄러우면 못 잰 판으로 본다" 1 \
  '.metrics.harness_baseline_ms["p(99)"] = 3.1'
case_is "대조군이 더 느리면 배선이 바뀐 것으로 본다" 1 \
  '.metrics.gateway_own_ms = {"med":0.3,"p(99)":0.4}'

# 실제 회귀
case_is "오버헤드가 문턱을 넘으면 잡는다" 1 \
  '.metrics.gateway_own_ms["p(99)"] = 6.0'
case_is "매진 단락이 문턱을 넘으면 잡는다" 1 \
  '.metrics.soldout_ms["p(99)"] = 6.2'

# 표본이 빠지거나 뒤집힌 판
case_is "못 잰 응답이 있으면 잡는다" 1 '.metrics.overhead_unmeasured.count = 1'
case_is "음수로 나온 응답이 있으면 잡는다" 1 '.metrics.overhead_clamped.count = 1'
case_is "반복이 버려지면 잡는다" 1 '.metrics.dropped_iterations.count = 400'

# 요약이 깨진 판. 숫자가 아닌 값을 통과로 세면 판정이 아니라 파싱을 재는 셈이다.
case_is "깨진 요약은 통과로 안 센다" 1 '.metrics.gateway_own_ms = {}'

if [[ "$failed" == 0 ]]; then
  printf '\033[1m게이트 자기검사 통과\033[0m\n'
else
  printf '\033[31m게이트 자기검사 미달\033[0m\n'
fi
exit "$failed"

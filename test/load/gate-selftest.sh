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
    "http_reqs": { "count": 24607 },
    "dropped_iterations": { "count": 0 },
    "gateway_measured": { "count": 9001 },
    "soldout_measured": { "count": 1801 },
    "overhead_unmeasured": { "count": 0 },
    "overhead_clamped": { "count": 0 },
    "gateway_own_ms": { "med": 2.08, "p(99)": 3.74 },
    "harness_baseline_ms": { "med": 0.78, "p(99)": 1.47 },
    "gateway_overhead_ms": { "med": 1.30, "p(99)": 2.27 },
    "soldout_ms": { "med": 1.24, "p(99)": 2.23 },
    "soldout_overhead_ms": { "med": 0.46, "p(99)": 1.45 }
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
case_is "한산 갈래가 통째로 죽으면 잡는다" 1 \
  '.metrics.gateway_overhead_ms = {"med":0,"p(99)":0} | .metrics.gateway_measured.count = 0'
case_is "매진 갈래가 통째로 죽으면 잡는다" 1 \
  '.metrics.soldout_overhead_ms = {"med":0,"p(99)":0} | .metrics.soldout_measured.count = 0'

# 심판이 헐거워지는 방향
case_is "바닥이 시끄러우면 못 잰 판으로 본다" 1 \
  '.metrics.harness_baseline_ms["p(99)"] = 3.1'
case_is "게이트웨이가 스텁 직행보다 빠르면 배선이 바뀐 것으로 본다" 1 \
  '.metrics.gateway_overhead_ms["p(99)"] = -0.4'

# 실제 회귀
case_is "오버헤드가 문턱을 넘으면 잡는다" 1 \
  '.metrics.gateway_overhead_ms["p(99)"] = 6.0'
# **G6.11 은 `< 5ms` 다.** 경계를 통과로 세면 계획서가 금지한 값이 초록이 된다.
case_is "문턱과 같은 값은 통과가 아니다" 1 \
  '.metrics.gateway_overhead_ms["p(99)"] = 5'
case_is "문턱 바로 아래는 통과한다" 0 \
  '.metrics.gateway_overhead_ms["p(99)"] = 4.999'
case_is "매진 단락이 문턱을 넘으면 잡는다" 1 \
  '.metrics.soldout_overhead_ms["p(99)"] = 6.2'

# 표본이 빠지거나 뒤집힌 판
case_is "못 잰 응답이 있으면 잡는다" 1 '.metrics.overhead_unmeasured.count = 1'
case_is "음수로 나온 응답이 있으면 잡는다" 1 '.metrics.overhead_clamped.count = 1'
case_is "반복이 버려지면 잡는다" 1 '.metrics.dropped_iterations.count = 400'

# **무결성 카운터가 사라진 판.** 0 건이었던 것이 아니라 요약이 깨진 것이다.
# 없는 값을 0 으로 읽으면 그 카운터를 지우는 것만으로 초록이 된다.
case_is "못 잰 응답 카운터가 없으면 깨진 판으로 본다" 1 \
  'del(.metrics.overhead_unmeasured)'
case_is "음수 카운터가 없으면 깨진 판으로 본다" 1 \
  'del(.metrics.overhead_clamped)'

# 요약이 깨진 판. 숫자가 아닌 값을 통과로 세면 판정이 아니라 파싱을 재는 셈이다.
case_is "깨진 요약은 통과로 안 센다" 1 '.metrics.gateway_overhead_ms = {}'

# ── G7.5 낭비 판정 (evaluate-waste.sh) ──────────────────────────────────────
#
# **심판이 틀리면 아무도 안 본다.** 이 판정은 실제로 상수 하나 때문에 같은
# 측정에서 통과와 미달이 갈렸다. 조작한 입력으로 그 갈래들을 못 박는다.
waste="$here/evaluate-waste.sh"

# 표본: 100초에 임계가 60,000,000 μs 자리, 200초에 200,000,000 자리.
waste_samples() {
  printf '100 60000000\n200 200000000\n'
}

# 유령 하나. score 가 곧 줄 선 시각(μs)이라 50초에 줄을 섰고, 임계는 100초에
# 그를 지났다 — **기다린 시간 50초.**
waste_ghost() { printf 'm1\t50000000\n'; }

run_waste() {  # 이름 기대코드 [인자...]
  local name="$1" want="$2"; shift 2
  "$waste" "$@" >"$work/waste.log" 2>&1
  local rc=$?
  if [ "$rc" -ne "$want" ]; then
    echo "  ✗ $name — 기대 $want, 실제 $rc"
    sed 's/^/      /' "$work/waste.log"
    failed=1
  else
    echo "  ✓ $name"
  fi
}

echo "== G7.5 낭비 판정 =="
waste_samples > "$work/s.log"

# 수명 40초: 50초를 기다렸으니 걷었어야 한다 → 피할 수 있었던 낭비
waste_ghost > "$work/g.tsv"
run_waste "피할 수 있었던 유령을 잡는다" 1 "$work/s.log" "$work/g.tsv" 1000 10 20 40

# 수명 60초: 50초는 수명 안이라 못 피한다 → 통과
run_waste "수명 안의 유령은 안 센다" 0 "$work/s.log" "$work/g.tsv" 1000 10 20 60

# 유령이 없으면 "기구를 안 잰 판" 이라 미판정이어야 한다 — 통과로 읽으면
# 이탈이 안 일어난 판이 매번 초록이 된다
: > "$work/empty.tsv"
run_waste "유령이 없으면 미판정" 1 "$work/s.log" "$work/empty.tsv" 1000 10 20 60

# 차례가 몇 명뿐이면 비율에 뜻이 없다
run_waste "차례가 적으면 미판정" 1 "$work/s.log" "$work/g.tsv" 10 10 20 60

# **과잉 청소.** 걷은 수가 이탈한 수를 넘으면 살아 있는 사람을 걷은 것이다
run_waste "과잉 청소를 미달로 잡는다" 1 "$work/s.log" "$work/g.tsv" 1000 30 20 60

# 표본이 비면 유령이 언제 차례를 받았는지 모른다
: > "$work/nosample.log"
run_waste "표본이 비면 미판정" 1 "$work/nosample.log" "$work/g.tsv" 1000 10 20 60


if [[ "$failed" == 0 ]]; then
  printf '\033[1m게이트 자기검사 통과\033[0m\n'
else
  printf '\033[31m게이트 자기검사 미달\033[0m\n'
fi
exit "$failed"

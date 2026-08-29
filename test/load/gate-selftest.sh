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
# 측정에서 통과와 미달이 갈렸다. 그리고 첫 판 자기검사는 **판정식을 통째로
# 지워도 초록**이었다 — 어느 사례도 낭비 비율이 문턱을 넘지 않아서다.
# 여기서는 문턱의 양쪽을 다 밟는다.
waste="$here/evaluate-waste.sh"

# 표본 셋. **첫 표본은 낮게 둔다** — 첫 표본에서 이미 지나가 있는 유령은
# 언제 지났는지 모르므로 판정에서 빠지고, 그러면 사례가 재려던 것을 못 잰다.
#
#   t=50  임계 10,000,000    t=100 임계 60,000,000    t=300 임계 200,000,000
#
# 되짚을 때 **직전 표본**을 하한으로 쓰므로 간격을 넉넉히 벌려 둔다 — 붙여
# 두면 여유(틱)를 조금만 바꿔도 사례가 갈래를 넘나든다.
waste_samples() { printf '50 10000000\n100 60000000\n300 200000000\n'; }

# 유령 n 명. score 가 곧 줄 선 시각(μs)이다.
#   50,000,000 → 50초에 줄 서서 100초에 차례. 직전 표본 50 이라 **하한 0초**
#   90,000,000 → 90초에 줄 서서 300초에 차례. 직전 표본 100 이라 **하한 10초**
waste_ghosts() {
  local n="$1" us="$2"
  for ((i = 0; i < n; i++)); do printf 'm%d_%d\t%d\n' "$us" "$i" "$us"; done
}

# 수명 5 에 틱 1 이면 하한 0초는 못 피하는 것, 10초는 피할 수 있었던 것이다.
못_피하는() { waste_ghosts "$1" 50000000; }
피할_수_있었던() { waste_ghosts "$1" 90000000; }

# 사례들이 쓰는 수명. 위 두 갈래를 가르는 값이다.
TTL=5

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

# **문턱의 양쪽을 밟는다.** 못 피하는 것 20명(정족수)에 피할 수 있었던 것을
# 얹어 비율을 만든다. 차례를 준 인원 1,000 이라 50명이면 정확히 5% 다.
{ 못_피하는 20; 피할_수_있었던 50; } > "$work/edge-pass.tsv"
{ 못_피하는 20; 피할_수_있었던 51; } > "$work/edge-fail.tsv"
run_waste "문턱과 같은 값은 통과한다" 0 "$work/s.log" "$work/edge-pass.tsv" 1000 100 200 "$TTL"
run_waste "문턱을 넘으면 미달이다" 1 "$work/s.log" "$work/edge-fail.tsv" 1000 100 200 "$TTL"

# 수명을 바꾸면 같은 유령이 갈래를 옮긴다. 경계가 `>=` 인 것도 같이 못 박는다.
못_피하는 30 > "$work/g30.tsv"
run_waste "수명 안의 유령은 안 센다" 0 "$work/s.log" "$work/g30.tsv" 1000 100 200 "$TTL"
# 하한이 정확히 `수명 + 틱` 인 유령은 피할 수 있었던 것이다 (`>=`).
{ 피할_수_있었던 51; 못_피하는 20; } > "$work/exact.tsv"
run_waste "수명과 같으면 피할 수 있었던 것이다" 1 "$work/s.log" "$work/exact.tsv" 1000 100 200 9
# 한 칸만 늘리면 같은 유령이 못 피하는 쪽으로 넘어간다.
run_waste "수명보다 짧게 기다렸으면 안 센다" 0 "$work/s.log" "$work/exact.tsv" 1000 100 200 10

# 판이 기구를 안 잰 경우는 통과도 미달도 아니다.
: > "$work/empty.tsv"
# 걷은 것도 걷었어야 하는 것도 없으면 기구에 기회가 없던 판이다.
run_waste "기회가 없던 판은 미판정" 1 "$work/s.log" "$work/empty.tsv" 1000 0 200 "$TTL"
못_피하는 1 > "$work/alibi.tsv"
run_waste "알리바이 한 명은 정족수가 아니다" 1 "$work/s.log" "$work/alibi.tsv" 1000 0 200 "$TTL"
run_waste "차례가 적으면 미판정" 1 "$work/s.log" "$work/g30.tsv" 10 100 200 "$TTL"

# **못 되짚은 유령이 판을 덮으면 미판정.** 알리바이 하나만 두고 나머지가 전부
# 표본 밖이면, 스위퍼가 한 명도 안 걷은 판이 통과한다.
{ 못_피하는 20; waste_ghosts 500 900000000; } > "$work/unknown.tsv"
run_waste "못 되짚은 것이 많으면 미판정" 1 "$work/s.log" "$work/unknown.tsv" 1000 100 500 "$TTL"

# **과잉 청소.** 걷은 수가 이탈한 수를 넘으면 살아 있는 사람을 걷은 것이다.
run_waste "과잉 청소를 미달로 잡는다" 1 "$work/s.log" "$work/g30.tsv" 1000 300 200 "$TTL"

# **표본 밖을 아는 쪽으로 접으면 안 된다.** 상한 안쪽이라 미판정은 아니지만,
# 그것을 피할 수 있었던 것으로 세면 문턱을 넘는다.
{ 못_피하는 20; 피할_수_있었던 50; waste_ghosts 3 900000000; } > "$work/edge-unknown.tsv"
run_waste "표본 밖은 피할 수 있었던 것이 아니다" 0 \
    "$work/s.log" "$work/edge-unknown.tsv" 1000 100 200 "$TTL"

# **임계와 같은 score 는 그 표본에서 지나간 것이다.** 등호를 빼면 다음 표본이
# 채택되어 기다린 시간이 늘고, 못 피하는 것이 피할 수 있었던 것으로 넘어간다.
{ 못_피하는 20; waste_ghosts 51 60000000; } > "$work/edge-eq.tsv"
run_waste "임계와 같은 score 는 그 표본에서 지난다" 0 \
    "$work/s.log" "$work/edge-eq.tsv" 1000 100 200 "$TTL"

# **첫 표본에서 이미 지나가 있으면 언제 지났는지 모른다.** 아는 척하면 그
# 유령들이 못 피하는 것으로 세어져 판이 통과한다.
waste_ghosts 60 5000000 > "$work/edge-left.tsv"
run_waste "첫 표본 앞의 유령은 못 되짚는다" 1 \
    "$work/s.log" "$work/edge-left.tsv" 1000 100 200 "$TTL"

# **표본이 뒤로 가면 되짚을 수 없다.** 다른 가드가 아니라 단조성이 잡아야 한다 —
# 아래 입력은 그것을 빼면 통과로 나온다.
printf '50 10000000\n300 200000000\n100 60000000\n' > "$work/dip.log"
피할_수_있었던 20 > "$work/g20.tsv"
run_waste "표본이 중간에 뒤로 가면 미판정" 1 "$work/dip.log" "$work/g20.tsv" 1000 100 200 "$TTL"

# **시계가 뒤로 갔으면 score 가 시각이 아니다.** `enqueue.lua` 가 바닥값+1 을
# 쓰므로 되짚으면 기다린 시간이 짧게 나오고, 결함이 못 피하는 것으로 넘어간다.
# 추측하지 않고 게이트웨이 지표로 판정한다.
run_waste "시계가 뒤로 갔으면 미판정" 1 \
    "$work/s.log" "$work/g30.tsv" 1000 100 200 "$TTL" 3
run_waste "시계가 안 갔으면 그대로 판정한다" 0 \
    "$work/s.log" "$work/g30.tsv" 1000 100 200 "$TTL" 0

# 입력이 깨진 판은 통과로 안 센다.
: > "$work/nosample.log"
run_waste "표본이 비면 미판정" 1 "$work/nosample.log" "$work/g30.tsv" 1000 100 200 "$TTL"
run_waste "유령 파일이 없으면 미판정" 1 "$work/s.log" "$work/없는파일.tsv" 1000 100 200 "$TTL"
printf '300 200000000\n100 60000000\n' > "$work/back.log"
run_waste "임계가 뒤로 가면 미판정" 1 "$work/back.log" "$work/g30.tsv" 1000 100 200 "$TTL"
printf 'm1\tERR_WRONGTYPE\n' > "$work/badscore.tsv"
run_waste "score 가 숫자가 아니면 미판정" 1 "$work/s.log" "$work/badscore.tsv" 1000 100 200 "$TTL"
run_waste "수명이 0 이면 미판정" 1 "$work/s.log" "$work/g30.tsv" 1000 100 200 0
run_waste "차례를 준 인원이 숫자가 아니면 미판정" 1 \
    "$work/s.log" "$work/g30.tsv" 없음 100 200 "$TTL"

if [[ "$failed" == 0 ]]; then
  printf '\033[1m게이트 자기검사 통과\033[0m\n'
else
  printf '\033[31m게이트 자기검사 미달\033[0m\n'
fi
exit "$failed"

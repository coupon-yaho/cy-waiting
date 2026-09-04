#!/usr/bin/env bash
# 9.4.3 실측 — **한 대를 죽이면 사용자에게 오류가 새는가.**
#
# 여기서 재는 것은 비율이 아니라 **응답 코드**다. 죽은 대로 간 요청이 그대로
# 5xx 가 되거나 응답 없이 끝나면 사용자가 그 장애를 본다.
#
# **보고를 멈추는 것으로는 못 잰다.** 그러면 낡음 창이 지난 뒤 후보에서 조용히
# 빠지고, 그건 정상 이탈이라 재시도 경로를 아예 안 지난다. 컨테이너를 죽이고
# 보고는 계속 올리게 둬야 게이트웨이가 살아 있다고 믿고 보낸다.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1
# **여유를 크게 잡는다.** 작게 두면 일꾼들이 보내는 속도가 한 틱 몫을 넘겨
# 쿠폰이 줄 모드로 켜지고, 그때부터 요청은 뒷단에 안 닿는다 — 죽은 대로 가는
# 경로를 한 번도 안 밟은 실행이 나온다. 실제로 그랬다.
# **라이브러리를 읽기 전에 정한다** — 읽은 뒤에 정하면 무시된다.
BIG_CAP="${BIG_CAP:-2000}"
SMALL_CAP="${SMALL_CAP:-400}"
MID_CAP="${MID_CAP:-1200}"

. test/load/routing-lib.sh || exit 2

# 죽일 대. 가장 여유 있는 대를 죽여야 유출이 있으면 드러난다.
VICTIM="${VICTIM:-backend}"
BEFORE_SEC="${BEFORE_SEC:-4}"
# 죽인 뒤 이만큼 보낸다(초). 낡음 창이 지나 후보에서 빠질 때까지가 위험
# 구간이라 그보다 넉넉히 잡는다.
AFTER_SEC="${AFTER_SEC:-12}"

# 동시에 보내는 일꾼 수. 죽은 대로 간 요청은 시간 제한까지 매달릴 수 있어,
# 하나로는 창 안에 표본이 안 쌓인다 — 12초에 네 건을 보낸 적이 있다.
WORKERS="${WORKERS:-12}"

# 죽이기 전 희생양에 이만큼은 가 있어야 한다. 안 가고 있었으면 죽여도 아무
# 경로를 안 밟는다.
MIN_VICTIM="${MIN_VICTIM:-20}"

require_positive_int BEFORE_SEC AFTER_SEC WORKERS || exit 2
require_non_negative_int MIN_VICTIM || exit 2

work=$(mktemp -d) || exit 1
# 죽인 대를 반드시 되살린다. 안 살리면 다음 실행이 두 대로 시작한다.
trap '$COMPOSE start "$VICTIM" >/dev/null 2>&1; rm -rf "$work"' EXIT

echo "$(banner) · 일꾼 ${WORKERS} · ${VICTIM} 을 죽이고 ${AFTER_SEC}초 동안 응답을 센다"

bring_up "$work/up.log" || exit 2
wait_for_ramp || exit 2
wait_for_idle_queue || exit 2

drive() {
  local out=$1 seconds=$2 base=$3 slot deadline
  deadline=$(( $(date +%s) + seconds ))
  for slot in $(seq 1 "$WORKERS"); do
    (
      k=0
      while [ "$(date +%s)" -lt "$deadline" ]; do
        k=$(( k + 1 ))
        issue "$(( base + k * WORKERS + slot ))"
      done
    ) &
  done > "$out"
  wait
}

# **죽일 대가 원래 트래픽을 받고 있었는지 본다.** 그 대가 후보에서 이미
# 빠져 있으면 죽여도 유출이 0 이고, 나머지 둘이 최소 도착 수를 채워 "충족" 이
# 난다 — 죽은 대로 가는 경로를 한 번도 안 밟은 실행이다. 판정기의 최소 도착
# 수는 **세 대 합계**라 이것을 못 가른다.
victim_before=$(served "$VICTIM") || exit 2
drive "$work/before" "$BEFORE_SEC" 5000000
victim_after=$(served "$VICTIM") || exit 2
victim_delta=$(( victim_after - victim_before ))

echo
echo "  죽이기 전: $(sort "$work/before" | uniq -c | tr -s ' \n' ' ')"
echo "  죽이기 전 ${VICTIM} 도착: ${victim_delta} 건"

if [ "$victim_delta" -lt "$MIN_VICTIM" ]; then
  echo
  echo "판정 불가 — 죽이기 전 ${VICTIM} 에 ${victim_delta} 건만 갔다. 이 대는 원래 트래픽을 안 받는다"
  exit 2
fi

# **죽이기 전에 이미 새고 있으면 인과가 안 선다.** 죽인 뒤의 유출을 죽음
# 때문이라고 적으려면 그 전이 깨끗해야 한다.
#
# 하한 둘을 낮춰 부른다. 여기서 보려는 것은 유출뿐인데, 표본·도착 하한이 걸리면
# 판정기가 "판정 불가" 를 내고 그것을 유출로 잘못 읽게 된다.
MIN_SAMPLE=1 MIN_ADMITTED=1 test/load/evaluate-kill.sh "$work/before" \
  > "$work/before.verdict" 2>&1
before_rc=$?
if [ "$before_rc" -ne 0 ]; then
  echo
  if [ "$before_rc" -eq 1 ]; then
    echo "판정 불가 — 죽이기 전부터 유출이 있다. 죽인 뒤의 유출을 죽음 탓으로 못 돌린다"
  else
    echo "판정 불가 — 죽이기 전 표본을 못 읽었다"
  fi
  sed 's/^/  /' "$work/before.verdict"
  exit 2
fi

$COMPOSE kill "$VICTIM" >/dev/null 2>&1 || { echo "$VICTIM 을 못 죽였다"; exit 2; }
drive "$work/after" "$AFTER_SEC" 6000000

echo
echo "  죽인 뒤: $(sort "$work/after" | uniq -c | tr -s ' \n' ' ')"
echo
test/load/evaluate-kill.sh "$work/after"

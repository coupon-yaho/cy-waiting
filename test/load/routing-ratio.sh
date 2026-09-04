#!/usr/bin/env bash
# 9.4.5 실측 — **고르개가 정한 비율이 실제 유입으로 나오는가.**
#
# 여유가 다른 뒷단 셋을 세우고 같은 부하를 넣은 뒤, 각 대에 **실제로 도착한
# 건수**를 센다. 기대는 여유 비율 그대로다. 고르개 단위 시험은 고른 결과만
# 보므로, 그 사이에 있는 배선·램프·상한·재시도가 비율을 어떻게 바꾸는지는
# 여기서만 드러난다.
#
# **도착은 뒷단이 센 값을 쓴다.** 게이트웨이 쪽 지표를 쓰면 고르개가 고른
# 것을 다시 읽는 셈이라 배선이 틀려도 맞게 나온다.
#
# **재시작하지 않고 차분으로 센다.** 부하 직전에 뒷단을 재시작하면 그 순간의
# 실패로 램프가 내려앉고, 회복하기 전에 부하가 들어가 표본이 몇 건만 남는다.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1
# 여유 200/40/120. **배수가 아니라 나머지가 남는 값**이라 절삭 보정 갈래를 밟는다.
# **라이브러리를 읽기 전에 정한다** — 읽은 뒤에 정하면 무시된다.
BIG_CAP="${BIG_CAP:-200}"
SMALL_CAP="${SMALL_CAP:-40}"
MID_CAP="${MID_CAP:-120}"

. test/load/routing-lib.sh || exit 2

REQUESTS="${REQUESTS:-600}"

# **동시성이 판정의 일부다.** 한 건씩 보내면 물린 건수가 늘 0 이고, 그러면
# 여유 대비 부하로 고르는 전략은 비교할 것이 없어 균등 무작위가 된다 —
# 설계가 리틀의 법칙을 근거로 드는 것이 정확히 이 지점이다. 그 조건에서 잰
# 값으로 "여유를 안 본다" 고 적으면 안 되는 것을 잰 것이다.
#
# 반대로 유입이 한 틱 몫(여유 합계)을 넘으면 쿠폰이 줄 모드로 켜져 요청이
# 뒷단에 아예 안 닿는다. 물린 건수 = 유입 × 지연이므로, 지연을 올리면 유입을
# 안 올리고도 물린다.
CONCURRENCY="${CONCURRENCY:-1}"

# 일꾼이 한 건 보낸 뒤 쉬는 시간(초). 유입 속도를 낮추는 손잡이다.
PACE_SEC="${PACE_SEC:-0}"

# 일꾼 사이의 출발 간격(밀리초). 0 이면 다 같이 출발한다.
#
# **깊이를 재는 회차는 이 값을 줘야 한다.** 동시성이 크면 첫 순간의 유입이
# 예산을 통째로 넘겨 줄 모드가 켜지고, 그 뒤로는 요청이 뒷단에 안 닿는다.
# 뒷단 지연을 동시성으로 나눈 값이 대략 고르게 퍼지는 지점이다.
STAGGER_MS="${STAGGER_MS:-0}"

# 허용 편차(%). 비워 두면 판정기의 기본값(게이트와 같은 ±15%)이 선다 —
# 여기에 숫자를 또 적으면 게이트와 갈라질 자리가 하나 더 생긴다.
MAX_DEVIATION="${MAX_DEVIATION:-}"

# 램프가 오른 뒤 배분이 한두 틱 더 돌 틈.
WARMUP_SEC="${WARMUP_SEC:-15}"

require_positive_int REQUESTS CONCURRENCY || exit 2
require_non_negative_int STUB_LATENCY_MS WARMUP_SEC PACE_SEC STAGGER_MS || exit 2

# **얕은 부하로 P2C 를 재지 않는다.** 물린 건수가 이보다 얕으면 세 대의 부하율
# 차이가 작아 여유가 비교에서 거의 빠지고, 그 실행은 고르개가 아니라 뽑기를
# 잰다. 실제로 그 구간에서 재고 "P2C 가 여유를 안 본다" 고 기록한 적이 있다.
#
# 이 값과 근거는 계획서 2.2 에 있다. 여기 다시 적으면 사본이 하나 더 생기고,
# 실제로 그 사본들 사이에서 320 과 400 이 갈린 적이 있다.
#
# **이 하한을 낮춰서 재지 않는다.** 낮추면 다음 사람이 같은 것을 재고 같은
# 결론을 낸다. 얕은 구간을 보고 싶으면 라운드로빈으로 잰다.
MIN_P2C_CONCURRENCY="${MIN_P2C_CONCURRENCY:-400}"
case "$STRATEGY" in
    *p2c*)
        if [ "$CONCURRENCY" -lt "$MIN_P2C_CONCURRENCY" ]; then
            echo "판정 불가 — 전략 ${STRATEGY} 를 동시 ${CONCURRENCY} 로 재면 이 기준의 적용 범위 밖이다"
            echo "  물린 건수가 ${MIN_P2C_CONCURRENCY} 이상이어야 여유가 비교에 들어온다"
            exit 2
        fi ;;
esac

work=$(mktemp -d) || exit 1
trap 'reap_children; rm -rf "$work"' EXIT

echo "$(banner) · 동시 ${CONCURRENCY} · 부하 ${REQUESTS} 건"

bring_up "$work/up.log" || exit 2
wait_for_ramp || exit 2
sleep "$WARMUP_SEC"
wait_for_idle_queue || exit 2

CREDITS=("$BIG_CAP" "$SMALL_CAP" "$MID_CAP")
for name in "${NAMES[@]}"; do served "$name" || exit 2; done > "$work/before"

base=$(( 1000000 + (RANDOM % 8000) * 1000 ))

# **일꾼을 따로 돌린다.** 묶음마다 다 끝나기를 기다리면 그 경계에서 물린
# 건수가 0 으로 떨어지고, 그 순간의 결정은 여유를 못 본다 — 재려던 조건이
# 회차마다 무너진다. 일꾼 각자가 제 차례를 이어 보내야 물린 건수가 동시성
# 근처에서 유지된다.
worker() {
  local slot=$1 n=$2 k i
  # **출발을 흩는다.** 일꾼이 다 같이 첫 요청을 쏘면 그 순간 유입이 한산 통과
  # 예산을 넘겨 줄 모드가 켜지고, 한 번 켜지면 줄이 빌 때까지 안 꺼진다 —
  # 그 회차는 분배가 아니라 줄을 잰다. 실제로 두 번 그렇게 돌았다.
  [ "$STAGGER_MS" = 0 ] || sleep "$(awk -v s="$slot" -v m="$STAGGER_MS" \
      'BEGIN { printf "%.4f", s * m / 1000 }')"
  for k in $(seq 1 "$n"); do
    i=$(( (k - 1) * CONCURRENCY + slot ))
    issue "$(( base + i ))"
    [ "$PACE_SEC" = 0 ] || sleep "$PACE_SEC"
  done
}

# **주문한 만큼 정확히 보낸다.** 절삭한 몫을 그대로 쓰면 `REQUESTS=600
# CONCURRENCY=7` 에서 595 건만 나가고, 그 595 를 기준으로 판정이 성립한다 —
# "넣은 부하가 다 닿았는가" 라는 가드가 그만큼 헐거워진다. 나머지는 앞쪽
# 일꾼들이 한 건씩 더 가져간다.
per_worker=$(( REQUESTS / CONCURRENCY ))
remainder=$(( REQUESTS % CONCURRENCY ))
for slot in $(seq 1 "$CONCURRENCY"); do
  n=$per_worker
  [ "$slot" -le "$remainder" ] && n=$(( n + 1 ))
  [ "$n" -gt 0 ] || continue
  worker "$slot" "$n" &
done > "$work/codes"
wait

for name in "${NAMES[@]}"; do served "$name" || exit 2; done > "$work/after"

total=0
specs=()
for idx in 0 1 2; do
  b=$(sed -n "$((idx + 1))p" "$work/before")
  a=$(sed -n "$((idx + 1))p" "$work/after")
  arrived=$(( a - b ))
  total=$(( total + arrived ))
  specs+=("${NAMES[idx]}:${CREDITS[idx]}:$arrived")
done

sent=$(grep -c '' "$work/codes")
echo
echo "응답 코드: $(sort "$work/codes" | uniq -c | tr -s ' \n' ' ')"
echo "도착 합계: $total (보낸 것 $sent / 주문 $REQUESTS)"
echo

# 보낸 것이 주문과 다르면 하네스가 고장 난 것이다. 그 실행의 비율은 무엇을
# 잰 것인지 알 수 없다.
if [ "$sent" -ne "$REQUESTS" ]; then
  echo "판정 불가 — 주문 ${REQUESTS} 건 중 ${sent} 건만 나갔다. 하네스가 고장 났다"
  exit 2
fi

# **다 통과하지 못했으면 비율을 논하지 않는다.** 일부만 닿아도 그 일부의
# 비율은 맞을 수 있어서, 못 잰 실행이 충족으로 적힌다.
bad=$(grep -cv '^200$' "$work/codes")
if [ "$bad" -ne 0 ]; then
  echo "판정 불가 — 보낸 것 중 $bad 건이 200 이 아니다. 이 실행으로는 분배를 못 잰다"
  exit 2
fi

MAX_DEVIATION="$MAX_DEVIATION" EXPECTED_TOTAL="$REQUESTS" \
  test/load/evaluate-routing-ratio.sh "${specs[@]}"

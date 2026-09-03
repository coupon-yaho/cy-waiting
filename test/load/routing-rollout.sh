#!/usr/bin/env bash
# 9.4.4 실측 — **새로 뜬 인스턴스로 처음부터 몰리지는 않는가.**
#
# 롤링 배포에서 새 인스턴스는 차갑다. 캐시도 커넥션 풀도 비어 있어서, 제 몫을
# 처음부터 받으면 그 대만 무너진다. 램프가 그것을 막는다.
#
# **식별자를 바꾸는 것이 배포다.** 재기동한 인스턴스는 새 식별자로 온다는 것이
# 계약이고, 램프는 처음 본 식별자에만 걸린다. 컨테이너를 재시작해도 씨더가
# 같은 이름으로 보고하면 램프를 안 타서 아무것도 안 잰다.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1
# **여유를 크게 잡는다.** 부하가 끊기지 않아야 초당 도착으로 램프를 볼 수 있다.
# **라이브러리를 읽기 전에 정한다** — 읽은 뒤에 정하면 무시된다.
BIG_CAP="${BIG_CAP:-2000}"
SMALL_CAP="${SMALL_CAP:-400}"
MID_CAP="${MID_CAP:-1200}"

. test/load/routing-lib.sh || exit 2

BEFORE_SEC="${BEFORE_SEC:-4}"
AFTER_SEC="${AFTER_SEC:-20}"
# 새로 뜬 것처럼 보이게 할 식별자. 앞 실행과 겹치면 램프를 안 탄다.
FRESH_ID="${FRESH_ID:-stub-1-$$}"

require_positive_int BEFORE_SEC AFTER_SEC || exit 2

# **P2C 는 이 하네스로 못 잰다.** 아래에서 부하를 한 건씩 흘리므로 물린 건수가
# 0 에 가깝고, 그러면 여유가 비교에서 빠져 램프가 크레딧을 깎아도 몫이 안
# 움직인다 — 설계대로 도는 구현이 "램프가 아니라 배제다" 로 적힌다.
case "$STRATEGY" in
    *p2c*)
        echo "판정 불가 — 전략 ${STRATEGY} 는 이 하네스로 못 잰다"
        echo "  부하를 한 건씩 흘려 물린 건수가 0 에 가깝다. 이 기준의 적용 범위 밖이다"
        exit 2 ;;
esac

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"; kill %1 2>/dev/null' EXIT

echo "$(banner) · 큰 대를 ${FRESH_ID} 로 갈아 끼운다"

bring_up "$work/up.log" || exit 2
wait_for_ramp || exit 2
wait_for_idle_queue || exit 2

( i=0; while :; do i=$(( i + 1 )); issue "$(( 7000000 + i ))" >/dev/null; done ) &

: > "$work/samples"
for name in "${NAMES[@]}"; do served "$name" || exit 2; done > "$work/prev"

# **라벨은 실제 경과 초다.** 순번을 그대로 쓰면 표본 하나를 뜨는 데 드는 시간
# (뒷단 셋에 각각 도커 명령)이 라벨에서 빠져, 예산 5초로 적힌 결과가 벽시계로는
# 그보다 한참 뒤가 된다 — 게이트가 요구하는 수가 조용히 늘어난다.
sample() {
  local second=$1 total=0 fresh=0 idx now prev delta
  for idx in 0 1 2; do
    now=$(served "${NAMES[idx]}") || return 1
    echo "$now" >> "$work/cur"
  done
  for idx in 0 1 2; do
    prev=$(sed -n "$((idx + 1))p" "$work/prev")
    now=$(sed -n "$((idx + 1))p" "$work/cur")
    delta=$(( now - prev ))
    [ "$idx" -eq 0 ] && fresh=$delta
    total=$(( total + delta ))
  done
  mv "$work/cur" "$work/prev"
  [ -z "${changed_at:-}" ] || second=$(( $(date +%s) - changed_at ))
  echo "$second $fresh $total" >> "$work/samples"
}

for s in $(seq -- "-$BEFORE_SEC" -1); do sleep 1; sample "$s" || exit 2; done

# **갈아 끼운다.** 주소는 그대로고 식별자만 바뀐다 — 같은 대가 새로 뜬 모양이다.
$COMPOSE exec -T redis redis-cli SET sim:id:stub-1 "$FRESH_ID" >/dev/null
changed_at=$(date +%s)
for s in $(seq 0 "$AFTER_SEC"); do sleep 1; sample "$s" || exit 2; done

kill %1 2>/dev/null; wait %1 2>/dev/null

steady=$(( BIG_CAP * 100 / (BIG_CAP + SMALL_CAP + MID_CAP) ))
echo
echo "큰 대의 평상시 몫: ${steady}%"
echo
test/load/evaluate-rollout.sh "$work/samples" "$steady"

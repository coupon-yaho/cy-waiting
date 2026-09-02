#!/usr/bin/env bash
# R1 실측 (6.7). **한산한 쿠폰과 경합 쿠폰을 같이 돌린다.**
#
# 둘을 따로 돌리면 아무것도 증명하지 못한다. 한산 쪽만 보면 대기열을 통째로
# 꺼도 통과하고, 경합 쪽만 보면 격리를 안 잰다. R1 은 "다른 쿠폰이 몰릴 때에도
# 한산한 쿠폰은 줄 없이 지나간다" 라서, 같은 시각에 둘 다 봐야 성립한다.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1

# **예측 가능한 경로에 안 쓴다.** 남이 미리 심볼릭 링크를 걸어 두면 리디렉션이
# 엉뚱한 파일을 덮어쓴다. 끝나면 지운다.
work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

COMPOSE="docker compose -f test/load/compose.yml"
export REDIS_CLI="$COMPOSE exec -T redis redis-cli"
BASE_URL="${BASE_URL:-http://localhost:18080}"
export BASE_URL

# **줄 키를 비우고 시작한다.** 앞선 실행이 남긴 원소가 있으면 이번 판정이
# 지난 실행을 잰다.
$REDIS_CLI DEL 'queue:{c1}' 'queue:{c2}' >/dev/null 2>&1

echo "== 경합 쿠폰(c2)에 부하를 넣는 동안 한산 쿠폰(c1)을 잰다 =="
k6 run --quiet --summary-export=$work/contended.json \
    test/load/contended-coupon.js >$work/contended.log 2>&1 &
hot=$!

# 램프가 올라간 뒤에 재기 시작한다. 겹치기 전에 재면 격리를 안 잰 것이다.
sleep 4
k6 run --quiet --summary-export=$work/idle.json \
    test/load/idle-coupon.js >$work/idle.log 2>&1
idle_run=$?
wait "$hot"

failed=0
echo
echo "-- 경합 쿠폰 --"
test/load/evaluate-gate.sh contended-coupon $work/contended.json || failed=1
echo
echo "-- 한산 쿠폰 (경합 중) --"
test/load/evaluate-gate.sh idle-coupon $work/idle.json || failed=1

# k6 자체가 못 돈 것을 통과로 세지 않는다.
if [[ $idle_run -ne 0 ]]; then
    echo "::error title=R1 실측::한산 시나리오가 임계를 넘겼다"
    tail -20 "$work/idle.log" >&2
    failed=1
fi

echo
if [[ $failed -eq 0 ]]; then
    echo "R1 통과 — 다른 쿠폰이 몰리는 동안에도 한산한 쿠폰은 줄 없이 지나갔다"
else
    echo "R1 미달"
fi
exit $failed

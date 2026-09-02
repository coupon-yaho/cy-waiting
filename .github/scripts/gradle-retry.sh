#!/usr/bin/env bash
# 그레이들을 돌리되 **일시적 의존성 해결 실패에만** 다시 시도한다.
#
# 메이븐 센트럴이 429 를 내면 그레이들은 그 저장소를 **판 전체에서 비활성화한다**
# (`Repository MavenRepo is disabled due to earlier error`). 그러면 뒤따르는 모든
# 해결이 같이 실패해 잡이 통째로 죽는다 — 캐시에 없던 산출물 하나가 잡 하나를
# 무너뜨린다. 실제로 커버리지 잡이 jacoco 에이전트를 받다 그렇게 죽었다.
#
# **시험 실패에는 재시도를 안 건다.** 다시 돌린다고 빨간 것이 초록이 되지 않고,
# 되면 그게 더 나쁘다 — 불안정한 시험을 재시도로 덮는 것이 된다 (TS-7).
# 그래서 로그에 해결 실패의 자국이 있을 때만 다시 돈다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

# 세 번이면 대개 넘어간다. 늘리면 정말 죽은 저장소에서 잡 상한을 다 태운다.
attempts=${GRADLE_RETRY_ATTEMPTS:-3}
log=$(mktemp)
trap 'rm -f "$log"' EXIT

rc=0
for ((i = 1; i <= attempts; i++)); do
    ./gradlew --no-daemon "$@" 2>&1 | tee "$log"
    rc=${PIPESTATUS[0]}
    if [ "$rc" -eq 0 ]; then
        exit 0
    fi
    # **자국이 없으면 그대로 실패시킨다.** 여기가 넓어지면 재시도가 시험 실패를
    # 삼키기 시작한다.
    if ! grep -qE "Could not resolve|Could not get resource|is disabled due to earlier error|Too Many Requests" "$log"; then
        exit "$rc"
    fi
    if [ "$i" -lt "$attempts" ]; then
        echo "::warning::의존성 해결이 실패했다 (${i}/${attempts}) — 잠시 뒤 다시 시도한다"
        sleep $((i * 15))
    fi
done
echo "::error::의존성 해결이 ${attempts}회 연속 실패했다 — 저장소가 살아 있는지 본다"
exit "$rc"

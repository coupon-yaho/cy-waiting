#!/usr/bin/env bash
# 그레이들을 돌리되 **일시적 의존성 해결 실패에만** 다시 시도한다.
#
# 메이븐 센트럴이 429 를 내면 그레이들은 그 저장소를 **실행 전체에서 비활성화한다**
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
#
# **0 이나 쓰레기가 들어오면 되돌린다.** 안 그러면 루프가 한 번도 안 돌고 rc 가 0 인
# 채로 끝난다 — 그레이들을 부르지 않고 잡이 초록이 된다. 재시도 장치가 스스로
# 거짓 초록을 내는 것이 가장 나쁜 종류다.
#
# **모양부터 본다.** 산술로 먼저 읽으면 `08` 이 8진수로, `abc` 가 0 으로 새어
# 들어와 루프가 한 번도 안 돌고 rc 가 0 인 채로 끝난다 — 그레이들을 안 부르고
# 잡이 초록이 된다. 자릿수만 있는 값인지 보고, 그다음 10진으로 못 박는다.
#
# **위쪽도 막는다.** 자릿수만 보면 상한이 없어, 긴 값이 산술에서 넘쳐 거대한
# 횟수가 되거나 그대로 커서 일시적 실패에 잡 상한을 다 태운다. 자리 수부터 자른다.
attempts=${GRADLE_RETRY_ATTEMPTS:-3}
case "$attempts" in
    ''|*[!0-9]*|??*[0-9]) attempts=3 ;;
    *) attempts=$((10#$attempts)); [ "$attempts" -ge 1 ] && [ "$attempts" -le 10 ] || attempts=3 ;;
esac
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
    #
    # **`Could not resolve` 만 보면 안 된다.** 스프링의
    # `Could not resolve placeholder` 와 잭슨의 `Could not resolve type id` 가 같은
    # 문구로 난다 — 하필 이 저장소가 가장 무서워하는 조용한 배선 실패다. 그것이
    # 여기 걸리면 결정적인 설정 결함이 "저장소가 죽었다" 로 보고되고 당직이
    # 엉뚱한 데를 본다. 그레이들의 해결 실패 형태로 좁힌다.
    #
    # **도커 허브도 같이 본다.** 통합·카오스가 컨테이너를 받는데 그쪽 429 는
    # 소문자 `toomanyrequests` 라 위 패턴에 하나도 안 걸린다. 네트워크를 제일
    # 많이 쓰는 두 계층이 정작 무방비였다.
    if ! grep -qE "Could not resolve (all|[a-zA-Z0-9_.-]+:)|Could not (GET|HEAD) 'http|toomanyrequests|pull rate limit" "$log"; then
        exit "$rc"
    fi
    if [ "$i" -lt "$attempts" ]; then
        echo "::warning::의존성 해결이 실패했다 (${i}/${attempts}) — 잠시 뒤 다시 시도한다"
        sleep $(( ${RETRY_SLEEP:-15} * i ))
    fi
done
echo "::error::의존성 해결이 ${attempts}회 연속 실패했다 — 저장소가 살아 있는지 본다"
exit "$rc"

#!/usr/bin/env bash
# 재시도 스크립트의 자기검증 (TS-9).
#
# **장애를 못 넣는 하네스는 다 통과시킨다.** 이 재시도가 무엇을 삼키고 무엇을
# 그대로 실패시키는지는 정규식 한 줄에 걸려 있는데, 그 줄은 다음 사람이 넓히기
# 쉽다. 넓히는 순간 시험 실패가 재시도로 덮이기 시작한다 (TS-7).
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

retry=$PWD/.github/scripts/gradle-retry.sh
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cd "$work" || exit 1
git init -q .

# 회차마다 다른 로그를 내는 가짜 그레이들. n 번째 실행에서 무엇을 낼지 MODE 가 정한다.
cat > gradlew <<'STUB'
#!/usr/bin/env bash
n=$(cat .n 2>/dev/null || echo 0); n=$((n + 1)); echo "$n" > .n
case "$MODE" in
  maven)  [ "$n" -lt 3 ] && { echo "> Could not resolve org.jacoco:org.jacoco.agent:0.8.15."; exit 1; } ;;
  docker) [ "$n" -lt 3 ] && { echo "toomanyrequests: You have reached your pull rate limit."; exit 1; } ;;
  test)   echo "FooTest > 어떤_시험 FAILED"; exit 1 ;;
  wiring) echo "Could not resolve placeholder 'waiting.x' in value \"\${waiting.x}\""; exit 1 ;;
esac
echo "BUILD SUCCESSFUL"
STUB
chmod +x gradlew

fail=0
# **셸 지역변수는 영문이어야 한다.** bash 의 local 이 한글 식별자를 안 받는다.
check() {
    local name=$1 mode=$2 want_rc=$3 want_rounds=$4
    rm -f .n
    MODE="$mode" GRADLE_RETRY_ATTEMPTS=3 RETRY_SLEEP=0 "$retry" test >/dev/null 2>&1
    local rc=$? rounds
    rounds=$(cat .n 2>/dev/null || echo 0)
    if [ "$rc" -eq "$want_rc" ] && [ "$rounds" -eq "$want_rounds" ]; then
        echo "  ✓ $name"
    else
        echo "  ✗ $name — 종료 $rc (기대 $want_rc) · $rounds 실행 (기대 $want_rounds)"
        fail=1
    fi
}

echo "재시도가 무는 것"
check "메이븐 해결 실패는 다시 시도한다" maven 0 3
check "도커 허브 429 도 다시 시도한다" docker 0 3

echo "재시도가 안 무는 것"
check "시험 실패는 한 번에 끝낸다" test 1 1
check "배선 실패를 해결 실패로 안 읽는다" wiring 1 1

# **망가진 시도 횟수로도 그레이들은 돈다.** 안 돌면 잡이 초록인데 아무것도 안 한
# 것이다. `08` 은 정수처럼 보이지만 산술에서 8진수로 읽혀 터진다.
for bad in 0 -1 08 abc "" 999999999999999999999 11; do
    rm -f .n
    MODE=test GRADLE_RETRY_ATTEMPTS="$bad" "$retry" test >/dev/null 2>&1
    rc=$?
    rounds=$(cat .n 2>/dev/null || echo 0)
    if [ "$rc" -ne 0 ] && [ "$rounds" -ge 1 ]; then
        echo "  ✓ 시도 횟수가 '$bad' 여도 한 번은 돌고 실패로 끝난다"
    else
        echo "  ✗ 시도 횟수가 '$bad' 이면 그레이들을 안 부르고 종료 $rc 다"
        fail=1
    fi
done

# **망가진 값이 기본 셋으로 돌아오는지도 본다.** 위 검사는 "한 번은 돈다" 까지만
# 보므로, 거대한 값이 그대로 커진 채여도 통과한다 — 그러면 일시적 실패 하나가
# 잡 상한을 다 태운다.
for bad in 999999999999999999999 11 abc; do
    rm -f .n
    MODE=maven GRADLE_RETRY_ATTEMPTS="$bad" RETRY_SLEEP=0 "$retry" test >/dev/null 2>&1
    rounds=$(cat .n 2>/dev/null || echo 0)
    if [ "$rounds" -eq 3 ]; then
        echo "  ✓ 시도 횟수가 '$bad' 이면 기본 셋으로 돌아온다"
    else
        echo "  ✗ 시도 횟수가 '$bad' 인데 $rounds 실행 돌았다 — 셋이어야 한다"
        fail=1
    fi
done

[ "$fail" -eq 0 ] && echo "자기검증 통과" || echo "::error::재시도 자기검증 실패"
exit $fail

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

# 판마다 다른 로그를 내는 가짜 그레이들. n 번째 판에서 무엇을 낼지 MODE 가 정한다.
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
        echo "  ✗ $name — 종료 $rc (기대 $want_rc) · $rounds 판 (기대 $want_rounds)"
        fail=1
    fi
}

echo "재시도가 무는 것"
check "메이븐 해결 실패는 다시 시도한다" maven 0 3
check "도커 허브 429 도 다시 시도한다" docker 0 3

echo "재시도가 안 무는 것"
check "시험 실패는 한 번에 끝낸다" test 1 1
check "배선 실패를 해결 실패로 안 읽는다" wiring 1 1

# **0 을 넣어도 그레이들은 돈다.** 안 돌면 잡이 초록인데 아무것도 안 한 것이다.
rm -f .n
MODE=test GRADLE_RETRY_ATTEMPTS=0 "$retry" test >/dev/null 2>&1
rc=$?
if [ "$rc" -ne 0 ] && [ "$(cat .n)" -ge 1 ]; then
    echo "  ✓ 시도 횟수가 0 이어도 한 번은 돈다"
else
    echo "  ✗ 시도 횟수가 0 이면 그레이들을 안 부르고 초록이 된다"
    fail=1
fi

[ "$fail" -eq 0 ] && echo "자기검증 통과" || echo "::error::재시도 자기검증 실패"
exit $fail

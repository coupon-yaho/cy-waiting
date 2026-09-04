#!/usr/bin/env bash
# 물린 표가 부하 뒤 제자리로 돌아오는지 판정한다 (G9.3 · R-8).
#
# 종료 0 충족 · 1 미달 · 2 판정 불가.
#
#   사용: TTL_SEC=30 [ZERO_BUDGET_SEC=5] evaluate-leak.sh <표본파일>
#         표본은 한 줄이 `<부하 끝난 뒤 경과 밀리초> <물린 수>` 다.
#         부하 중의 표본은 경과를 음수로 적는다.
#
# **수명이 대신 걷어 주는 것은 회수가 아니다.** 표를 놓는 자리가 빠져도 수명이
# 지나면 0 이 되므로, "언젠가 0" 만 보면 누수가 있는 구현도 초록이다. 놓는
# 자리가 도는지는 **수명보다 훨씬 빨리** 0 이 되는 것으로만 갈린다.
set -uo pipefail

SAMPLES="${1:?표본 파일이 필요하다}"
TTL_SEC="${TTL_SEC:?수명을 알아야 판정한다}"
ZERO_BUDGET_SEC="${ZERO_BUDGET_SEC:-5}"
# **얕은 회차는 누수를 못 잰다.** 한 건이 물렸다 빠진 것으로 "누수 0" 을 적으면
# 부하가 뒷단에 거의 안 닿은 회차가 게이트를 통과한다. G9.1 이 든 부하 조건과
# 같은 자리다.
MIN_PEAK="${MIN_PEAK:-400}"

for setting in TTL_SEC ZERO_BUDGET_SEC MIN_PEAK; do
    value=$(eval "printf '%s' \"\$$setting\"")
    case "$value" in
        ''|*[!0-9]*) echo "$setting 은 0 이상의 정수여야 한다: '$value'"; exit 2 ;;
    esac
done
[ "$TTL_SEC" -gt "$ZERO_BUDGET_SEC" ] || {
    echo "판정 불가 — 예산 ${ZERO_BUDGET_SEC}초가 수명 ${TTL_SEC}초 이상이면 둘을 못 가른다"; exit 2; }
[ -s "$SAMPLES" ] || { echo "판정 불가 — 표본 파일이 비었다: $SAMPLES"; exit 2; }

TTL_SEC="$TTL_SEC" ZERO_BUDGET_SEC="$ZERO_BUDGET_SEC" MIN_PEAK="$MIN_PEAK" python3 - "$SAMPLES" <<'PY'
import os, sys

ttl = int(os.environ["TTL_SEC"]) * 1000
budget = int(os.environ["ZERO_BUDGET_SEC"]) * 1000

during, after = [], []
for line in open(sys.argv[1]):
    parts = line.split()
    if not parts:
        continue
    if len(parts) != 2:
        print("판정 불가 — 표본은 `<경과 밀리초> <물린 수>` 두 칸이다: %r" % line.strip())
        sys.exit(2)
    try:
        elapsed, value = int(parts[0]), int(parts[1])
    except ValueError:
        print("판정 불가 — 표본에 숫자가 아닌 것이 있다: %r" % line.strip())
        sys.exit(2)
    (during if elapsed < 0 else after).append((elapsed, value))

if not during:
    print("판정 불가 — 부하 중 표본이 없다. 무엇이 올라갔다 내려왔는지 모른다")
    sys.exit(2)
if not after:
    print("판정 불가 — 부하 뒤 표본이 없다. 회수를 못 봤다")
    sys.exit(2)

peak = max(v for _, v in during)
print("부하 중 — 표본 %d 개 · 최대 %d 건" % (len(during), peak))
# **안 올라갔으면 잰 것이 없다.** 0 에서 0 으로 끝난 회차를 "누수 없음" 으로
# 적으면, 부하가 뒷단에 안 닿은 회차가 게이트를 통과한다.
if peak == 0:
    print("판정 불가 — 부하 중에도 0 이었다. 요청이 균형기를 안 탔다")
    sys.exit(2)
min_peak = int(os.environ["MIN_PEAK"])
if peak < min_peak:
    print("판정 불가 — 최대가 %d 건뿐이다 (최소 %d). 이 깊이로는 누수를 못 잰다"
          % (peak, min_peak))
    sys.exit(2)

# **음수는 회수가 아니라 이중 감소다** (R-8). 표를 두 번 놓으면 그 대가 영영
# 가장 한가해 보여 전량을 받는다 — 0 만 보면 이 갈래가 안 보인다.
lowest = min(v for _, v in during + after)
if lowest < 0:
    print("미달 — 물린 수가 %d 로 음수다. 표를 두 번 놓는 자리가 있다" % lowest)
    sys.exit(1)

zero_at = next((e for e, v in sorted(after) if v == 0), None)
last = max(e for e, _ in after)
if zero_at is None:
    print("미달 — %.1f초가 지나도 %d 건이 남아 있다. 놓는 자리가 안 돈다"
          % (last / 1000, sorted(after)[-1][1]))
    sys.exit(1)

print("부하 뒤 — %.1f초 만에 0 이 됐다 (예산 %d초 · 수명 %d초)"
      % (zero_at / 1000, budget // 1000, ttl // 1000))
if zero_at > budget:
    print("미달 — 예산 안에 못 돌아온다. 수명이 대신 걷어 준 것과 구분이 안 된다")
    sys.exit(1)
print("충족 — 수명을 기다리지 않고 제자리로 돌아온다")
PY

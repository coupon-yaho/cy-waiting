#!/usr/bin/env bash
# 게이트웨이 여러 대의 쏠림 판정 (9.4.5 · R-4).
#
# **집계 비율만 보면 쏠림이 안 보인다.** 게이트웨이 둘이 같은 순서로 같은 대를
# 고르면 각자는 여유대로 나눈 것이라 합계는 맞는다. 깨지는 것은 같은 순간의
# 동시성이다 — 그 순간 대부분이 한 대에 몰리고, 그 대가 제 여유를 넘긴다.
# 그래서 도착 합계와 **순간 점유**를 따로 잰다.
set -uo pipefail

SAMPLES="${SAMPLES:?표본 파일이 필요하다}"
MAX_DEVIATION="${MAX_DEVIATION:-15}"
# 순간 점유의 문턱. **기본은 안 문다** — 이 값은 실측으로 정할 것이고, 재기도
# 전에 숫자를 박으면 게이트가 근거 없이 서는 셈이다.
MAX_SKEW="${MAX_SKEW:-}"
# 표본이 얕으면 점유가 한두 건에 흔들린다. 여유 합의 이 비율 아래는 버린다.
MIN_DEPTH_RATIO="${MIN_DEPTH_RATIO:-50}"

[ $# -ge 2 ] || { echo "사용법: SAMPLES=<파일> $0 <이름>:<여유>:<도착> ..." >&2; exit 2; }
[ -s "$SAMPLES" ] || { echo "판정 불가 — 표본 파일이 비었다: $SAMPLES" >&2; exit 2; }

names=(); credits=(); arrived=()
for spec in "$@"; do
    IFS=: read -r n c a <<< "$spec"
    case "$c$a" in ''|*[!0-9]*) echo "판정 불가 — 스펙이 어긋난다: $spec" >&2; exit 2 ;; esac
    names+=("$n"); credits+=("$c"); arrived+=("$a")
done

SAMPLES="$SAMPLES" MAX_DEVIATION="$MAX_DEVIATION" MAX_SKEW="$MAX_SKEW" \
MIN_DEPTH_RATIO="$MIN_DEPTH_RATIO" \
python3 - "${names[@]}" -- "${credits[@]}" -- "${arrived[@]}" <<'PY'
import os, sys

argv = sys.argv[1:]
a = argv.index('--'); b = argv.index('--', a + 1)
names = argv[:a]
credits = [int(x) for x in argv[a + 1:b]]
arrived = [int(x) for x in argv[b + 1:]]
n = len(names)

total_credit = sum(credits)
total_arrived = sum(arrived)
if total_credit <= 0:
    print("판정 불가 — 여유 합이 0 이다"); sys.exit(2)
if total_arrived <= 0:
    print("판정 불가 — 도착이 0 이다. 부하가 뒷단에 안 닿았다"); sys.exit(2)

max_dev = float(os.environ["MAX_DEVIATION"])
raw_skew = os.environ.get("MAX_SKEW", "").strip()
max_skew = float(raw_skew) if raw_skew else None
depth_floor = total_credit * float(os.environ["MIN_DEPTH_RATIO"]) / 100

expected = [c / total_credit for c in credits]

print("집계 비율 — 도착 %d 건" % total_arrived)
worst_dev = 0.0
for i in range(n):
    share = arrived[i] / total_arrived
    dev = (share - expected[i]) / expected[i] * 100
    worst_dev = max(worst_dev, abs(dev))
    print("  %-14s 여유 %3d · 도착 %5d · 점유 %5.1f%% (기대 %4.1f%%) · 편차 %+6.1f%%"
          % (names[i], credits[i], arrived[i], share * 100, expected[i] * 100, dev))

# **순간 점유.** 표본마다 그때 물린 것의 점유를 내고, 기대 대비 초과를 본다.
# **최댓값 하나로는 못 정한다.** 같은 조건을 두 번 돌려 20.0% 와 10.2% 가
# 나왔다 — 표본 하나가 문턱을 정하게 두면 통과 여부가 운에 걸린다. 상위
# 백분위를 함께 낸다: 몰림은 여러 표본에 걸쳐 나타나므로 백분위가 따라 오르고,
# 튄 표본 하나는 안 따라온다.
overs = [[] for _ in range(n)]
kept = 0
dropped = 0
for line in open(os.environ["SAMPLES"]):
    parts = line.split()
    if not parts:
        continue
    if len(parts) != n:
        print("판정 불가 — 표본의 열이 %d 개여야 하는데 %d 개다: %r" % (n, len(parts), line.strip()))
        sys.exit(2)
    try:
        depth = [int(x) for x in parts]
    except ValueError:
        print("판정 불가 — 표본에 숫자가 아닌 것이 있다: %r" % line.strip())
        sys.exit(2)
    live = sum(depth)
    # 얕은 표본은 버린다. 한두 건이 점유를 통째로 흔든다.
    if live < depth_floor:
        dropped += 1
        continue
    kept += 1
    for i in range(n):
        overs[i].append((depth[i] / live - expected[i]) / expected[i] * 100)

print()
print("순간 점유 — 표본 %d 개 (얕아서 버린 것 %d 개, 문턱 물린 것 %.0f 건)"
      % (kept, dropped, depth_floor))
if kept == 0:
    print("판정 불가 — 쓸 만한 표본이 없다. 깊이가 문턱에 못 닿았다")
    sys.exit(2)
def pct(xs, q):
    """상위 q 백분위. 표본이 적어 보간은 안 한다 — 있는 값 중 하나를 고른다."""
    ordered = sorted(xs)
    idx = min(len(ordered) - 1, int(len(ordered) * q / 100))
    return ordered[idx]

worst_skew = 0.0
for i in range(n):
    high = pct(overs[i], 95)
    worst_skew = max(worst_skew, high)
    print("  %-14s 상위 5%% 초과 %+6.1f%% (최대 %+6.1f%%)"
          % (names[i], high, max(overs[i])))

print()
fail = 0
if worst_dev > max_dev:
    print("미달 — 집계 편차 %.1f%% 가 한계 %.0f%% 를 넘는다" % (worst_dev, max_dev))
    fail = 1
else:
    print("집계 편차 %.1f%% (한계 %.0f%%)" % (worst_dev, max_dev))

if max_skew is None:
    print("상위 5%% 초과 %.1f%% — **문턱 없이 재기만 한다.** 값은 두 전략을 견줘 정한다"
          % worst_skew)
else:
    if worst_skew > max_skew:
        print("미달 — 상위 5%% 초과 %.1f%% 가 한계 %.0f%% 를 넘는다" % (worst_skew, max_skew))
        fail = 1
    else:
        print("상위 5%% 초과 %.1f%% (한계 %.0f%%)" % (worst_skew, max_skew))

sys.exit(1 if fail else 0)
PY

#!/usr/bin/env bash
# 게이트웨이 여러 대의 쏠림 판정 (9.4.5 · R-4).
#
# 종료 0 충족 · 1 미달 · 2 판정 불가.
#
#   사용: SAMPLES=<표본파일> [MAX_DEVIATION=15] [MAX_SKEW=25] [MIN_TOTAL=100] \
#           evaluate-skew.sh <이름>:<여유>:<도착> ...
#         표본 파일은 한 줄이 한 시점이고, 스펙과 같은 순서로 그때 물린 건수를 든다.
#
# **집계 비율만 보면 쏠림이 안 보인다.** 게이트웨이 둘이 같은 순서로 같은 대를
# 고르면 각자는 여유대로 나눈 것이라 합계는 맞는다. 깨지는 것은 같은 순간의
# 동시성이다 — 그 순간 대부분이 한 대에 몰리고, 그 대가 제 여유를 넘긴다.
# 그래서 도착 합계와 **순간 점유**를 따로 잰다.
set -uo pipefail

SAMPLES="${SAMPLES:?표본 파일이 필요하다}"
MAX_DEVIATION="${MAX_DEVIATION:-15}"
# 순간 점유의 문턱. **실측에서 뽑았다** — 여유 200/40/120 · 유입 264/s 에서
# 라운드로빈이 0.5%(게이트웨이 1대)와 2.0%(2대), P2C 가 34.8% 와 36.0% 였다.
# 25 는 라운드로빈의 열 배 위이고 P2C 아래라 양쪽에 여유가 있다. 처음에는 안
# 물게 두고 두 전략을 견준 뒤 정했다. 계기의 눈금은 그 조건에서 2.3% 다.
MAX_SKEW="${MAX_SKEW:-25}"
# 표본이 얕으면 점유가 한두 건에 흔들린다. 여유 합의 이 비율 아래는 버린다.
MIN_DEPTH_RATIO="${MIN_DEPTH_RATIO:-50}"
# 도착이 적으면 집계 편차가 우연히 맞는다. 표본이 이만큼은 있어야 잰 것이다.
MIN_TOTAL="${MIN_TOTAL:-100}"

# **설정값부터 본다.** 숫자가 아니면 뒤의 계산이 오류를 내는데, 그 오류가
# 종료 1 로 나와 **미달과 구분이 안 된다** — 망가진 설정이 "못 넘겼다" 로
# 적히고, 이 판정기의 종료 1 은 기본 전략을 뒤집은 근거다.
for setting in MAX_DEVIATION MAX_SKEW MIN_DEPTH_RATIO MIN_TOTAL; do
    value=$(eval "printf '%s' \"\$$setting\"")
    case "$value" in
        ''|*[!0-9]*) echo "$setting 은 0 이상의 정수여야 한다: '$value'"; exit 2 ;;
    esac
done

[ $# -ge 2 ] || { echo "사용법: SAMPLES=<파일> $0 <이름>:<여유>:<도착> ..." >&2; exit 2; }
[ -s "$SAMPLES" ] || { echo "판정 불가 — 표본 파일이 비었다: $SAMPLES" >&2; exit 2; }

names=(); credits=(); arrived=()
for spec in "$@"; do
    # **칸을 따로 본다.** 붙여서 검사하면 한쪽이 빈 것이 통과하고, 그 빈칸이
    # 0 으로 읽혀 판정이 정상처럼 나온다.
    IFS=: read -r n c a extra <<< "$spec"
    if [ -n "${extra:-}" ] || [ -z "${n:-}" ]; then
        echo "판정 불가 — 모양은 <이름:여유:도착> 이어야 한다: $spec" >&2; exit 2
    fi
    for field in "${c:-}" "${a:-}"; do
        case "$field" in
            ''|*[!0-9]*) echo "판정 불가 — 스펙이 어긋난다: $spec" >&2; exit 2 ;;
        esac
    done
    [ "$c" -gt 0 ] || { echo "판정 불가 — 여유가 0 인 대가 있다: $spec" >&2; exit 2; }
    names+=("$n"); credits+=("$c"); arrived+=("$a")
done

SAMPLES="$SAMPLES" MAX_DEVIATION="$MAX_DEVIATION" MAX_SKEW="$MAX_SKEW" \
MIN_DEPTH_RATIO="$MIN_DEPTH_RATIO" MIN_TOTAL="$MIN_TOTAL" \
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
max_skew = float(os.environ["MAX_SKEW"])
min_total = int(os.environ["MIN_TOTAL"])
# **도착이 적으면 비율이 우연히 맞는다.** 아홉 건을 나눠도 편차 0% 가 나온다.
if total_arrived < min_total:
    print("판정 불가 — 도착이 %d 건뿐이다 (최소 %d). 이 표본으로는 비율을 못 잰다"
          % (total_arrived, min_total))
    sys.exit(2)
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
mean_depth = []
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
    mean_depth.append(live)
    for i in range(n):
        overs[i].append((depth[i] / live - expected[i]) / expected[i] * 100)

print()
print("순간 점유 — 표본 %d 개 (얕아서 버린 것 %d 개, 문턱 물린 것 %.0f 건)"
      % (kept, dropped, depth_floor))
if kept == 0:
    print("판정 불가 — 쓸 만한 표본이 없다. 깊이가 문턱에 못 닿았다")
    sys.exit(2)
# **절반 넘게 버렸으면 그 회차는 못 잰 것이다.** 남은 몇 개로 백분위를 내면
# 그것은 사실상 최댓값이고, 최댓값으로는 통과 여부가 운에 걸린다.
if kept * 2 < kept + dropped or kept < 30:
    print("판정 불가 — 쓸 만한 표본이 %d 개뿐이다 (버린 것 %d 개). 깊이가 모자랐다"
          % (kept, dropped))
    sys.exit(2)

# **계기의 눈금을 같이 적는다.** 한 건이 점유의 몇 퍼센트인지가 유입과 지연으로
# 정해지는데, 그 눈금이 문턱에 가까우면 행동이 같은 구현도 한두 건 흔들림에
# 미달로 적힌다. 문턱의 3 분의 1 을 넘으면 그 회차는 이 문턱을 잴 해상도가 없다.
depth = sum(mean_depth) / kept
# **가장 작은 대가 눈금을 정한다.** 기대 점유가 작을수록 한 건이 크게 흔든다.
tick = max((1 / depth) / e * 100 for e in expected)
print("계기의 눈금 — 평균 깊이 %.0f 건에서 한 건이 %.1f%%" % (depth, tick))
if tick > max_skew / 3:
    print("판정 불가 — 눈금 %.1f%% 가 문턱 %.0f%% 의 3 분의 1 을 넘는다. "
          "여유를 키우거나 유입·지연을 올려 깊이를 늘린다" % (tick, max_skew))
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

if worst_skew > max_skew:
    print("미달 — 상위 5%% 초과 %.1f%% 가 한계 %.0f%% 를 넘는다" % (worst_skew, max_skew))
    fail = 1
else:
    print("상위 5%% 초과 %.1f%% (한계 %.0f%%)" % (worst_skew, max_skew))

if not fail:
    print("충족 — 집계도 순간도 한계 안이다")
sys.exit(1 if fail else 0)
PY

#!/usr/bin/env bash
# 서킷 진입·유지·회복 판정 (RC3 · RC4 · R1).
#
# **램프의 유일한 실측 근거다.** 서킷 해제 램프는 게이트웨이 한 대짜리 단위
# 시험으로만 잡혀 있었다. 승계로 이어받은 노드는 조인 적이 없어 램프가 아예
# 안 걸리는데, 그 구멍은 게이트웨이가 둘 이상일 때만 열린다.
#
# 표본 한 줄은 `<시각ms> <발행 크레딧> <뒷단 누적 도착> <노드 수>` 다. 구간은
# `#` 줄로 가르고, 러너가 자극을 준 시각에 그 줄을 쓴다.
#
# **발행 크레딧을 읽는다. 게이지가 아니다.** `waiting.capacity.credit` 은 보고를
# 합친 값이라 조임·램프 구간에 실제 발행분과 갈린다 (AIJ-0245). 그 게이지로
# 재면 이 판정이 "아무 일도 없었다" 로 자동 통과한다.
set -uo pipefail

# 2 는 계기를 고치라는 뜻이고 1 은 제품을 고치라는 뜻이다. 같은 코드로 내면
# "못 쟀다" 가 "미달" 로 읽힌다.
UNMEASURABLE=2

samples=${1:?표본 파일}

# 회복은 30초 안에 끝나야 한다 (RC3). 램프의 배수를 정하는 제약이 이것이다.
recovery_limit_sec=${RECOVERY_LIMIT_SEC:-30}
# 회복 봉우리의 허용 배수 (RC4). 넘으면 회복이 곧 2차 장애다.
burst_limit=${BURST_LIMIT:-1.2}
# 노드당 몫이 이만큼은 돼야 한산 통과 상한이 1 이 된다 (R1).
idle_divisor=${IDLE_DIVISOR:-2}
# 램프가 한 회차에 올릴 수 있는 배수. 승계 뒤 한 틱이 이 안이어야 한다.
ramp_step=${RAMP_STEP:-2.0}
# 기준선을 만들 최소 표본. 한둘로는 그 회차의 목표를 못 정한다.
min_baseline=${MIN_BASELINE:-4}
# 회복을 끝났다고 볼 기준선 대비 비율.
recovered_pct=${RECOVERED_PCT:-95}

if [ ! -s "$samples" ]; then
    echo "::error title=서킷 회복::표본이 비었다 — 회차를 못 쟀다"
    exit "$UNMEASURABLE"
fi

for phase in 정상 진입 유지 회복 승계; do
    if ! grep -q "^# ${phase}\$" "$samples"; then
        echo "::error title=서킷 회복::구간 표시 '# ${phase}' 가 없다 — 어디가 어느 구간인지 모른다"
        exit "$UNMEASURABLE"
    fi
done

verdict=$(awk \
    -v limit_sec="$recovery_limit_sec" -v burst="$burst_limit" \
    -v divisor="$idle_divisor" -v step="$ramp_step" \
    -v min_baseline="$min_baseline" -v recovered_pct="$recovered_pct" '
    function fail(msg) { printf "MISS %s\n", msg; exit }
    function block(msg) { printf "BLOCK %s\n", msg; exit }

    /^#/ { phase = $2; next }
    /^[[:space:]]*$/ { next }

    {
        if (NF != 4) {
            block(sprintf("표본의 열이 4 개가 아니다 — %d 번째 줄", NR))
        }
        for (i = 1; i <= 4; i++) {
            if ($i !~ /^[0-9]+$/) {
                block(sprintf("표본이 숫자가 아니다 — %d 번째 줄의 %d 번째 칸 \047%s\047", NR, i, $i))
            }
        }
        t = $1; credit = $2; served = $3; nodes = $4

        if (seen && served < prevServed) {
            block(sprintf("뒷단 누적 도착이 줄었다 (%d → %d) — 회차 중에 뒷단이 다시 떴다",
                    prevServed, served))
        }
        # **줄어드는 것은 우리가 만든 자극이다.** 회복 도중에 리더를 죽이므로
        # 그 수는 내려간다. 반대로 느는 것은 자극이 아니라 오염이다 — 앞 회차의
        # 등록이 살아나거나 다른 스택이 붙은 것이고, 그러면 한산 통과의 문턱이
        # 회차 중에 올라가 지킨 구간이 미달로 적힌다.
        if (seen && nodes > prevNodes) {
            block(sprintf("노드가 %d 에서 %d 로 늘었다 — 회차 중에 다른 대가 붙었다",
                    prevNodes, nodes))
        }

        # **정상 구간이 기준선이다.** 크레딧은 합으로 평균을 내고, 유입은 구간
        # 전체의 도착 증분을 벽시계로 나눈다 — 한 점만 보면 잡음이 기준이 된다.
        if (phase == "정상") {
            baseSum += credit; baseN++
            baseNodes = nodes
            if (baseN == 1) { baseT = t; baseServed = served }
            baseLastT = t; baseLastServed = served
        }

        # 조인 구간의 상한은 노드 수다. 반쯤 열리면 노드당 한 건이 나가고,
        # 열려 있으면 0 이다 — 어느 쪽이든 이 수를 안 넘는다.
        if (phase == "진입") {
            enterN++
            if (credit <= nodes) { entered = 1 }
        }
        if (phase == "유지" && credit > nodes) {
            fail(sprintf("조임이 유지되지 않았다 — 크레딧 %d 가 상한 %d 를 넘었다", credit, nodes))
        }

        if (phase == "회복" || phase == "승계") {
            recN++
            if (recN == 1) { recT = t; recServed = served }
            # **회복 구간 내내 한산 통과가 성립해야 한다** (R1). 노드당 몫이
            # 유휴 나눗값 아래면 그 상한이 0 이고, 줄 설 이유가 없는 쿠폰이
            # 전 노드에서 줄을 선다.
            if (credit < nodes * divisor) {
                fail(sprintf("회복 중 한산 통과가 막혔다 — 크레딧 %d, 노드 %d, 최소 %d",
                        credit, nodes, nodes * divisor))
            }
            # 틱당 도착을 기준선과 견준다. 첫 표본은 앞이 없어 건너뛴다.
            if (recPrevT > 0 && t > recPrevT) {
                rate = (served - recPrevServed) * 1000.0 / (t - recPrevT)
                if (rate > peakRate) { peakRate = rate }
            }
            recPrevT = t; recPrevServed = served
            if (!doneAt && baseN >= min_baseline) {
                target = baseSum / baseN * recovered_pct / 100.0
                if (credit >= target) { doneAt = t }
            }
        }

        # **승계 직후 한 틱이 램프 안이어야 한다.** 이어받은 노드는 조인 적이
        # 없어 램프가 안 걸린다 — 게이트웨이가 둘 이상일 때만 열리는 구멍이다.
        if (phase == "승계" && !handoverSeen) {
            handoverSeen = 1
            allowed = beforeHandover * step
            floorAllowed = nodes * divisor
            if (allowed < floorAllowed) { allowed = floorAllowed }
            if (credit > allowed + 0.5) {
                fail(sprintf("승계 뒤 한 틱이 램프를 넘었다 — %d 에서 %d 로, 허용 %d",
                        beforeHandover, credit, int(allowed)))
            }
        }
        if (phase != "승계") { beforeHandover = credit }

        prevServed = served; prevNodes = nodes; seen = 1
    }

    END {
        if (!seen) { block("표본이 한 줄도 없다") }
        # **회차를 시작한 대수로 본다.** 끝 값으로 보면 리더를 죽인 뒤의 수라,
        # 우리가 만든 자극이 이 회차를 판정 불가로 만든다.
        if (baseNodes < 2) {
            block(sprintf("게이트웨이가 %d 대로 시작했다 — 이 판정은 둘 이상이라야 뜻이 있다",
                    baseNodes))
        }
        if (baseN < min_baseline) {
            block(sprintf("기준선 표본이 %d 개다 (최소 %d) — 그 회차의 목표를 못 정한다",
                    baseN, min_baseline))
        }
        baseSec = (baseLastT - baseT) / 1000.0
        baseRate = baseSec > 0 ? (baseLastServed - baseServed) / baseSec : 0
        if (baseRate <= 0) {
            block("기준선 유입이 0 이다 — 부하가 안 닿았다")
        }
        if (!enterN) { block("진입 구간에 표본이 없다") }
        if (!entered) {
            fail("서킷이 열렸는데 배분을 조이지 않았다 — 진입 구간의 크레딧이 상한 위다")
        }
        if (!recN) { block("회복 구간에 표본이 없다") }
        if (!handoverSeen) { block("승계 구간에 표본이 없다") }

        if (!doneAt) {
            fail(sprintf("회복이 안 끝났다 — %.1f 초 동안 기준선의 %d%% 에 못 닿았다",
                    (recPrevT - recT) / 1000.0, recovered_pct))
        }
        took = (doneAt - recT) / 1000.0
        if (took > limit_sec) {
            fail(sprintf("회복이 %.1f 초 걸렸다 (한계 %d 초)", took, limit_sec))
        }
        if (peakRate > baseRate * burst) {
            fail(sprintf("회복 봉우리가 초당 %.1f 건이다 — 기준선 %.1f 의 %.2f 배 (한계 %.1f)",
                    peakRate, baseRate, peakRate / baseRate, burst))
        }
        printf "PASS %.1f %.1f %.1f %.2f\n", took, baseRate, peakRate, peakRate / baseRate
    }
' "$samples")

case "$verdict" in
    BLOCK*)
        echo "::error title=서킷 회복::${verdict#BLOCK }"
        exit "$UNMEASURABLE" ;;
    MISS*)
        echo "판정: 미달 — ${verdict#MISS }"
        exit 1 ;;
    PASS*)
        set -- $verdict
        printf '  %-24s %s초\n' "회복에 걸린 시간" "$2"
        printf '  %-24s 초당 %s건\n' "기준선 유입" "$3"
        printf '  %-24s 초당 %s건 (%s배)\n' "회복 봉우리" "$4" "$5"
        echo "판정: 충족 — 진입·유지·회복이 다 기준 안이다"
        exit 0 ;;
    *)
        echo "::error title=서킷 회복::판정을 못 냈다: '$verdict'"
        exit "$UNMEASURABLE" ;;
esac

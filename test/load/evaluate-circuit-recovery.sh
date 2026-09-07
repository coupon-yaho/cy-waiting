#!/usr/bin/env bash
# 서킷 진입·유지·회복 판정 (RC3 · RC4 · R1).
#
# **램프의 유일한 실측 근거다.** 서킷 해제 램프는 게이트웨이 한 대짜리 단위
# 시험으로만 잡혀 있었다. 승계로 이어받은 노드는 조인 적이 없어 램프가 아예
# 안 걸리는데, 그 구멍은 게이트웨이가 둘 이상일 때만 열린다.
#
# 표본 한 줄은 `<시각ms> <발행 크레딧> <뒷단 누적 도착> <노드 수> <노드별 서킷 표>`
# 다. 구간은 `#` 줄로 가르고, 러너가 자극을 준 시각에 그 줄을 쓴다.
#
# **표를 같이 뜨는 이유.** 크레딧 하나만 보면 "서킷이 아직 안 닫혔다" 와 "표는
# 닫혔는데 게이트가 안 풀렸다" 가 같은 그림이다. 둘은 고칠 자리가 다르다 —
# 앞엣것은 서킷 설정이고 뒤엣것은 클러스터 표의 비대칭 완화다.
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
# 램프 배수. **제품 상수에서 읽는다** — 손으로 적으면 둘이 갈라져, 배수를 바꾼
# 회차가 지킨 것도 어긴 것도 아닌 값으로 판정된다.
ramp_src=src/main/java/com/kafkick/waiting/domain/allocation/ReleaseRamp.java
ramp_step=${RAMP_STEP:-$(sed -n 's/.*DEFAULT_STEP = \([0-9.]*\);.*/\1/p' "$ramp_src" 2>/dev/null)}
case "$ramp_step" in
    ''|*[!0-9.]*) echo "::error title=서킷 회복::램프 배수를 못 읽었다 ($ramp_src)"; exit "$UNMEASURABLE" ;;
esac
# 배분의 정책 하한. 제품 기본값과 같아야 한다 (ControlPlaneProperties 의 capacity.floor).
capacity_floor=${CAPACITY_FLOOR:-5}
# 기준선을 만들 최소 표본. 한둘로는 그 회차의 목표를 못 정한다.
min_baseline=${MIN_BASELINE:-4}
# 회복을 끝났다고 볼 기준선 대비 비율.
recovered_pct=${RECOVERED_PCT:-95}
# 전 노드가 닫혔다고 한 뒤로 게이트가 풀리기까지 봐 주는 시간(초).
#
# **제품의 설계 최소에서 끌어온다.** 완화는 한 계단당 연속 관측 세 틱이고 열림에서
# 닫힘까지 두 계단이라 6초, 거기에 배분 틱 하나와 표 왕복이 더 붙는다. 5 로 두면
# 열린 상태에서 돌아오는 회차가 맞게 도는데도 "완화가 늦다" 로 미달이 된다.
vote_gate_limit_sec=${VOTE_GATE_LIMIT_SEC:-9}
# 열린 노드가 다시 반쯤 열리기까지 걸리는 최소 시간(ms). 잔여를 잴 때 표가
# 재진입을 숨기는지 가르는 데만 쓴다.
#
# **`application.yml` 의 `wait-duration-in-open-state` 와 같아야 한다.** 거기를
# 바꾸면 여기도 바꾼다. 게다가 우리가 보는 것은 전이 시각이 아니라 표본에 처음
# 보인 시각이라 실제보다 늦다 — 유예는 그만큼 낙관이다.
reopen_grace_ms=${REOPEN_GRACE_MS:-5000}
# 봉우리를 재는 창(ms). 뒷단 계수가 뭉쳐 오르므로 이웃 표본으로 나누면 봉우리가
# 표본 간격의 산물이 된다. RC4 는 지속 봉우리를 막는 조항이다.
peak_window_ms=${PEAK_WINDOW_MS:-1000}
# 배분이 열려 있는데 뒷단 도착이 멎어도 봐 주는 시간(ms). 조인 구간의 프로브가
# 초당 한 건 아래라 몇 표본은 그냥 평평하다 — 표본 수로 세면 정상을 잡는다.
tail_idle_ms=${TAIL_IDLE_MS:-8000}
# 해제 표시와 크레딧이 실제로 오르는 사이의 유예(ms). 표시는 로그로 잡는데
# 크레딧은 다음 배분 틱에서야 오른다 — 틱이 1초라 그보다 넉넉히 준다.
release_grace_ms=${RELEASE_GRACE_MS:-1500}

if [ ! -s "$samples" ]; then
    echo "::error title=서킷 회복::표본이 비었다 — 회차를 못 쟀다"
    exit "$UNMEASURABLE"
fi

for phase in 정상 진입 유지 회복; do
    if ! grep -q "^# ${phase}\$" "$samples"; then
        echo "::error title=서킷 회복::구간 표시 '# ${phase}' 가 없다 — 어디가 어느 구간인지 모른다"
        exit "$UNMEASURABLE"
    fi
done

verdict=$(awk \
    -v limit_sec="$recovery_limit_sec" -v burst="$burst_limit" \
    -v divisor="$idle_divisor" -v step="$ramp_step" \
    -v min_baseline="$min_baseline" -v recovered_pct="$recovered_pct" \
    -v grace_ms="$release_grace_ms" -v vote_limit="$vote_gate_limit_sec" \
    -v tail_idle_ms="$tail_idle_ms" -v reopen_grace_ms="$reopen_grace_ms" \
    -v peak_window_ms="$peak_window_ms" \
    -v capacity_floor="$capacity_floor" '
    # **awk 의 exit 는 END 를 건너뛰지 않는다.** 표시를 안 두면 본문에서 낸
    # 판정 뒤에 END 가 한 줄을 더 찍고, 부르는 쪽은 둘 중 뒤엣것을 읽는다.
    # 표에 완전히 열린 노드가 있는가. `HALF_OPEN` 이 부분 문자열로 걸리므로
    # 접힌 문자열을 쪼개서 본다.
    function hasOpen(v,   parts, i, n) {
        n = split(v, parts, "|")
        for (i = 1; i <= n; i++) { if (parts[i] == "OPEN") { return 1 } }
        return 0
    }

    function layers() {
        return sprintf("게이트 해제까지 %.1f초 · 표가 닫힌 뒤 조인 시간 %.1f초 · half-open 잔여 %.1f초",
                releasedAt ? (releasedAt - recT) / 1000.0 : -1, maxVoteGapMs / 1000.0,
                residualMs / 1000.0)
    }
    # 층 수치를 실패에도 싣는다. 이 판정은 대개 미달인데, 층을 가르려고 칸을
    # 늘려 놓고 그 수가 실패 경로에서 안 보이면 손으로 표본을 뒤지게 된다.
    function fail(msg) { decided = 1; printf "MISS %s [%s]\n", msg, layers(); exit }
    function block(msg) { decided = 1; printf "BLOCK %s\n", msg; exit }

    # **미측정을 첫 줄부터 -1 로 둔다.** 회복 구간에 닿기 전에 나는 실패도
    # 층 수치를 싣는데, 그때 0 이 찍히면 "즉시 전이했다" 로 읽힌다.
    BEGIN { residualMs = -1000 }

    /^#/ { phase = $2; next }
    /^[[:space:]]*$/ { next }

    {
        if (NF != 5) {
            block(sprintf("표본의 열이 5 개가 아니다 — %d 번째 줄", NR))
        }
        for (i = 1; i <= 4; i++) {
            if ($i !~ /^[0-9]+$/) {
                block(sprintf("표본이 숫자가 아니다 — %d 번째 줄의 %d 번째 칸 \047%s\047", NR, i, $i))
            }
        }
        t = $1; credit = $2; served = $3; nodes = $4; vote = $5
        if (vote !~ /^[A-Z_|-]+$/) {
            block(sprintf("서킷 표가 상태 문자열이 아니다 — %d 번째 줄 \047%s\047", NR, vote))
        }

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
        if (phase == "정상" && credit > ceiling) { ceiling = credit }
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
        if (phase == "유지") { holdN++ }
        if (phase == "유지" && credit > nodes) {
            fail(sprintf("조임이 유지되지 않았다 — 크레딧 %d 가 상한 %d 를 넘었다", credit, nodes))
        }

        # **표가 다 닫혔는데도 조여 있는 시간을 잰다.** 그 구간이 길면 고칠
        # 자리는 서킷이 아니라 게이트다 — 표는 이미 "뒷단이 멀쩡하다" 인데
        # 배분만 안 푼 것이다.
        if ((phase == "회복" || phase == "해제" || phase == "승계") &&
                vote == "CLOSED" && credit <= nodes) {
            if (voteClosedFrom == 0) { voteClosedFrom = t }
            voteGapMs = t - voteClosedFrom
            if (voteGapMs > maxVoteGapMs) { maxVoteGapMs = voteGapMs }
        } else {
            voteClosedFrom = 0
        }

        if (phase == "회복" || phase == "해제" || phase == "승계") {
            recN++
            if (recN == 1) { recT = t; recFirstServed = served; recVote = vote }
            # **자극을 걷는 순간의 서킷 위상은 통제되지 않는다.** 그때 열려 있던
            # half-open 은 자극 구간에서 시작한 것이라, 남은 수명이 회차마다
            # 0~상한 사이에서 다르게 나온다. 그 항을 안 재면 회차 간 차이를
            # 프로브 공급이 좋아진 것으로 읽는다.
            #
            # **표에서 half-open 이 사라질 때까지 잰다.** 노드 하나가 먼저 나가면
            # 표는 바뀌지만 다른 노드는 아직 반쯤 열려 있다. 첫 변화로 끊으면
            # 회차마다 제일 짧은 노드의 값이 적힌다.
            #
            # **표가 재진입을 숨길 수 있다.** 노드별 상태를 접은 문자열이라, 먼저
            # 나간 노드가 열림 대기 뒤 다시 반쯤 열리면 그 구간이 처음 구간의
            # 잔여에 섞인다. 열린 노드가 보인 뒤 그 대기보다 오래 half-open 이
            # 남아 있으면 가를 수 없으므로 못 잰 것으로 둔다.
            if (residualMs < 0 && recVote ~ /HALF_OPEN/) {
                if (openSeenAt == 0 && hasOpen(vote)) { openSeenAt = t }
                if (vote !~ /HALF_OPEN/) {
                    residualMs = (openSeenAt && t - openSeenAt >= reopen_grace_ms) \
                            ? -1000 : t - recT
                }
            }
            lastRecT = t
            # **도착이 멎은 시간을 잰다. 표본 수가 아니다.**
            #
            # 그리고 **크레딧이 0 인 동안은 안 센다.** 서킷이 활짝 열린 구간에는
            # 배분이 0 이라 뒷단에 아무것도 안 가는 것이 정상이다 — 표본 수로
            # 세면 제품이 제 일을 한 구간을 "부하가 먼저 끝났다" 로 읽고, 이
            # 하네스가 찾으려던 실패를 판정 불가로 덮는다.
            if (credit > 0) {
                if (recPrevSample > 0 && served == recPrevSample) {
                    if (flatFrom == 0) { flatFrom = recPrevSampleT }
                } else {
                    flatFrom = 0
                }
            } else {
                flatFrom = 0
            }
            if (flatFrom > 0 && t - flatFrom > maxFlatMs) { maxFlatMs = t - flatFrom }
            recPrevSample = served; recPrevSampleT = t
            # **자극을 걷은 것과 게이트가 풀린 것은 다르다.** 서킷은 제 창을
            # 채워야 닫히므로, 뒷단이 멀쩡해진 뒤로도 한동안 조인 채로 있다.
            # 그 구간의 낮은 크레딧은 램프가 만든 것이 아니라 게이트가 만든
            # 것이라, 램프의 기준으로 재면 지킨 회차가 미달로 적힌다.
            #
            # **크레딧으로 유추하지 않는다.** 승계로 노드 수가 줄면 같은 값이
            # 갑자기 상한 위로 보여, 안 풀린 회차가 풀린 것으로 적힌다. 러너가
            # 배분의 전이 로그를 보고 표시를 남긴다.
            if (!releasedAt && (phase == "해제" || phase == "승계")) {
                releasedAt = t
            }
            if (releasedAt) {
                relN++
                # **풀린 뒤로는 한산 통과가 성립해야 한다** (R1). 노드당 몫이
                # 유휴 나눗값 아래면 그 상한이 0 이고, 줄 설 이유가 없는 쿠폰이
                # 전 노드에서 줄을 선다.
                #
                # **한 틱을 유예한다.** 해제는 로그로 잡는데 크레딧은 다음 배분
                # 틱에서야 오른다 — 표본이 그보다 촘촘하면 그 사이 여러 표본이
                # 아직 조인 값이고, 맞게 도는 제품이 미달로 적힌다. 표본 수가
                # 아니라 시간으로 준다.
                if (t - releasedAt > grace_ms && credit < nodes * divisor) {
                    fail(sprintf("풀린 뒤 한산 통과가 막혔다 — 크레딧 %d, 노드 %d, 최소 %d",
                            credit, nodes, nodes * divisor))
                }
                # **봉우리는 창으로 잰다.** 뒷단 계수는 뭉쳐 오르는데 표본은
                # 0.5초 간격이라, 이웃 표본으로 나누면 한 뭉치가 통째로 그 칸에
                # 실려 두 배짜리 봉우리가 만들어진다. 기준선은 구간 전체를
                # 평균하므로 그렇게 견주면 재는 것이 다르다.
                #
                # RC4 가 막으려는 것은 회복이 곧 2차 장애가 되는 지속 봉우리다.
                relN++; relT[relN] = t; relS[relN] = served
                if (relAnchor == 0) { relAnchor = 1 }
                # 창 안에 드는 가장 오래된 표본까지 물린다.
                while (relAnchor < relN && t - relT[relAnchor] > peak_window_ms) {
                    relAnchor++
                }
                # 창을 못 채우면 있는 만큼으로 잰다. 표본이 그것뿐이면 그 값이
                # 최선이고, 아예 안 재면 RC4 가 조용히 통과한다.
                if (relAnchor < relN && t > relT[relAnchor]) {
                    rate = (served - relS[relAnchor]) * 1000.0 / (t - relT[relAnchor])
                    peakSeen = 1
                    if (rate > peakRate) { peakRate = rate }
                }
                if (!doneAt && baseN >= min_baseline) {
                    target = baseSum / baseN * recovered_pct / 100.0
                    if (credit >= target) { doneAt = t }
                }
            }
        }

        # **승계 직후 한 틱이 램프 안이어야 한다.** 이어받은 노드는 조인 적이
        # 없어 램프가 안 걸린다 — 게이트웨이가 둘 이상일 때만 열리는 구멍이다.
        #
        # 게이트가 아직 안 풀린 승계는 건너뛴다. 그 구간의 앞 값은 램프가 아니라
        # 게이트가 정한 것이라, 배수를 거기에 걸면 아무 뜻이 없다.
        #
        # **첫 표본만 보지 않는다.** 승계 뒤 몇 틱은 이어받은 노드가 제 스무더를
        # 이월받고 첫 회차를 도는 구간이라, 계단이 둘째나 셋째 틱에 선다. 첫
        # 표본만 보면 그 계단을 통째로 놓친다 — 이 하네스가 있는 이유가 거기다.
        if (phase == "승계" && releasedAt) {
            # **램프 밖에서 난 승계는 계단을 못 잰다.** 직전 값이 이미 상한이면
            # 허용이 상한의 배수라, 이어받은 노드가 램프를 통째로 건너뛰어도
            # 통과한다. 그 회차는 이 검사가 없는 것과 같다.
            if (!handoverSeen && ceiling > 0 && beforeHandover >= ceiling) {
                printf "  승계가 램프 밖에서 났다 — 계단 검사는 이 회차로 뜻이 없다 (직전 %d, 상한 %d)\n",
                        beforeHandover, ceiling | "cat 1>&2"
            }
            handoverSeen = 1
            allowed = beforeHandover * step
            # **제품의 하한과 같은 값이라야 한다.** 배분은 정책 하한과 R1 최소 중
            # 큰 쪽을 램프에 넘기므로, 노드 수만으로 잡으면 지킨 회차가 미달로
            # 적힌다 — 승계 직후 노드가 하나로 줄면 특히 그렇다.
            floorAllowed = nodes * divisor
            if (capacity_floor > floorAllowed) { floorAllowed = capacity_floor }
            if (allowed < floorAllowed) { allowed = floorAllowed }
            if (credit > allowed + 0.5) {
                fail(sprintf("승계 뒤 한 틱이 램프를 넘었다 — %d 에서 %d 로, 허용 %d",
                        beforeHandover, credit, int(allowed)))
            }
        }
        beforeHandover = credit

        prevServed = served; prevNodes = nodes; seen = 1
    }

    END {
        if (decided) { exit }
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
        # **유입이 죽은 꼬리로 판정하지 않는다.** 러너의 산술이 다시 어긋나도
        # 여기서 끊긴다 — 앞선 회차가 그 꼬리를 분모에 넣고 결론을 냈다.
        if (maxFlatMs > tail_idle_ms) {
            block(sprintf("배분이 열려 있는데 뒷단 도착이 %.1f 초 없다 — 부하가 먼저 끝났다",
                    maxFlatMs / 1000.0))
        }
        # **유지 구간에 표본이 있어야 한다.** 없으면 조임이 붙어 있었는지를 한
        # 번도 안 보고 지나간다 — 그 구간을 재려고 만든 회차다.
        if (!holdN) { block("유지 구간에 표본이 없다 — 조임이 붙어 있었는지를 못 본다") }

        # **게이트가 안 풀렸으면 회복이 시작도 안 한 것이다.** 원인을 이름으로
        # 부른다 — 그러지 않으면 램프의 기준이 대신 울려, 램프가 못 한 일처럼
        # 적힌다. 실측에서 이 자리가 먼저 걸렸다.
        if (!releasedAt) {
            # **표를 먼저 본다.** 전 노드가 닫혔다고 하는데도 끝내 안 풀렸으면
            # 고칠 자리는 서킷이 아니라 완화다. 표 칸을 넣은 이유가 이것인데,
            # 아래 검사는 이 갈래 뒤에 있어 영영 도달 못 했다.
            if (maxVoteGapMs / 1000.0 > vote_limit) {
                fail(sprintf("표는 닫혔는데 게이트가 끝내 안 풀렸다 — %.1f 초 (한계 %d 초), 완화가 늦다",
                        maxVoteGapMs / 1000.0, vote_limit))
            }
            fail(sprintf("서킷이 안 닫혀 배분이 안 풀렸다 — %.1f 초 동안 크레딧이 상한 %d 위로 안 올라갔다",
                    (lastRecT - recT) / 1000.0, prevNodes))
        }
        if (!handoverSeen) { block("승계가 게이트 해제 뒤에 안 왔다 — 램프를 못 잰다") }

        if (!doneAt) {
            fail(sprintf("회복이 안 끝났다 — 풀린 뒤 %.1f 초 동안 기준선의 %d%% 에 못 닿았다",
                    (lastRecT - releasedAt) / 1000.0, recovered_pct))
        }
        # **먼저 층을 가른다.** 합계만 보면 서킷이 늦은 것과 게이트가 늦은 것이
        # 같은 미달로 나가고, 다음 사람이 엉뚱한 데를 고친다.
        if (maxVoteGapMs / 1000.0 > vote_limit) {
            fail(sprintf("표는 닫혔는데 게이트가 %.1f 초 동안 안 풀렸다 (한계 %d 초) — 서킷이 아니라 완화가 늦다",
                    maxVoteGapMs / 1000.0, vote_limit))
        }
        took = (doneAt - recT) / 1000.0
        if (took > limit_sec) {
            fail(sprintf("회복이 %.1f 초 걸렸다 (한계 %d 초)", took, limit_sec))
        }
        # **못 잰 봉우리를 통과로 적지 않는다.** 창을 채울 표본이 없으면 RC4 는
        # 검증된 것이 아니다. 0 으로 찍으면 그 회차가 조용히 초록이 된다.
        if (!peakSeen) {
            block("회복 봉우리를 못 쟀다 — 풀린 뒤 표본이 둘도 안 된다")
        }
        # **기준은 정상 유입이다** (RC4). 분모를 상한으로 바꾸면 여유가 있는
        # 회차가 전부 통과하는데, 그건 판정을 느슨하게 한 것이지 제품이 나아진
        # 것이 아니다. 상한 대비도 같이 보되 그쪽은 보조다.
        if (ceiling > 0 && peakRate > ceiling * burst) {
            fail(sprintf("회복 봉우리가 초당 %.1f 건이다 — 상한 %d 의 %.2f 배 (한계 %.1f)",
                    peakRate, ceiling, peakRate / ceiling, burst))
        }
        if (peakRate > baseRate * burst) {
            fail(sprintf("회복 봉우리가 초당 %.1f 건이다 — 기준선 %.1f 의 %.2f 배 (한계 %.1f)",
                    peakRate, baseRate, peakRate / baseRate, burst))
        }
        printf "PASS %.1f %.1f %.1f %.2f %.1f %.1f %.1f\n", took, baseRate, peakRate,
                peakRate / baseRate, (releasedAt - recT) / 1000.0, maxVoteGapMs / 1000.0,
                residualMs / 1000.0
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
        printf '  %-24s %s초\n' "게이트가 풀리기까지" "$6"
        printf '  %-24s %s초\n' "표가 닫힌 뒤 조인 시간" "$7"
        printf '  %-24s %s초\n' "half-open 잔여" "$8"
        echo "판정: 충족 — 진입·유지·회복이 다 기준 안이다"
        exit 0 ;;
    *)
        echo "::error title=서킷 회복::판정을 못 냈다: '$verdict'"
        exit "$UNMEASURABLE" ;;
esac

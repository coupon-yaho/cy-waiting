#!/usr/bin/env bash
# 현재 최대치 회차 (10.7.2).
#
# **100K 를 만들 수단이 없다.** 목표 수를 게이트로 쓰면 만든 적 없는 부하를
# 판정하게 되므로, 이 러너는 유입을 낮은 쪽에서부터 올리며 **지금 실제로 서는
# 최대치를 기록**한다 (O-8).
#
# **회차마다 빈 줄에서 시작한다.** 앞 회차가 남긴 대기열이 있으면 첫 요청부터
# QUEUE_FULL 이라 등록 경로를 한 번도 안 밟는다 — 착수 판정을 세 번 갈리게
# 만든 것이 정확히 그것이다 (shard-gate.sh 의 머리글).
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1

. test/load/peak-lib.sh || exit 2

COMPOSE="docker compose -f test/load/compose.yml"

# **코어 한도를 주면 천장 원인이 뜻을 갖는다.** 한도가 없으면 게이트웨이가 호스트 코어를 다 쓸 수
# 있어, 게이트웨이가 붙는 것과 호스트가 마르는 것이 같은 일이 된다. 안 주면 옛 회차와 같은 조건으로 돈다.
if [ -n "${GATEWAY_CPUS:-}" ]; then
    COMPOSE="$COMPOSE -f test/load/compose.limits.yml"
fi
bottleneck_cpus=${GATEWAY_CPUS:-$(nproc)}

# **게이트웨이 대수.** 여럿이면 유입을 고르게 나누고 판정 비율은 대마다 낸다.
GATEWAYS=${GATEWAYS:-1}
case "$GATEWAYS" in
    ''|*[!0-9]*|0*|??????????*) echo "::error title=현재 최대치::GATEWAYS 는 아홉 자리 이하의 양의 정수여야 한다: '$GATEWAYS'"; exit 2 ;;
esac
# 표집기가 컨테이너 이름 앞머리로 우리 것만 고른다. compose 가 쓰는 프로젝트 이름과 같아야 한다.
PROJECT=${COMPOSE_PROJECT_NAME:-load}
if [ "$GATEWAYS" -gt 1 ]; then
    COMPOSE="$COMPOSE -f test/load/compose.multi.yml"
fi

# **LB 를 지나는 회차.** 실제 요청 경로다. 대신 LB 가 측정에 섞이므로 원인 판정이 LB 도 보고, 오버헤드는
# 같은 코어 한도의 기준선(`lb-baseline.sh`)을 빼서 읽는다.
VIA_LB=${VIA_LB:-0}
case "$VIA_LB" in
    0) lb_env="" ;;
    1) COMPOSE="$COMPOSE -f test/load/compose.lb.yml"; lb_env=${LB_CPUS:-2} ;;
    *) echo "::error title=현재 최대치::VIA_LB 는 0 이나 1 이어야 한다: '$VIA_LB'"; exit 2 ;;
esac

# peak.js 가 두 쿠폰을 박아 두고 있다. 여기만 바꾸면 다른 쿠폰을 비우고 이 쿠폰을
# 때리게 된다 — 시나리오를 고칠 때 같이 고친다.
COUPONS="c1 c2"

# 사다리. 낮은 쪽에서부터 올린다 — 천장을 지나친 뒤의 값은 뜻이 없다.
RATES=${RATES:-"1000 2000 4000 8000 16000"}
DURATION=${DURATION:-30s}
# **회차를 돌리기 전에 끊는다.** 못 읽는 형식이면 기대 건수가 엉뚱해져 "부하가
# 안 닿았다" 가드가 사라지는데, 그 사실은 회차를 다 돌린 뒤에야 드러난다.
DURATION_SEC=$(peak_duration_sec "$DURATION")
if [ "$DURATION_SEC" = 0 ]; then
    echo "DURATION 을 못 읽는다 — 30s · 1m · 1m30s 꼴이어야 한다: '$DURATION'"; exit 2
fi

# **천장을 지나면 사다리를 멈춘다.** 그 위 회차는 판정에 안 쓰이면서 호스트만
# 먹는다 — 32,000 회차에서 VU 가 2 만을 넘자 연결이 EOF 로 끊기고 러너가
# 호스트를 물었다. 값을 못 주는 회차에 기계를 태우지 않는다.
#
# **천장은 세 종류다.** 하네스가 못 만든 것, 게이트웨이가 못 버틴 것, 판정을
# 못 한 것이다. 판정기는 셋 다 위를 버리므로 러너도 셋 다에서 멈춘다 — 하나만
# 보면 게이트웨이가 무너진 뒤에도 사다리가 계속 올라가 그 상황이 되풀이된다.
STOP_AT_CEILING=${STOP_AT_CEILING:-1}
# **여기서도 확인한다.** 판정기의 같은 검사는 기계를 이미 태운 뒤에 돈다.
# 숫자가 아니면 awk 가 0 으로 읽어 `실측 >= 요청 * 0` 이 늘 참이 되고, 멈추라고
# 둔 가드가 그 자리에서 사라진다.
TOLERANCE=${PEAK_ARRIVAL_TOLERANCE:-0.95}
if ! printf '%s' "$TOLERANCE" | grep -Eq '^[0-9]+(\.[0-9]+)?$' \
        || ! awk -v t="$TOLERANCE" 'BEGIN{ exit (t > 0 && t <= 1) ? 0 : 1 }'; then
    echo "허용 오차는 0 초과 1 이하의 수여야 한다: '$TOLERANCE'"; exit 2
fi

OUT_TABLE=${OUT_TABLE:-peak-steps.tsv}
case "$OUT_TABLE" in
    *.tsv) ;;
    *) echo "OUT_TABLE 은 .tsv 여야 한다: '$OUT_TABLE'"; exit 2 ;;
esac
OUT_DIR=${OUT_DIR:-peak-out}

command -v k6 >/dev/null || { echo "k6 가 없다"; exit 2; }

jar=${WAITING_JAR:-build/libs/waiting.jar}
[ -f "$jar" ] || { echo "실행 JAR 이 없다: $jar — ./gradlew build 를 먼저 돌린다"; exit 2; }

# **이미지를 먼저 짓는다.** compose 는 JAR 이 바뀌어도 있는 이미지를 그대로 쓴다.
$COMPOSE build gateway backend >/dev/null 2>&1 || { echo "이미지를 못 지었다"; exit 2; }
$COMPOSE rm -sf gateway warmup >/dev/null 2>&1
$COMPOSE up -d --wait --wait-timeout 240 --scale gateway="$GATEWAYS" \
    || { echo "스택을 못 세웠다"; exit 2; }

# **실제로 열린 포트를 찾는다.** 범위로 열면 어느 대가 어느 포트를 받는지 순서가 안 정해진다. 박아 두면 한 대에만
# 전부 보내면서 여럿에 나눠 보냈다고 적는다.
bases=""
for idx in $(seq 1 "$GATEWAYS"); do
    port=$($COMPOSE port --index "$idx" gateway 8080 2>/dev/null | sed 's/.*://')
    case "$port" in
        ''|*[!0-9]*) echo "게이트웨이 $idx 의 포트를 못 찾았다"; exit 2 ;;
    esac
    bases="${bases:+$bases,}http://localhost:$port"
done
echo "게이트웨이 ${GATEWAYS}대: $bases"
# LB 를 지나면 k6 는 LB 한 곳만 친다. 나누기는 LB 가 한다.
if [ "$VIA_LB" = 1 ]; then
    bases=http://localhost:18070
    echo "LB 경유: $bases"
fi
export BASE_URLS=$bases

rm -rf "$OUT_DIR"; mkdir -p "$OUT_DIR"
: > "$OUT_TABLE"

# **대마다 따로 긁는다.** 파일은 `<경로>.<대 번호>` 다. 못 긁은 대는 빈 파일로 남고 판정기가 판정 불가로 낸다.
metrics() {
    local idx
    for idx in $(seq 1 "$GATEWAYS"); do
        $COMPOSE exec -T --index "$idx" gateway \
            wget -qO- http://localhost:8081/actuator/prometheus 2>/dev/null > "$1.$idx" \
            || : > "$1.$idx"
    done
}

# **줄 키를 다 지운다.** 셋만 지우면 이탈 기록과 생존 신호와 배분 펜스가 앞
# 회차 값을 들고 넘어간다. 재고는 시더가 관리하므로 안 건드린다.
empty_queues() {
    local c
    for c in $COUPONS; do
        $COMPOSE exec -T redis redis-cli DEL \
            "queue:{$c}" "admitted:{$c}" "maxscore:{$c}" \
            "grace:{$c}" "alive:{$c}" "dropfence:{$c}" >/dev/null 2>&1
    done
}

wait_idle() {
    local c state
    for c in $COUPONS; do
        state=""
        for _ in $(seq 1 30); do
            state=$($COMPOSE exec -T redis redis-cli HGET gw:snapshot "$c" 2>/dev/null)
            case "$state" in *:IDLE:*) break ;; *) state=""; sleep 1 ;; esac
        done
        [ -n "$state" ] || return 1
    done
    # 발행자가 IDLE 이어도 노드는 아직 아니다. 한 주기를 더 준다.
    sleep "${SNAPSHOT_SETTLE_SEC:-2}"
}

# 요약 읽기와 종료 코드 해석은 `peak-lib.sh` 가 든다 — 자기검증이 그것을 직접 잰다.

echo "현재 최대치 회차 · 사다리 [$RATES] · 회차당 $DURATION"

# **예열 회차는 표에 안 넣는다.** 갓 뜬 JVM 의 첫 회차는 응답 p99 가 한 자릿수
# 대신 세 자릿수로 나온다. 그 값이 표에 남으면 사다리의 첫 칸이 늘 제일 느리고,
# 응답 기준을 걸었을 때 천장이 예열 자리로 잡힌다 (O-5 가 같은 이유로 나왔다).
warm_rate=${WARMUP_RATE:-500}
warm_dur=${WARMUP_DURATION:-20s}
# **예열은 수렴할 때까지 되풀이한다.** 2 코어 한 대는 500/초 첫 예열을 p99 20초로 뒤집어써, 그 뒤 첫 칸이
# 예열 노릇을 했다.
warm_p99_ms=${WARMUP_P99_MS:-100}
warm_rounds=${WARMUP_ROUNDS:-5}
# 숫자가 아니면 횟수 비교가 늘 거짓이라 수렴 안 하는 예열이 끝없이 돈다.
case "$warm_rounds" in
    ''|*[!0-9]*|0*|??????????*) echo "::error title=현재 최대치::WARMUP_ROUNDS 는 아홉 자리 이하의 양의 정수여야 한다: '$warm_rounds'"; exit 2 ;;
esac
if [ "$warm_rate" != 0 ]; then
    warm_vus=${VUS:-$(peak_vus "$warm_rate" "$(peak_duration_sec "$warm_dur")")}
    if [ "$warm_vus" = 0 ]; then
        echo "::error title=현재 최대치::예열 '$warm_rate/초 · $warm_dur' 로 VU 풀을 못 잡는다"
        exit 2
    fi
    round=1
    while :; do
        echo "── 예열 ${warm_rate}/초 · ${warm_dur} · ${round} 번째 (표에 안 넣는다)"
        empty_queues
        wait_idle || { echo "::error title=현재 최대치::줄 모드가 안 꺼진다"; exit 2; }
        # 앞 번의 요약을 남기면 k6 가 못 뜬 번에 그 p99 를 읽는다.
        rm -f "$OUT_DIR/k6-warmup.json"
        VUS=$warm_vus RATE=$warm_rate DURATION=$warm_dur k6 run \
            --summary-export="$OUT_DIR/k6-warmup.json" test/load/peak.js \
            > "$OUT_DIR/k6-warmup.log" 2>&1
        peak_warm_converged "$OUT_DIR/k6-warmup.json" "$warm_p99_ms"
        case $? in
            0) break ;;
            # **예열이 돌았는지 본다.** 안 돌면 첫 회차가 갓 뜬 JVM 을 그대로 잰다.
            2) echo "::error title=현재 최대치::예열 회차가 안 돌았다 — 첫 회차가 예열을 뒤집어쓴다"; exit 2 ;;
        esac
        echo "    예열 p99 $(peak_summary_value "$OUT_DIR/k6-warmup.json" p99)ms — ${warm_p99_ms}ms 위라 한 번 더"
        if [ "$round" -ge "$warm_rounds" ]; then
            echo "::error title=현재 최대치::예열이 ${warm_rounds} 번에도 수렴 안 했다 — 예열 유입을 낮춘다"
            exit 2
        fi
        round=$((round + 1))
    done
fi

printf '# 요청유입\t실측유입\t판정\t응답p99ms\n' >> "$OUT_TABLE"

# **표집기는 정지 파일로 멈춘다.** 러너가 중간에 끝나면 루프가 남아 다음 실행의 호스트 유휴를 깎는다. 돌던
# 한 바퀴는 마치므로 k6 뒤 표본 한 벌이 붙는데, 그것은 판정기의 가운데 값이 흡수한다.
sampler=""
stop_sampler() {
    [ -n "$sampler" ] || return 0
    touch "$cpu.stop"
    wait "$sampler" 2>/dev/null
    sampler=""
}
trap stop_sampler EXIT
trap 'exit 130' INT TERM

for rate in $RATES; do
    echo "── 요청 유입 ${rate}/초"
    empty_queues
    if ! wait_idle; then
        echo "::error title=현재 최대치::줄 모드가 안 꺼진다 — 이 상태로는 못 잰다"
        exit 2
    fi

    before=$OUT_DIR/metrics-$rate-before.txt
    after=$OUT_DIR/metrics-$rate-after.txt
    summary=$OUT_DIR/k6-$rate.json
    log=$OUT_DIR/k6-$rate.log

    cpu=$OUT_DIR/cpu-$rate.tsv

    # 풀을 유입에 맞춘다. 고정 2000 이면 낮은 칸에서 폴링 갈래가 표 없이 돌아 임계가 깨진다.
    vus=${VUS:-$(peak_vus "$rate" "$DURATION_SEC")}
    if [ "$vus" = 0 ]; then
        echo "::error title=현재 최대치::유입 '$rate' 로 VU 풀을 못 잡는다 — 정수여야 한다"
        exit 2
    fi

    metrics "$before"
    # 정지 파일은 띄우기 전에 여기서 지운다. 자식이 지우면 곧바로 끝난 회차의 정지 신호를 먹는다.
    rm -f "$cpu.stop"
    peak_sample_cpu "$cpu" "$PROJECT" "$$" &
    sampler=$!
    VUS=$vus RATE=$rate DURATION=$DURATION k6 run --summary-export="$summary" \
        test/load/peak.js 2>&1 | tee "$log"
    k6_rc=${PIPESTATUS[0]}
    stop_sampler
    metrics "$after"

    # 천장 원인은 회차마다 남긴다. 사다리가 멈춘 칸의 것이 그 천장의 원인이다.
    LB_CPUS=$lb_env GATEWAYS=$GATEWAYS GATEWAY_CPUS=$bottleneck_cpus test/load/evaluate-bottleneck.sh "$cpu" \
        > "$OUT_DIR/bottleneck-$rate.txt" 2>&1
    sed 's/^/    /' "$OUT_DIR/bottleneck-$rate.txt"

    actual=$(peak_summary_value "$summary" rate)
    p99=$(peak_summary_value "$summary" p99)

    # **깨진 임계를 가려 읽는다.** 통째로 정상으로 읽으면 게이트웨이가 연결을
    # 끊은 회차가 `ok` 로 표에 남고, 그 수가 계획서로 간다.
    k6_verdict=$(peak_verdict_from_k6 "$k6_rc" "$summary")
    if [ -z "$actual" ] || [ -z "$p99" ]; then
        echo "  요약에서 값을 못 읽었다 — 이 회차는 판정 불가"
        printf '%s\t0\tunmeasurable\t0\n' "$rate" >> "$OUT_TABLE"
        stop_rate=${stop_rate:-$rate}
        break
    fi
    if [ "$k6_verdict" != ok ]; then
        echo "  k6 임계가 ${k6_verdict} 로 갈렸다 (종료 ${k6_rc})"
        printf '%s\t%s\t%s\t%s\n' "$rate" "$actual" "$k6_verdict" "$p99" >> "$OUT_TABLE"
        stop_rate=${stop_rate:-$rate}
        break
    fi

    # 판정 비율은 그 자가 낸다. 여기서 다시 셈하면 둘이 갈린다. **대마다 부르고 가장 나쁜 것을 쓴다.**
    verdicts=()
    : > "$OUT_DIR/judged-$rate.txt"
    for idx in $(seq 1 "$GATEWAYS"); do
        EXPECT_TOTAL=$(awk -v r="$rate" -v d="$DURATION_SEC" -v n="$GATEWAYS" 'BEGIN{ printf "%d", r * d / n }') \
            test/load/evaluate-judged.sh "$before.$idx" "$after.$idx" > "$OUT_DIR/judged-$rate.$idx.txt" 2>&1
        case $? in
            0) verdicts+=(ok) ;;
            1) verdicts+=(under) ;;
            *) verdicts+=(unmeasurable) ;;
        esac
        { echo "게이트웨이 $idx"; cat "$OUT_DIR/judged-$rate.$idx.txt"; } >> "$OUT_DIR/judged-$rate.txt"
    done
    verdict=$(peak_worst_verdict "${verdicts[@]}")
    sed 's/^/    /' "$OUT_DIR/judged-$rate.txt"

    printf '%s\t%s\t%s\t%s\n' "$rate" "$actual" "$verdict" "$p99" >> "$OUT_TABLE"

    # **천장 원인은 처음 안 선 칸의 것이다.** 사다리를 멈추지 않으면 맨 윗칸이 다른 회차의 원인을 낸다.
    stood=1
    if awk -v a="$actual" -v r="$rate" -v t="$TOLERANCE" 'BEGIN{ exit (a >= r * t) ? 1 : 0 }'; then
        echo "  하네스가 이 유입을 못 만들었다 (${actual}/${rate})"
        stood=0
    elif [ "$verdict" != ok ]; then
        echo "  이 회차가 안 섰다 (${verdict})"
        stood=0
    fi
    if [ "$stood" = 0 ]; then
        stop_rate=${stop_rate:-$rate}
        [ "$STOP_AT_CEILING" = 1 ] && { echo "  사다리를 멈춘다"; break; }
    fi
done

echo
if [ -n "${stop_rate:-}" ]; then
    echo "멈춘 칸의 $(grep -m1 '^원인' "$OUT_DIR/bottleneck-$stop_rate.txt" || echo '원인: 판정 불가')"
    # 증설 효율 판정기가 이 파일로 두 천장이 게이트웨이의 것인지, 멈춘 칸의 것인지 본다.
    { cat "$OUT_DIR/bottleneck-$stop_rate.txt"; echo "멈춘 칸: $stop_rate"; } > "$OUT_DIR/ceiling-cause.txt"
else
    echo "천장을 못 봐 원인 파일을 안 남긴다"
fi
test/load/evaluate-peak.sh "$OUT_TABLE"

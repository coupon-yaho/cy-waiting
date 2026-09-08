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
$COMPOSE up -d --wait --wait-timeout 240 || { echo "스택을 못 세웠다"; exit 2; }

rm -rf "$OUT_DIR"; mkdir -p "$OUT_DIR"
: > "$OUT_TABLE"

metrics() {
    $COMPOSE exec -T gateway wget -qO- http://localhost:8081/actuator/prometheus 2>/dev/null \
        > "$1"
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
if [ "$warm_rate" != 0 ]; then
    echo "── 예열 ${warm_rate}/초 · ${warm_dur} (표에 안 넣는다)"
    empty_queues
    wait_idle || { echo "::error title=현재 최대치::줄 모드가 안 꺼진다"; exit 2; }
    RATE=$warm_rate DURATION=$warm_dur k6 run \
        --summary-export="$OUT_DIR/k6-warmup.json" test/load/peak.js \
        > "$OUT_DIR/k6-warmup.log" 2>&1
fi

printf '# 요청유입\t실측유입\t판정\t응답p99ms\n' >> "$OUT_TABLE"

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

    metrics "$before"
    RATE=$rate DURATION=$DURATION k6 run --summary-export="$summary" \
        test/load/peak.js 2>&1 | tee "$log"
    k6_rc=${PIPESTATUS[0]}
    metrics "$after"

    actual=$(peak_summary_value "$summary" rate)
    p99=$(peak_summary_value "$summary" p99)

    # **깨진 임계를 가려 읽는다.** 통째로 정상으로 읽으면 게이트웨이가 연결을
    # 끊은 회차가 `ok` 로 표에 남고, 그 수가 계획서로 간다.
    k6_verdict=$(peak_verdict_from_k6 "$k6_rc" "$summary")
    if [ -z "$actual" ] || [ -z "$p99" ]; then
        echo "  요약에서 값을 못 읽었다 — 이 회차는 판정 불가"
        printf '%s\t0\tunmeasurable\t0\n' "$rate" >> "$OUT_TABLE"
        break
    fi
    if [ "$k6_verdict" != ok ]; then
        echo "  k6 임계가 ${k6_verdict} 로 갈렸다 (종료 ${k6_rc})"
        printf '%s\t%s\t%s\t%s\n' "$rate" "$actual" "$k6_verdict" "$p99" >> "$OUT_TABLE"
        break
    fi

    # 판정 비율은 그 자가 낸다. 여기서 다시 셈하면 둘이 갈린다.
    if EXPECT_TOTAL=$(awk -v r="$rate" -v d="$DURATION_SEC" 'BEGIN{ printf "%d", r * d }') \
            test/load/evaluate-judged.sh "$before" "$after" > "$OUT_DIR/judged-$rate.txt" 2>&1; then
        verdict=ok
    else
        case $? in
            1) verdict=under ;;
            *) verdict=unmeasurable ;;
        esac
    fi
    sed 's/^/    /' "$OUT_DIR/judged-$rate.txt"

    printf '%s\t%s\t%s\t%s\n' "$rate" "$actual" "$verdict" "$p99" >> "$OUT_TABLE"

    if [ "$STOP_AT_CEILING" = 1 ]; then
        if awk -v a="$actual" -v r="$rate" -v t="$TOLERANCE" \
                'BEGIN{ exit (a >= r * t) ? 1 : 0 }'; then
            echo "  하네스가 이 유입을 못 만들었다 (${actual}/${rate}) — 사다리를 멈춘다"
            break
        fi
        if [ "$verdict" != ok ]; then
            echo "  이 회차가 안 섰다 (${verdict}) — 사다리를 멈춘다"
            break
        fi
    fi
done

echo
test/load/evaluate-peak.sh "$OUT_TABLE"

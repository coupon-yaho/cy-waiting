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

COMPOSE="docker compose -f test/load/compose.yml"

# peak.js 가 두 쿠폰을 박아 두고 있다. 여기만 바꾸면 다른 쿠폰을 비우고 이 쿠폰을
# 때리게 된다 — 시나리오를 고칠 때 같이 고친다.
COUPONS="c1 c2"

# 사다리. 낮은 쪽에서부터 올린다 — 천장을 지나친 뒤의 값은 뜻이 없다.
RATES=${RATES:-"1000 2000 4000 8000 16000"}
DURATION=${DURATION:-30s}

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

# k6 요약에서 값을 뽑는다. 없으면 빈 문자열을 내고 부르는 쪽이 판정 불가로 읽는다.
from_summary() {
    python3 - "$1" "$2" <<'PY'
import json, sys
try:
    d = json.load(open(sys.argv[1]))
except Exception:
    sys.exit(0)
m = d.get('metrics', {})
if sys.argv[2] == 'rate':
    v = m.get('http_reqs', {}).get('rate')
else:
    v = m.get('http_req_duration', {}).get('p(99)')
if isinstance(v, (int, float)):
    print(f'{v:.4f}')
PY
}

# **잰 호스트를 같이 남긴다.** 천장 하나만 적으면 다음 사람이 다른 기계에서
# 나온 수와 견주게 되고, 이 페이즈가 되풀이한 오류가 정확히 그것이다.
{
    echo "코어: $(nproc 2>/dev/null || echo 모름)"
    echo "메모리: $(free -g 2>/dev/null | awk '/^Mem:/{print $2 " GB"}')"
    echo "k6: $(k6 version 2>/dev/null | head -1)"
    echo "사다리: $RATES · 회차당 $DURATION"
} > "$OUT_DIR/host.txt"
cat "$OUT_DIR/host.txt"

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

    actual=$(from_summary "$summary" rate)
    p99=$(from_summary "$summary" p99)

    # **k6 의 임계 위반(99)은 이 회차에서 정상이다.** 못 만든 유입이 있다는
    # 뜻이고, 그것이 곧 하네스 천장이라 표에 적어야 한다. 다른 코드는 다르다 —
    # 요약이 안 나왔거나 러너가 죽은 것이라 판정할 재료가 없다.
    if [ "$k6_rc" -ne 0 ] && [ "$k6_rc" -ne 99 ]; then
        echo "  k6 가 ${k6_rc} 로 끝났다 — 이 회차는 판정 불가"
        printf '%s\t%s\tunmeasurable\t%s\n' "$rate" "${actual:-0}" "${p99:-0}" >> "$OUT_TABLE"
        continue
    fi
    if [ -z "$actual" ] || [ -z "$p99" ]; then
        echo "  요약에서 값을 못 읽었다 — 이 회차는 판정 불가"
        printf '%s\t0\tunmeasurable\t0\n' "$rate" >> "$OUT_TABLE"
        continue
    fi

    # 판정 비율은 그 자가 낸다. 여기서 다시 셈하면 둘이 갈린다.
    if EXPECT_TOTAL=$(awk -v r="$rate" -v d="${DURATION%s}" 'BEGIN{ printf "%d", r * d }') \
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

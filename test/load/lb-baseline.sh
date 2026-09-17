#!/usr/bin/env bash
# 앞단 LB 기준선 사다리.
#
# **LB 뒤에 더미 응답만 두고 유입을 올린다.** 최대치 회차가 LB 를 지나면 LB 몫이 측정에 섞이는데, 그 몫을 따로
# 재 두지 않으면 느려진 것이 누구 탓인지 말할 수 없다. 표는 최대치 러너와 같은 형식이라 최대치 판정기가 읽는다.
#
# 이 스택은 끝나면 내린다. LB 경유 회차(`VIA_LB=1`)와 같은 18070 을 열어, 그 스택이 떠 있으면 올리기에서 끊긴다.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1

. test/load/peak-lib.sh || exit 2

COMPOSE="docker compose -p lb-baseline -f test/load/compose.lb-baseline.yml"

RATES=${RATES:-"2000 4000 8000 16000"}
DURATION=${DURATION:-30s}
DURATION_SEC=$(peak_duration_sec "$DURATION")
if [ "$DURATION_SEC" = 0 ]; then
    echo "::error title=LB 기준선::DURATION 을 못 읽는다 — 30s · 1m · 1m30s 꼴이어야 한다: '$DURATION'"; exit 2
fi
TOLERANCE=${PEAK_ARRIVAL_TOLERANCE:-0.95}
if ! printf '%s' "$TOLERANCE" | grep -Eq '^[0-9]+(\.[0-9]+)?$' \
        || ! awk -v t="$TOLERANCE" 'BEGIN{ exit (t > 0 && t <= 1) ? 0 : 1 }'; then
    echo "::error title=LB 기준선::허용 오차는 0 초과 1 이하의 수여야 한다: '$TOLERANCE'"; exit 2
fi

OUT_TABLE=${OUT_TABLE:-lb-steps.tsv}
case "$OUT_TABLE" in
    *.tsv) ;;
    *) echo "::error title=LB 기준선::OUT_TABLE 은 .tsv 여야 한다: '$OUT_TABLE'"; exit 2 ;;
esac
OUT_DIR=${OUT_DIR:-lb-out}

command -v k6 >/dev/null || { echo "::error title=LB 기준선::k6 가 없다"; exit 2; }

# 표집기는 정지 파일로 멈추고, 끝나면 스택을 내린다.
sampler="" cpu=""
cleanup() {
    if [ -n "$sampler" ]; then touch "$cpu.stop"; wait "$sampler" 2>/dev/null; fi
    $COMPOSE down -v >/dev/null 2>&1
}
trap cleanup EXIT
trap 'exit 130' INT TERM
$COMPOSE up -d --wait --wait-timeout 60 || { echo "::error title=LB 기준선::LB 를 못 세웠다"; exit 2; }

rm -rf "$OUT_DIR"; mkdir -p "$OUT_DIR"
printf '# 요청유입\t실측유입\t판정\t응답p99ms\n' > "$OUT_TABLE"

echo "LB 기준선 · 사다리 [$RATES] · 회차당 $DURATION"

for rate in $RATES; do
    echo "── 요청 유입 ${rate}/초"
    vus=${VUS:-$(peak_vus "$rate" "$DURATION_SEC")}
    if [ "$vus" = 0 ]; then
        echo "::error title=LB 기준선::유입 '$rate' 로 VU 풀을 못 잡는다 — 정수여야 한다"
        exit 2
    fi
    summary=$OUT_DIR/k6-$rate.json
    # **기준선도 CPU 를 남긴다.** 경유 회차는 게이트웨이·레디스와 호스트를 나눠, 같은 유입이라도 LB 가 받는
    # 경합이 다르다. 뺀 값을 읽을 때 그 차이를 볼 재료다.
    cpu=$OUT_DIR/cpu-$rate.tsv
    rm -f "$cpu.stop"
    peak_sample_cpu "$cpu" lb-baseline "$$" "$(( $(date +%s) + DURATION_SEC ))" &
    sampler=$!
    VUS=$vus RATE=$rate DURATION=$DURATION k6 run --summary-export="$summary" \
        test/load/lb-baseline.js > "$OUT_DIR/k6-$rate.log" 2>&1
    k6_rc=$?
    touch "$cpu.stop"
    wait "$sampler" 2>/dev/null
    sampler=""
    awk -F '\t' '
        function med(k,    i, j, t, n) { n = c[k]; for (i = 2; i <= n; i++) { t = v[k, i]; for (j = i - 1; j >= 1 && v[k, j] > t; j--) v[k, j + 1] = v[k, j]; v[k, j + 1] = t }
            return n ? ((n % 2) ? v[k, (n + 1) / 2] : (v[k, n / 2] + v[k, n / 2 + 1]) / 2) : -1 }
        $1 == "cpu" && $2 ~ /-lb-[0-9]+$/     { v["lb", ++c["lb"]] = $3 }
        $1 == "cpu" && $2 ~ /-origin-[0-9]+$/ { v["origin", ++c["origin"]] = $3 }
        $1 == "idle"                          { v["idle", ++c["idle"]] = $3 }
        END { printf "  LB CPU 가운데 %.1f%% · origin CPU 가운데 %.1f%% · 호스트 유휴 가운데 %.1f%%\n", med("lb"), med("origin"), med("idle") }
    ' "$cpu"

    actual=$(peak_summary_value "$summary" rate)
    p99=$(peak_summary_value "$summary" p99)
    if [ -z "$actual" ] || [ -z "$p99" ]; then
        echo "  요약에서 값을 못 읽었다 — 이 회차는 판정 불가"
        printf '%s\t0\tunmeasurable\t0\n' "$rate" >> "$OUT_TABLE"
        break
    fi
    # 드롭만 깨진 칸은 실측으로 가른다. 실패 응답이 섞이면 LB 가 못 받은 것이다.
    verdict=$(peak_verdict_from_k6 "$k6_rc" "$summary")
    # 걸린 요청이 회차를 늘린 칸은 생성기 한계가 아니다 — 파일 한도가 기준선을 반으로 자른 모양이다. 뒤에 제품이
    # 없는 이 러너에서만 계기 탓으로 친다. 최대치 러너에서는 게이트웨이가 요청을 붙잡은 것일 수 있다.
    if peak_hung "$summary" "$rate" "$DURATION_SEC" "$TOLERANCE"; then
        echo "  요청이 끝나지 않아 회차가 늘었다 — 판정 불가"
        verdict=unmeasurable
    fi
    printf '%s\t%s\t%s\t%s\n' "$rate" "$actual" "$verdict" "$p99" >> "$OUT_TABLE"
    echo "  실측 ${actual}/초 · p99 ${p99}ms · ${verdict}"

    if [ "$verdict" != ok ] \
            || awk -v a="$actual" -v r="$rate" -v t="$TOLERANCE" 'BEGIN{ exit (a >= r * t) ? 1 : 0 }'; then
        echo "  이 칸이 안 섰다 — 사다리를 멈춘다"
        break
    fi
done

echo
test/load/evaluate-peak.sh "$OUT_TABLE"

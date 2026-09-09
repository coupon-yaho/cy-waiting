#!/usr/bin/env bash
# 두 전략을 실제로 가르는 회차.
#
# **기존 회차는 한쪽에 답안지를 줬다.** `routing-skew.sh` 는 뒷단 셋에 지연을
# 똑같이 주고 동시 한도도 똑같이 준 뒤, 레디스에 적힌 숫자만 다르게 실었다.
# 그러고 "선언된 비율대로 갔는가" 를 판정 기준으로 삼았다 — 가중 라운드로빈은
# 정의상 그 숫자를 재생하므로 편차 0% 가 나올 수밖에 없고, P2C 는 실제로
# 물린 건수를 보고 균등화하니 그만큼 벗어난 것으로 찍힌다. **세 뒷단이
# 물리적으로 같은 서버라 검증할 가용량 차이가 아예 없었다.**
#
# 여기서는 반대로 둔다. **보고 여유는 셋 다 같고, 실제 능력만 한 대가 다르다.**
# 그러면 선언된 비율은 정보가 아니고, 실제 부하를 보는 쪽만 그것을 알 수 있다.
#
# 재는 것도 바꾼다. 도착 건수가 아니라 **뒷단이 실제로 처리한 것과 밀어낸
# 것**이다 — 약한 대가 무너지는지가 이 회차의 질문이다.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1

# **셋 다 같은 여유를 보고한다.** 이것이 이 회차의 핵심이다.
BIG_CAP="${BIG_CAP:-120}"
SMALL_CAP="${SMALL_CAP:-120}"
MID_CAP="${MID_CAP:-120}"

# 실제 능력만 다르다. 큰 대(backend)가 세 배 느리다 — 같은 도착이면 물린 건수가
# 세 배가 되고, 동시 한도에 먼저 닿아 밀어내기 시작한다.
STUB_LATENCY_MS="${STUB_LATENCY_MS:-500}"
BIG_LATENCY_MS="${BIG_LATENCY_MS:-1500}"
export BIG_LATENCY_MS

. test/load/routing-lib.sh || exit 2

RATE="${RATE:-300}"
DURATION_SEC="${DURATION_SEC:-30}"
WARMUP_SEC="${WARMUP_SEC:-10}"

require_positive_int RATE DURATION_SEC STUB_LATENCY_MS BIG_LATENCY_MS || exit 2

# **느린 배수가 정수여야 한다.** 판정기에 넘길 때 정수 나눗셈으로 자르는데,
# 900/500 은 1.8 인데 1 로 잘려 판정 기준이 통째로 달라진다. 나누어떨어지지
# 않으면 회차를 시작하지 않는다 — 잘린 배수로 낸 판정은 아무 뜻이 없다.
if [ $((BIG_LATENCY_MS % STUB_LATENCY_MS)) -ne 0 ] \
        || [ "$BIG_LATENCY_MS" -le "$STUB_LATENCY_MS" ]; then
    echo "느린 대 지연 ${BIG_LATENCY_MS}ms 는 정상 ${STUB_LATENCY_MS}ms 의 2 배 이상 정수배여야 한다"
    exit 2
fi

# **유입이 보고 여유 합을 넘으면 이 회차는 성립하지 않는다.** 넘는 순간 대기열이
# 켜져 요청이 뒷단까지 안 가고, 그러면 뒷단 계수로는 아무것도 못 읽는다 —
# 실제로 그 상태에서 처리 합이 289 건까지 떨어졌다. 여기서 재는 것은 통과
# 경로의 나눔이므로 줄이 꺼진 채로 돌아야 한다.
if [ "$RATE" -ge $((BIG_CAP + SMALL_CAP + MID_CAP)) ]; then
    echo "유입 ${RATE}/s 가 보고 여유 합 $((BIG_CAP + SMALL_CAP + MID_CAP)) 이상이다 — 줄이 켜져 못 잰다"
    exit 2
fi
require_non_negative_int WARMUP_SEC || exit 2
command -v k6 >/dev/null || { echo "k6 가 없다"; exit 2; }

# 동시 한도는 셋 다 같게 둔다. **한도까지 같아야** 다른 것이 지연 하나뿐이다.
export BIG_INFLIGHT="${BIG_INFLIGHT:-150}" SMALL_INFLIGHT="${SMALL_INFLIGHT:-150}" \
       MID_INFLIGHT="${MID_INFLIGHT:-150}" \
       ROUTING_PER_INSTANCE_CAP="${ROUTING_PER_INSTANCE_CAP:-500}"

work=$(mktemp -d) || exit 1
trap 'reap_children; rm -rf "$work"' EXIT

# **보고 여유를 그대로 적는다.** "셋 다" 라고 적어 두면 값이 다른 회차에서
# 거짓말이 된다 — 이 하네스의 요점이 보고와 실제의 어긋남이라 더 나쁘다.
echo "$(banner) · 보고 여유 ${BIG_CAP}/${SMALL_CAP}/${MID_CAP}" \
     "· 실제 지연 ${STUB_LATENCY_MS}ms (느린 대 ${BIG_LATENCY_MS}ms)"

$COMPOSE rm -sf gateway >> "$work/up.log" 2>&1
bring_up "$work/up.log" || exit 2
wait_for_ramp || exit 2
wait_for_idle_queue || exit 2

# **앞 회차 산출물을 먼저 지운다.** k6 가 요약을 못 남기고 죽으면 아래 복사가
# 조용히 실패하고, 앞 회차 요약이 이번 표와 짝지어진다.
rm -f "${OUT_RESULT:-routing-truth.txt}" "${OUT_SUMMARY:-routing-truth-k6.json}"       "${OUT_LOG:-routing-truth-k6.log}"

sleep "$WARMUP_SEC"

# 뒷단 이름은 `routing-lib.sh` 의 `NAMES` 를 쓴다. 손으로 적으면 한 곳만 바뀐다.
before_ok=(); before_bad=()
for name in "${NAMES[@]}"; do
    before_ok+=("$(served "$name")") || exit 2
    before_bad+=("$(counter "$name" rejected)") || exit 2
done

rc=0
RATE="$RATE" DURATION_SEC="$DURATION_SEC" BASE_URL="$GATEWAY" COUPON="$COUPON" \
    k6 run --summary-export="$work/k6.json" test/load/routing-ratio-rate.js \
    > "$work/k6.log" 2>&1 || rc=$?
echo "k6=$rc"

# **흘린 회차는 이 시나리오에서 예상된 신호다.** VU 수가 고정인데 뒷단이 느리면
# 고정 유입 실행기가 회차를 못 시작하고 흘린다 — 두 전략을 가르는 신호가 바로
# 그것이다. 그래서 그 임계 하나만 깨진 것은 받아들인다.
#
# **나머지는 안 받는다.** 다른 임계가 깨졌거나 k6 가 다른 이유로 죽었으면 그
# 회차의 표는 재려던 것이 아니다. 종료 코드를 그냥 버리면 응답의 절반이
# 비정상인 회차로 결론을 내게 된다 — 같은 구멍을 이 저장소가 한 번 밟았다.
breached=$(jq -r '[.metrics | to_entries[]
    | select(.value.thresholds != null)
    | select([.value.thresholds[]] | any(. == true))
    | .key] | join(",")' "$work/k6.json" 2>/dev/null)
if [ "$breached" != "" ] && [ "$breached" != "dropped_iterations" ]; then
    echo "::error title=라우팅 비교::깨진 임계가 흘린 회차 말고도 있다: $breached"
    exit 1
fi
if [ "$rc" -ne 0 ] && [ "$rc" -ne 99 ]; then
    echo "::error title=라우팅 비교::k6 가 ${rc} 로 끝났다 — 이 회차로는 비교하지 않는다"
    exit 1
fi

sleep "$(awk -v ms="$BIG_LATENCY_MS" 'BEGIN{printf "%.1f", ms/1000 + 1.5}')"

# **읽기 실패를 안 보면 0 이 된다.** 빈 문자열이 뺄셈에서 0 으로 읽혀, 그 뒷단이
# 한 건도 처리 못 한 것으로 표에 찍힌다.
: > "$work/result.txt"
labels=(느린대 정상1 정상2)
for i in "${!NAMES[@]}"; do
    ok=$(served "${NAMES[i]}") || exit 2
    bad=$(counter "${NAMES[i]}" rejected) || exit 2
    printf '%s %s %s\n' "$((ok - before_ok[i]))" "$((bad - before_bad[i]))" \
        "${labels[i]}" >> "$work/result.txt"
done

echo
printf '  %-8s %10s %10s\n' "뒷단" "처리" "밀어냄"
while read -r ok bad name; do
    printf '  %-8s %10s %10s\n' "$name" "$ok" "$bad"
done < "$work/result.txt"

# **나눈 것만 보면 절반이다.** 느린 대에 안 보낸 몫이 정상 대로 갔는지, 아니면
# 게이트웨이가 흘려버렸는지가 갈린다 — 뒤엣것이면 쏠림을 고치면서 처리량을
# 깎은 것이다. 총합과 응답 분포를 같이 남긴다.
total=$(awk '{s+=$1} END {print s}' "$work/result.txt")
printf '  %-8s %10s\n' "처리 합" "$total"
# **흘린 회차도 같이 낸다.** 처리 합만 보면 두 전략의 제안 부하가 같았는지를
# 못 본다 — 처리와 흘림을 더한 값이 같아야 비교가 성립한다.
dropped=$(jq -r '(.metrics.dropped_iterations.values.count
    // .metrics.dropped_iterations.count) // 0' "$work/k6.json" 2>/dev/null)
printf '  %-8s %10s\n' "흘린 회차" "$dropped"
printf '  %-8s %10s\n' "제안 합" "$((total + dropped))"
grep -E 'http_reqs\.\.|http_req_failed|http_req_duration\.\.|checks_succ' "$work/k6.log" \
    | sed 's/^/  /'

# **산출물 복사가 실패하면 판정하지 않는다.** 실패를 넘기면 앞 회차 파일이
# 남아 있을 때 그것으로 판정이 난다.
cp "$work/result.txt" "${OUT_RESULT:-routing-truth.txt}" || exit 2
cp "$work/k6.json" "${OUT_SUMMARY:-routing-truth-k6.json}" || exit 2
# 로그는 기록용이라 없어도 판정은 선다.
cp "$work/k6.log" "${OUT_LOG:-routing-truth-k6.log}" 2>/dev/null

# **판정기를 여기서 부른다.** 표만 내고 끝내면 사람이 눈으로 읽어 결론을 내는데,
# 그 결론이 계획서와 결정 문서에 인용된다. 판정은 자기검증이 붙은 자가 낸다.
echo
SLOW_FACTOR=$((BIG_LATENCY_MS / STUB_LATENCY_MS))     test/load/evaluate-routing-truth.sh     "${OUT_RESULT:-routing-truth.txt}" "${OUT_SUMMARY:-routing-truth-k6.json}"

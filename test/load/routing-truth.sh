#!/usr/bin/env bash
# 두 전략을 실제로 가르는 회차.
#
# **기존 회차는 한쪽에 답안지를 줬다.** `routing-skew.sh` 는 뒷단 셋에 지연을
# 똑같이 주고 동시 한도도 똑같이 준 뒤, 레디스에 적힌 숫자만 다르게 실었다.
# 그러고 "선언된 비율대로 갔는가" 를 판정 기준으로 삼았다 — 라운드로빈은
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
require_non_negative_int WARMUP_SEC || exit 2
command -v k6 >/dev/null || { echo "k6 가 없다"; exit 2; }

# 동시 한도는 셋 다 같게 둔다. **한도까지 같아야** 다른 것이 지연 하나뿐이다.
export BIG_INFLIGHT="${BIG_INFLIGHT:-150}" SMALL_INFLIGHT="${SMALL_INFLIGHT:-150}" \
       MID_INFLIGHT="${MID_INFLIGHT:-150}" \
       ROUTING_PER_INSTANCE_CAP="${ROUTING_PER_INSTANCE_CAP:-500}"

work=$(mktemp -d) || exit 1
trap 'reap_children; rm -rf "$work"' EXIT

echo "$(banner) · 보고 여유 셋 다 ${BIG_CAP} · 실제 지연 ${STUB_LATENCY_MS}ms (큰 대 ${BIG_LATENCY_MS}ms)"

$COMPOSE rm -sf gateway >> "$work/up.log" 2>&1
bring_up "$work/up.log" || exit 2
wait_for_ramp || exit 2
wait_for_idle_queue || exit 2

before_ok=$(served backend) || exit 2
before_bad=$(counter backend rejected) || exit 2
b2_ok=$(served backend-small) || exit 2
b2_bad=$(counter backend-small rejected) || exit 2
b3_ok=$(served backend-mid) || exit 2
b3_bad=$(counter backend-mid rejected) || exit 2

RATE="$RATE" DURATION_SEC="$DURATION_SEC" BASE_URL="$GATEWAY" COUPON="$COUPON" \
    k6 run --summary-export="$work/k6.json" test/load/routing-ratio-rate.js \
    > "$work/k6.log" 2>&1
echo "k6=$?"

sleep "$(awk -v ms="$BIG_LATENCY_MS" 'BEGIN{printf "%.1f", ms/1000 + 1.5}')"

after_ok=$(served backend); after_bad=$(counter backend rejected)
a2_ok=$(served backend-small); a2_bad=$(counter backend-small rejected)
a3_ok=$(served backend-mid); a3_bad=$(counter backend-mid rejected)

printf '%s %s %s\n' "$((after_ok - before_ok))" "$((after_bad - before_bad))" "느린대" \
    > "$work/result.txt"
printf '%s %s %s\n' "$((a2_ok - b2_ok))" "$((a2_bad - b2_bad))" "정상1" >> "$work/result.txt"
printf '%s %s %s\n' "$((a3_ok - b3_ok))" "$((a3_bad - b3_bad))" "정상2" >> "$work/result.txt"

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
grep -E 'http_reqs\.\.|http_req_failed|http_req_duration\.\.|checks_succ' "$work/k6.log" \
    | sed 's/^/  /'

cp "$work/result.txt" "${OUT_RESULT:-routing-truth.txt}"
cp "$work/k6.json" "${OUT_SUMMARY:-routing-truth-k6.json}" 2>/dev/null
cp "$work/k6.log" "${OUT_LOG:-routing-truth-k6.log}" 2>/dev/null

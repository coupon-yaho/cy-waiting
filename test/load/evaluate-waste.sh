#!/usr/bin/env bash
# G7.5 판정 — **하네스가 모은 것을 받아 판정만 한다.**
#
# 스택과 갈라 놓는다. 판정이 드라이버 안에 있으면 그것을 검증하려고 매번
# 도커와 k6 를 15분씩 돌려야 하고, 그러면 아무도 안 고친다. 조작한 입력으로
# 몇 초에 도는 자기검증이 `gate-selftest.sh` 에 있다.
#
# **모르는 것을 아는 쪽으로 안 접는다.** 이 판정은 한 번 상수 하나 때문에
# 통과와 미달을 오갔다. 접을 수 없는 것은 미판정으로 뺀다 — 통과도 미달도
# 아닌 답이 있어야 판정이 값에 걸린다.
#
# 인자
#   $1  임계 표본       "<레디스 초> <임계 score>" 줄들. 단조 증가여야 한다
#   $2  남은 유령       "<memberId>\t<줄 score>" 줄들
#   $3  차례를 준 인원
#   $4  걷은 수
#   $5  이탈한 수
#   $6  생존 신호 수명(초)
#   $7  시계 바닥값이 걸린 횟수 (선택). 0 이 아니면 미판정
set -uo pipefail

samples="${1:?임계 표본 파일}"
ghosts="${2:?유령 파일}"
admitted_total="${3:?차례를 준 인원}"
swept="${4:?걷은 수}"
abandoned="${5:?이탈한 수}"
alive_ttl="${6:?생존 신호 수명}"
clock_floor="${7:-0}"

WASTE_MAX="${WASTE_MAX:-0.05}"
MIN_ADMITTED="${MIN_ADMITTED:-300}"

# **못 되짚은 유령이 이 비율을 넘으면 판정을 안 한다.** 표본 밖은 "괜찮다" 가
# 아니라 "모른다" 다. 상한이 없으면 알리바이 한 명만 두고 나머지가 전부
# 표본 밖인 실행이 통과한다 — 스위퍼가 한 명도 안 걷어도.
UNKNOWN_MAX="${UNKNOWN_MAX:-0.05}"

# **스위퍼에게 기회가 있었던 인원의 하한.**
#
# "못 피하는 유령이 있는가" 를 묻는 것은 방향이 반대다 — 실행이 퇴화해서 아무도
# 수명을 넘겨 안 기다리면 그 부류만 남아 검사가 만족된다. 크레딧을 올려 줄이
# 빨리 빠지게 하면 스위퍼를 통째로 꺼도 통과한다.
#
# 기회가 있었던 인원 = 걷은 것 + 걷었어야 하는데 못 걷은 것.
MIN_SWEEPABLE="${MIN_SWEEPABLE:-100}"

# 배분 틱. 임계가 유령을 지나는 틱에는 이미 창 밖이라 스위퍼는 그 **이전**
# 틱에 걷었어야 한다. 결함이라고 주장하는 쪽에 이만큼 여유를 준다.
TICK_SEC="${TICK_SEC:-1}"

fail() { echo "::error title=G7.5 미판정::$1"; exit 1; }

whole() { [[ "${1:-}" =~ ^[0-9]+$ ]]; }

for pair in "차례를 준 인원:$admitted_total" "걷은 수:$swept" \
            "이탈한 수:$abandoned" "생존 신호 수명:$alive_ttl"; do
  whole "${pair#*:}" || fail "${pair%%:*} 가 음이 아닌 정수가 아니다: ${pair#*:}"
done
[ "$alive_ttl" -gt 0 ] || fail "생존 신호 수명이 0 이다"
[ -s "$samples" ] || fail "임계 표본이 비었다 — 유령이 언제 차례를 받았는지 모른다"
[ -f "$ghosts" ] || fail "유령 파일이 없다: $ghosts"

# **시계가 뒤로 갔으면 score 가 시각이 아니다.** `enqueue.lua` 가 바닥값+1 을
# 쓰므로(A-9) 그 항목의 score 는 줄 선 시각보다 크고, 되짚으면 기다린 시간이
# 짧게 나와 결함이 못 피하는 것으로 넘어간다 — 통과 쪽으로 틀린다.
#
# 추측하지 않고 잰다. 바닥값이 걸린 횟수는 게이트웨이가 지표로 낸다.
whole "$clock_floor" || fail "시계 바닥값 횟수가 정수가 아니다: $clock_floor"
[ "$clock_floor" -eq 0 ] \
    || fail "시계 바닥값이 $clock_floor 번 걸렸다 — 줄 score 가 줄 선 시각이 아니다"

# 유령 하나의 기다린 시간 = (임계가 그를 지난 시각) − (그가 줄 선 시각).
# 줄 score 가 곧 줄 선 시각(μs)이라 둘 다 실측이다.
counts=$(awk -v ttl="$alive_ttl" -v tick="$TICK_SEC" '
    function bail(msg) { print "BAD " msg; exit 3 }
    FILENAME == ARGV[1] {
        if ($1 !~ /^[0-9]+$/ || $2 !~ /^[0-9]+$/) bail("임계 표본이 숫자가 아니다: " $0)
        # **단조성을 본다.** stale read 로 값이 한 번 뒤로 가면 그 시각이
        # 채택되어 기다린 시간이 짧아진다 — 통과 쪽으로 틀린다.
        if (n > 0 && $2 + 0 < th[n]) bail("임계 표본이 뒤로 갔다")
        t[++n] = $1 + 0; th[n] = $2 + 0
        next
    }
    NF == 0 { next }
    {
        if ($2 !~ /^[0-9]+$/) bail("유령의 줄 score 가 숫자가 아니다: " $0)
        joinedAt = $2 / 1000000
        admittedAt = -1
        for (i = 1; i <= n; i++) {
            if (th[i] >= $2 + 0) { admittedAt = t[i]; idx = i; break }
        }
        total++
        # **양쪽 절단을 다 뺀다.** 표본 뒤로 지나간 것도, 첫 표본 때 이미
        # 지나가 있던 것도 언제 지났는지 모른다. 한쪽만 빼면 그 방향으로 틀린다.
        if (admittedAt < 0 || idx == 1) { unknown++; next }
        # **여유를 준다.** 표본은 실제 교차보다 늦게 잡히므로 직전 표본을
        # 하한으로 쓰고, 배분 뒤에 청소가 도는 한 틱도 빼 준다. 결함이라고
        # 주장하는 쪽을 보수적으로 잡는다.
        if (t[idx - 1] - joinedAt >= ttl + tick) avoidable++; else unavoidable++
    }
    END { printf "%d %d %d %d", total, avoidable, unavoidable, unknown }
    ' "$samples" "$ghosts")
awk_rc=$?

case "$counts" in BAD\ *) fail "${counts#BAD }" ;; esac
[ "$awk_rc" -eq 0 ] || fail "유령을 되짚지 못했다 (awk $awk_rc)"
read -r total avoidable unavoidable unknown <<<"$counts"
for pair in "유령 수:$total" "피할 수 있었던 것:$avoidable" \
            "못 피하는 것:$unavoidable" "표본 밖:$unknown"; do
  whole "${pair#*:}" || fail "${pair%%:*} 를 못 셌다: '${pair#*:}'"
done

waste=$(awk -v a="$avoidable" -v t="$admitted_total" \
    'BEGIN { printf "%.6f", (t > 0 ? a / t : -1) }')

printf '  %-26s %s\n' "임계 아래 남은 유령" "$total"
printf '  %-26s %s\n' "  피할 수 있었던 것" "$avoidable"
printf '  %-26s %s\n' "  못 피하는 것(수명 안)" "$unavoidable"
printf '  %-26s %s\n' "  못 되짚은 것" "$unknown"
printf '  %-26s %s\n' "피할 수 있었던 낭비" "$waste"

# **차례가 충분히 지나가야 잰 것이다.** 몇 명뿐이면 한 사람이 비율을 흔든다.
[ "$admitted_total" -ge "$MIN_ADMITTED" ] \
    || fail "차례가 $admitted_total 명뿐이다 — 실행이 짧거나 크레딧이 안 나갔다"
# **못 피하는 유령이 있어야 기구를 잰 실행이다.** 하나도 없으면 이탈이 안
# 일어났거나 실행이 수명보다 짧아, 기구가 아무 일도 안 하고 통과한다.
sweepable=$((avoidable + swept))
[ "$sweepable" -ge "$MIN_SWEEPABLE" ] \
    || fail "스위퍼에게 기회가 있었던 인원이 $sweepable 명뿐이다 — 이 실행은 기구를 안 잰다"
awk -v u="$unknown" -v t="$total" -v m="$UNKNOWN_MAX" \
    'BEGIN { exit (t == 0 || u / t <= m) ? 0 : 1 }' \
    || fail "$total 명 중 $unknown 명을 못 되짚었다 — 표본이 실행을 못 덮는다"

failed=0
# **살아 있는 사람을 걷었는가.** 이탈한 적 없는 사람은 걷힐 수 없다 — 최대
# 폴링 간격이 수명보다 짧아 성실한 폴러의 신호는 안 만료된다. 넘으면 순번
# 역행이고, 그건 놓치는 것보다 나쁘다.
if [ "$swept" -gt "$abandoned" ]; then
  echo "::error title=G7.5 미달::걷은 수 $swept 가 이탈한 수 $abandoned 를 넘었다 — 줄 선 사람을 걷었다"
  failed=1
fi
if awk -v w="$waste" -v m="$WASTE_MAX" 'BEGIN { exit (w >= 0 && w <= m) ? 0 : 1 }'; then
  echo "G7.5 통과 — 피할 수 있었던 낭비 $waste <= $WASTE_MAX"
else
  echo "::error title=G7.5 미달::피할 수 있었던 낭비 $waste > $WASTE_MAX ($avoidable 명)"
  failed=1
fi

exit "$failed"

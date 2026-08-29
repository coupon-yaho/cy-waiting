#!/usr/bin/env bash
# G7.5 판정 — **하네스가 모은 것을 받아 판정만 한다.**
#
# 스택과 갈라 놓는다. 판정이 드라이버 안에 있으면 그것을 검증하려고 매번
# 도커와 k6 를 15분씩 돌려야 하고, 그러면 아무도 안 고친다. 조작한 입력으로
# 몇 초에 도는 자기검증이 `gate-selftest.sh` 에 있다.
#
# 인자
#   $1  임계 표본       "<레디스 초> <임계 score>" 줄들
#   $2  남은 유령       "<memberId>\t<줄 score>" 줄들
#   $3  차례를 준 인원
#   $4  걷은 수
#   $5  이탈한 수
#   $6  생존 신호 수명(초)
set -uo pipefail

samples="${1:?임계 표본 파일}"
ghosts="${2:?유령 파일}"
admitted_total="${3:?차례를 준 인원}"
swept="${4:?걷은 수}"
abandoned="${5:?이탈한 수}"
alive_ttl="${6:?생존 신호 수명}"

WASTE_MAX="${WASTE_MAX:-0.05}"
MIN_ADMITTED="${MIN_ADMITTED:-300}"

numeric() { [[ "${1:-}" =~ ^-?[0-9]+([.][0-9]+)?$ ]]; }

for v in "$admitted_total" "$swept" "$abandoned" "$alive_ttl"; do
  if ! numeric "$v"; then
    echo "::error title=G7.5 미판정::숫자가 아닌 입력: $v"
    exit 1
  fi
done
if [ ! -s "$samples" ]; then
  echo "::error title=G7.5 미판정::임계 표본이 비었다 — 유령이 언제 차례를 받았는지 모른다"
  exit 1
fi

# 유령 하나의 기다린 시간 = (임계가 그를 지난 시각) − (그가 줄 선 시각).
# 줄 score 가 곧 줄 선 시각(μs)이라 둘 다 실측이다.
read -r total avoidable unavoidable unknown < <(awk -v ttl="$alive_ttl" '
    FILENAME == ARGV[1] { t[++n] = $1 + 0; th[n] = $2 + 0; next }
    NF == 0 { next }
    {
        joinedAt = $2 / 1000000
        admittedAt = -1
        for (i = 1; i <= n; i++) {
            if (th[i] >= $2 + 0) { admittedAt = t[i]; break }
        }
        total++
        # **표본 밖에서 지나간 것은 셈에서 뺀다.** 모르는 것을 아는 쪽으로
        # 접으면 판정이 표본 간격에 흔들린다.
        if (admittedAt < 0) { unknown++; next }
        if (admittedAt - joinedAt >= ttl) avoidable++; else unavoidable++
    }
    END { printf "%d %d %d %d", total, avoidable, unavoidable, unknown }
    ' "$samples" "$ghosts")

waste=$(awk -v a="$avoidable" -v t="$admitted_total" \
    'BEGIN { printf "%.6f", (t > 0 ? a / t : -1) }')

printf '  %-26s %s\n' "임계 아래 남은 유령" "$total"
printf '  %-26s %s\n' "  피할 수 있었던 것" "$avoidable"
printf '  %-26s %s\n' "  못 피하는 것(수명 안)" "$unavoidable"
printf '  %-26s %s\n' "  표본 밖" "$unknown"
printf '  %-26s %s\n' "피할 수 있었던 낭비" "$waste"

failed=0
# **차례가 충분히 지나가야 잰 것이다.** 몇 명뿐이면 한 사람이 비율을 흔든다.
if ! awk -v a="$admitted_total" -v m="$MIN_ADMITTED" \
    'BEGIN { exit (a >= m) ? 0 : 1 }'; then
  echo "::error title=G7.5 미판정::차례가 $admitted_total 명뿐이다 — 판이 짧거나 크레딧이 안 나갔다"
  failed=1
fi
# **못 피하는 유령이 있어야 판이 제 모양이다.** 하나도 없으면 이탈이 안
# 일어났거나 판이 수명보다 짧아, 기구가 아무 일도 안 하고 통과한다.
if [ "$unavoidable" -lt 1 ]; then
  echo "::error title=G7.5 미판정::수명 안에 차례가 온 유령이 없다 — 이 판은 기구를 안 잰다"
  failed=1
fi
# **살아 있는 사람을 걷었는가.** 이탈한 적 없는 사람은 걷힐 수 없다 — 최대
# 폴링 간격이 수명보다 짧아 성실한 폴러의 신호는 안 만료된다. 넘으면 순번
# 역행이고, 그건 놓치는 것보다 나쁘다.
if [ "$swept" -gt "$abandoned" ]; then
  echo "::error title=G7.5 미달::걷은 수 $swept 가 이탈한 수 $abandoned 를 넘었다 — 줄 선 사람을 걷었다"
  failed=1
fi
if awk -v w="$waste" -v m="$WASTE_MAX" 'BEGIN { exit (w >= 0 && w <= m) ? 0 : 1 }'; then
  # 앞의 미판정 검사가 걸렸으면 통과라고 적지 않는다 — 둘이 같이 찍히면
  # 로그를 훑는 사람이 통과로 읽는다.
  [ "$failed" -eq 0 ] && echo "G7.5 통과 — 피할 수 있었던 낭비 $waste <= $WASTE_MAX"
else
  echo "::error title=G7.5 미달::피할 수 있었던 낭비 $waste > $WASTE_MAX ($avoidable 명)"
  failed=1
fi

exit "$failed"

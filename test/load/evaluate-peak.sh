#!/usr/bin/env bash
# 현재 최대치 (G10.10 의 기준).
#
# **100K 를 만들 수단이 없다.** 목표 수를 게이트로 쓰면 만든 적 없는 부하를
# 판정하게 된다. 그래서 이 자는 목표를 재지 않고 **지금 실제로 선 최대치를
# 기록**한다 (90-decisions O-8).
#
# **천장의 종류를 가른다.** 하네스가 못 만든 것과 게이트웨이가 못 버틴 것은
# 다음에 할 일이 다르다 — 앞엣것은 생성기를, 뒤엣것은 제품을 고쳐야 한다.
# 둘을 한 수로 적으면 어느 쪽도 못 고친다.
#
# 회차 표는 한 줄이 한 회차다. 탭으로 나눈 네 칸이다.
#
#   요청유입<TAB>실측유입<TAB>판정(ok|under|unmeasurable)<TAB>응답p99ms
#
# **응답 p99 는 끝에서 끝까지다.** G10.10 의 게이트웨이 오버헤드가 아니다 —
# 그 값은 LB 기준선을 빼야 나온다 (10.7.5 · 10.7.6). 기준은 안 지어낸다.
# `PEAK_LATENCY_P99_MS` 를 줄 때만 걸고, 안 주면 적기만 한다.
set -uo pipefail

# 종료 코드를 가른다 — 2 는 계기를 고치라는 뜻이고 1 은 제품을 고치라는 뜻이다.
UNMEASURABLE=2

fail() { echo "::error title=현재 최대치::$1"; exit "$UNMEASURABLE"; }

# **인자 누락도 판정 불가다.** `${1:?}` 로 두면 bash 가 1 로 끝내, 배선 실수가
# "미달"(제품 결함)로 읽힌다.
[ $# -ge 1 ] || fail "회차 표 파일이 필요하다"
table=$1
[ -s "$table" ] || fail "회차 표가 비었다 — 회차를 한 번도 안 돌렸다: $table"

number='^[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$'

# **기준도 확인한다.** 그대로 산술에 넘기면 오타 하나가 모든 회차를 통과시킨다.
tolerance=${PEAK_ARRIVAL_TOLERANCE:-0.95}
printf '%s' "$tolerance" | grep -Eq "$number" \
    || fail "허용 오차가 숫자가 아니다: '$tolerance'"
awk -v t="$tolerance" 'BEGIN{ exit (t > 0 && t <= 1) ? 0 : 1 }' \
    || fail "허용 오차는 0 초과 1 이하여야 한다: '$tolerance'"

latency_limit=${PEAK_LATENCY_P99_MS:-}
if [ -n "$latency_limit" ]; then
    printf '%s' "$latency_limit" | grep -Eq "$number" \
        || fail "응답 기준이 숫자가 아니다: '$latency_limit'"
fi

# 바닥은 선택이다. 주면 되돌아가는 것을 막고, 안 주면 기록만 한다.
floor=${PEAK_FLOOR:-}
if [ -n "$floor" ]; then
    printf '%s' "$floor" | grep -Eq "$number" \
        || fail "바닥이 숫자가 아니다: '$floor'"
fi

# **한 번에 읽고 검사한다.** 줄을 두 번 훑으면 검사한 표와 판정한 표가 갈린다.
best_rate=""
best_actual=""
best_p99=""
stop_kind=""
stop_rate=""
broke=0
gap=0
prev_rate=""
line_no=0
rows=0

while IFS=$'\t' read -r rate actual verdict p99 rest || [ -n "${rate:-}" ]; do
    line_no=$((line_no + 1))
    # 빈 줄과 주석은 넘긴다. 표를 사람이 읽을 수 있게 두려고 허용한다.
    case "${rate:-}" in ''|'#'*) continue ;; esac
    rows=$((rows + 1))
    [ -n "${p99:-}" ] || fail "${line_no} 번째 줄의 칸이 모자란다 — 넷이어야 한다"
    [ -z "${rest:-}" ] || fail "${line_no} 번째 줄의 칸이 넷을 넘는다"

    printf '%s' "$rate" | grep -Eq "$number" \
        || fail "${line_no} 번째 줄의 요청 유입이 숫자가 아니다: '$rate'"
    printf '%s' "$actual" | grep -Eq "$number" \
        || fail "${line_no} 번째 줄의 실측 유입이 숫자가 아니다: '$actual'"
    printf '%s' "$p99" | grep -Eq "$number" \
        || fail "${line_no} 번째 줄의 응답 p99 가 숫자가 아니다: '$p99'"
    case "$verdict" in
        ok|under|unmeasurable) ;;
        *) fail "${line_no} 번째 줄의 판정 칸이 ok·under·unmeasurable 이 아니다: '$verdict'" ;;
    esac

    # **오름차순을 요구한다.** 사다리가 아니면 "여기서 멈췄다" 가 뜻을 잃는다.
    if [ -n "$prev_rate" ] \
            && awk -v a="$prev_rate" -v b="$rate" 'BEGIN{ exit (b > a) ? 1 : 0 }'; then
        fail "요청 유입이 오름차순이 아니다: ${prev_rate} 다음에 ${rate}"
    fi
    prev_rate=$rate

    # **실측이 요청을 넘으면 계기가 틀어진 것이다.** 고정 도착률은 요청을 넘길
    # 수 없다. 여유를 넓게 두면 만든 적 없는 부하가 표에 남고 계획서로 간다 —
    # 반올림과 마지막 회차의 꼬리만 덮을 만큼만 둔다.
    awk -v a="$actual" -v r="$rate" 'BEGIN{ exit (a > r * 1.05) ? 0 : 1 }' \
        && fail "${line_no} 번째 줄의 실측이 요청을 넘는다 (${actual} > ${rate}) — 계기를 본다"

    made=$(awk -v a="$actual" -v r="$rate" -v t="$tolerance" \
        'BEGIN{ print (a >= r * t) ? 1 : 0 }')
    if [ -n "$latency_limit" ]; then
        fast=$(awk -v p="$p99" -v l="$latency_limit" 'BEGIN{ print (p <= l) ? 1 : 0 }')
    else
        fast=1
    fi

    if [ "$made" = 1 ] && [ "$verdict" = ok ] && [ "$fast" = 1 ]; then
        # **실패 뒤에 선 회차는 안 쓴다.** 사다리는 아래에서부터 이어져야 한다.
        if [ "$broke" = 1 ]; then gap=1; continue; fi
        best_rate=$rate
        best_actual=$actual
        best_p99=$p99
        continue
    fi

    [ "$broke" = 1 ] && continue
    broke=1
    stop_rate=$rate
    # **판정 불가를 먼저 본다.** 요약을 못 읽은 회차는 실측을 0 으로 적으므로,
    # 도착 미달을 먼저 보면 계기 사고가 "생성기 한계" 로 적힌다 — 이 자가
    # 가르려던 바로 그 둘이 그 자리에서 붙는다.
    if [ "$verdict" = unmeasurable ]; then
        stop_kind="판정 불가"
    elif [ "$made" = 0 ]; then
        stop_kind="하네스"
    else
        stop_kind="제품"
    fi
done < "$table"

# **표에 회차가 없는 것과 회차가 안 선 것은 다르다.** 앞엣것은 러너가 아무것도
# 안 남긴 것이고, 뒤엣것은 가장 낮은 유입조차 못 만든 것이다.
[ "$rows" -gt 0 ] || fail "회차가 한 줄도 없다 — 표가 빈 채로 왔다"

# **선 회차가 없을 때도 종료 코드를 가른다.** 0 을 최대치로 적으면 안 되지만,
# 제품이 가장 낮은 회차부터 무너진 것을 "계기를 고쳐라" 로 내보내는 것도 같은
# 종류의 잘못이다 — 다음에 할 일을 반대로 가리킨다.
if [ -z "$best_actual" ]; then
    if [ "$stop_kind" = "제품" ]; then
        echo "  가장 낮은 회차부터 안 선다 (요청 ${stop_rate})"
        echo "판정: 미달 — 게이트웨이가 가장 낮은 유입도 못 버텼다"
        exit 1
    fi
    fail "선 회차가 하나도 없다 (${stop_kind:-사다리가 비었다}) — 잰 것이 없다"
fi

# **천장을 못 봤으면 최대치라고 안 부른다.** 사다리가 짧았을 뿐인데 마지막 칸을
# 최대치로 적으면, 사다리를 줄이는 것으로 기록을 낮출 수 있고 그 수가 계획서로
# 간다. 못 본 것은 아래 경계다 — 그보다 크다는 것만 안다.
if [ "$broke" = 1 ]; then
    printf '  %-24s %s\n' "현재 최대치(실측 유입)" "$best_actual"
else
    printf '  %-24s %s\n' "아래 경계(실측 유입)" "$best_actual"
fi
printf '  %-24s %s\n' "그때의 요청 유입" "$best_rate"
printf '  %-24s %s ms\n' "그때의 응답 p99" "$best_p99"
printf '  %-24s %s\n' "허용 오차" "$tolerance"
printf '  %-24s %s\n' "응답 기준" "${latency_limit:+${latency_limit} ms}${latency_limit:-없음 — 적기만 한다}"

if [ "$broke" = 1 ]; then
    printf '  %-24s %s (요청 %s)\n' "천장의 종류" "$stop_kind" "$stop_rate"
else
    printf '  %-24s %s\n' "천장의 종류" "천장을 아직 못 봤다 — 사다리가 짧다"
    # 사다리를 늘려야 하는 회차를 기록으로 받아들일지는 부르는 쪽이 정한다.
    if [ "${PEAK_REQUIRE_CEILING:-0}" = 1 ]; then
        fail "천장을 못 봤다 — 사다리를 늘려야 최대치를 안다"
    fi
fi
[ "$gap" = 1 ] && echo "  ::경고:: 멈춘 회차 위에 선 회차가 있다 — 사다리가 이어지지 않았다"

if [ -n "$floor" ]; then
    if awk -v a="$best_actual" -v f="$floor" 'BEGIN{ exit (a >= f) ? 0 : 1 }'; then
        echo "판정: 충족 — 기록한 바닥 ${floor} 이상이다"
        exit 0
    fi
    # **바닥 아래인 이유를 가른다.** 하네스가 못 만들어 내려간 것을 제품 미달로
    # 내면 고칠 곳을 반대로 가리킨다. 바로 위 줄이 "하네스" 라고 찍었는데 종료
    # 코드가 "제품" 이면 게이트를 읽는 쪽은 종료 코드만 본다.
    if [ "$stop_kind" = "제품" ]; then
        echo "판정: 미달 — 최대치가 기록한 바닥 ${floor} 아래로 내려갔다"
        exit 1
    fi
    fail "값이 바닥 ${floor} 아래인데 천장이 ${stop_kind:-못 봤다} 쪽이다 — 제품 미달로 못 읽는다"
fi
echo "판정: 기록 — 바닥이 없어 되돌아감은 안 본다"
exit 0

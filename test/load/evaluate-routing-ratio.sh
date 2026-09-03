#!/usr/bin/env bash
# 유입 비율 판정 — **도착 건수가 여유 비율을 따르는가.**
#
# 재는 일(도커·부하)에서 떼어 놓는다. 붙여 두면 판정을 확인하려고 매번 스택을
# 세워야 하고, 그러면 아무도 확인하지 않는다 (TS-9).
#
#   사용: evaluate-routing-ratio.sh <이름:여유:도착> ...
#   예:   evaluate-routing-ratio.sh backend:200:333 backend-small:40:67 backend-mid:120:200
#
# 종료 0 충족 · 1 미달 · 2 판정 불가.
set -uo pipefail

# 허용 편차(%). **게이트가 적은 값과 같아야 한다** (G9.1 은 ±15%). 여기만
# 더 엄하면 기준을 넘긴 구현이 미달로 적히고, 더 헐거우면 못 넘긴 것이
# 통과로 적힌다. 둘 다 조용히 일어난다.
MAX_DEVIATION="${MAX_DEVIATION:-15}"

# 도착이 이만큼도 안 되면 비율을 논할 표본이 아니다. 적은 표본에서는 한두
# 건의 흔들림이 편차 수십 %로 보여, 못 잰 것이 미달로 기록된다.
MIN_TOTAL="${MIN_TOTAL:-100}"

# 넣은 부하 건수. 주면 도착 합계가 그것과 같은지까지 본다 — **일부만 닿은
# 실행을 통과시키지 않기 위해서다.** 600 건을 넣고 120 건만 닿아도 그 120 건의
# 비율은 맞을 수 있고, 그러면 못 잰 것이 충족으로 적힌다.
EXPECTED_TOTAL="${EXPECTED_TOTAL:-}"

# **설정값부터 본다.** 숫자가 아니면 뒤의 정수 비교가 오류를 내고, errexit 이
# 없으니 그대로 흘러 마지막 "충족" 이 찍힌다 — 망가진 설정이 무엇이든
# 통과시키게 된다.
for setting in MAX_DEVIATION MIN_TOTAL EXPECTED_TOTAL; do
    value=$(eval "printf '%s' \"\$$setting\"")
    [ "$setting" = EXPECTED_TOTAL ] && [ -z "$value" ] && continue
    case "$value" in
        ''|*[!0-9]*) echo "$setting 은 0 이상의 정수여야 한다: '$value'"; exit 2 ;;
    esac
done

[ $# -ge 2 ] || { echo "뒷단이 둘 이상이어야 비율을 잰다"; exit 2; }

names=() credits=() arrived=()
credit_sum=0 total=0
for spec in "$@"; do
    # **칸을 따로 본다.** 붙여서 검사하면 `a::100` 처럼 한쪽이 빈 것이 통과하고,
    # 셸 산술이 그 빈칸을 0 으로 읽어 판정이 정상처럼 나온다.
    IFS=: read -r name credit count extra <<<"$spec"
    if [ -n "${extra:-}" ] || [ -z "${name:-}" ]; then
        echo "모양은 <이름:여유:도착> 이어야 한다: $spec"; exit 2
    fi
    for field in "${credit:-}" "${count:-}"; do
        case "$field" in
            ''|*[!0-9]*) echo "여유와 도착은 숫자여야 한다: $spec"; exit 2 ;;
        esac
    done
    names+=("$name"); credits+=("$((10#$credit))"); arrived+=("$((10#$count))")
    credit_sum=$(( credit_sum + 10#$credit ))
    total=$(( total + 10#$count ))
done

[ "$credit_sum" -gt 0 ] || { echo "여유 합계가 0 이라 기대값을 못 만든다"; exit 2; }

if [ -n "$EXPECTED_TOTAL" ] && [ "$total" -ne "$EXPECTED_TOTAL" ]; then
    echo "판정 불가 — 넣은 부하 $EXPECTED_TOTAL 건 중 $total 건만 뒷단에 닿았다"
    exit 2
fi

if [ "$total" -lt "$MIN_TOTAL" ]; then
    echo "판정 불가 — 도착 합계 $total 이 최소 표본 $MIN_TOTAL 에 못 미친다"
    exit 2
fi

# **나눗셈을 하지 않고 견준다.** 기대값을 먼저 내림하고 백분율을 또 버리면
# 절삭이 두 번 겹쳐, 실제로는 허용 밖인 비율이 안쪽 값으로 찍힌다. 대신
# 양변에 곱해 정수로만 비교한다.
#
#   편차 = (도착 × 여유합 − 합계 × 여유) / (합계 × 여유)
#
# 표시할 백분율만 반올림한다 — 판정은 위 정수 비교가 한다.
printf '%-16s %8s %8s %9s\n' 뒷단 도착 기대 편차
violated=0
for idx in "${!names[@]}"; do
    denominator=$(( total * credits[idx] ))
    numerator=$(( arrived[idx] * credit_sum - denominator ))
    magnitude=${numerator#-}

    if [ "$denominator" -eq 0 ]; then
        # 여유가 0 인데 무언가 도착했다면 그 자체가 위반이다. 0 으로 나눌 수
        # 없다고 편차 0 으로 적으면, 보내면 안 되는 곳으로 보낸 것이 묻힌다.
        expected=0
        if [ "${arrived[idx]}" -gt 0 ]; then
            violated=1
            printf '%-16s %8d %8d %9s\n' "${names[idx]}" "${arrived[idx]}" 0 "여유 0"
            continue
        fi
        percent=0
    else
        expected=$(( denominator / credit_sum ))
        percent=$(( (magnitude * 100 + denominator / 2) / denominator ))
        [ "$numerator" -lt 0 ] && percent=$(( -percent ))
        [ $(( magnitude * 100 )) -gt $(( MAX_DEVIATION * denominator )) ] && violated=1
    fi
    printf '%-16s %8d %8d %8d%%\n' "${names[idx]}" "${arrived[idx]}" "$expected" "$percent"
done

echo
if [ "$violated" -ne 0 ]; then
    echo "미달 — 허용 ±${MAX_DEVIATION}% 를 넘은 뒷단이 있다"
    exit 1
fi
echo "충족 — 모든 뒷단이 허용 ±${MAX_DEVIATION}% 안이다"

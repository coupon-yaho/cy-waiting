#!/usr/bin/env bash
# 종료 유출 판정 — **죽은 대로 간 요청이 사용자에게 오류로 갔는가.**
#
#   사용: evaluate-kill.sh <응답코드파일>
#
# 종료 0 충족 · 1 미달 · 2 판정 불가.
#
# 재는 일에서 떼어 놓는다. 붙여 두면 판정을 확인하려고 매번 스택을 세워야 하고,
# 그러면 아무도 확인하지 않는다 (TS-9).
set -uo pipefail

# 허용 5xx. **0 이다.** 하나라도 새면 사용자가 장애를 본 것이다.
MAX_5XX="${MAX_5XX:-0}"
# 이만큼도 안 보냈으면 유출이 없다고 말할 수 없다.
MIN_SAMPLE="${MIN_SAMPLE:-20}"

for setting in MAX_5XX MIN_SAMPLE; do
    value=$(eval "printf '%s' \"\$$setting\"")
    case "$value" in
        ''|*[!0-9]*) echo "$setting 은 0 이상의 정수여야 한다: '$value'"; exit 2 ;;
    esac
done

codes=${1:-}
[ -n "$codes" ] && [ -f "$codes" ] || { echo "응답 코드 파일이 없다: '$codes'"; exit 2; }

# 숫자가 아닌 줄이 섞이면 셈이 조용히 어긋난다. 세기 전에 막는다.
if grep -qv '^[0-9][0-9][0-9]$' "$codes"; then
    echo "응답 코드가 아닌 줄이 있다"; exit 2
fi

total=$(grep -c '' "$codes")
leaked=$(grep -c '^5' "$codes")

echo "보낸 ${total} 건 중 5xx ${leaked} 건"
if [ "$total" -lt "$MIN_SAMPLE" ]; then
    echo "판정 불가 — 표본 ${total} 건으로는 유출을 말할 수 없다"
    exit 2
fi
if [ "$leaked" -gt "$MAX_5XX" ]; then
    echo "미달 — 허용 ${MAX_5XX} 건을 넘었다"
    exit 1
fi
echo "충족 — 5xx 유출이 허용 안이다"

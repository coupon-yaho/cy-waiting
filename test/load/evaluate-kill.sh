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

# **뒷단까지 간 요청이 이만큼은 있어야 한다.** 전부 대기(202)나 한도(429)로
# 끊기면 죽은 대로 가는 경로를 아예 안 밟은 것이라, 그 실행의 "유출 0" 은
# 아무것도 뜻하지 않는다. 실제로 10,932 건이 전부 202 인 실행이 "충족" 으로
# 나온 적이 있다 — **거짓 초록은 없는 판정보다 나쁘다.**
MIN_ADMITTED="${MIN_ADMITTED:-20}"

for setting in MAX_5XX MIN_SAMPLE MIN_ADMITTED; do
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
# 판정이 돌려보낸 것(대기·한도)은 뒷단에 안 닿았다. 나머지가 실제로 간 것이다.
turned_back=$(grep -cE '^(202|429)$' "$codes")
admitted=$(( total - turned_back ))
server_error=$(grep -c '^5' "$codes")
# **응답이 아예 없는 것도 유출이다.** curl 은 그때 `000` 을 낸다. 사용자에게는
# 5xx 보다 나쁘다 — 오류 대신 몇 초를 기다린 뒤 아무것도 못 받는다. 이것을
# 안 세면 "5xx 0 건" 이라는 초록이 매달린 요청 위에 뜬다.
no_answer=$(grep -c '^000$' "$codes")
leaked=$(( server_error + no_answer ))

echo "보낸 ${total} 건 중 뒷단까지 ${admitted} 건 · 5xx ${server_error} 건 · 응답 없음 ${no_answer} 건"
if [ "$total" -lt "$MIN_SAMPLE" ]; then
    echo "판정 불가 — 표본 ${total} 건으로는 유출을 말할 수 없다"
    exit 2
fi
if [ "$admitted" -lt "$MIN_ADMITTED" ]; then
    echo "판정 불가 — 뒷단까지 간 것이 ${admitted} 건뿐이다. 죽은 대로 가는 경로를 안 밟았다"
    exit 2
fi
if [ "$leaked" -gt "$MAX_5XX" ]; then
    echo "미달 — 유출 ${leaked} 건이 허용 ${MAX_5XX} 건을 넘었다"
    exit 1
fi
echo "충족 — 유출이 허용 안이다"

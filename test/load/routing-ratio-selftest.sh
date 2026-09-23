#!/usr/bin/env bash
# 유입 비율 판정의 자기검증 (TS-9).
#
# **이 판정이 라우팅 게이트를 열고 닫는다.** 늘 "충족" 을 내면 비율이 무너져도
# 통과하고, 늘 "미달" 을 내면 맞는 구현도 못 넘는다. 둘 다 조용히 일어난다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1
. test/load/selftest-lib.sh

SELFTEST_JUDGE=$PWD/test/load/evaluate-routing-ratio.sh
# **물려받은 값을 버린다.** 이 셋을 밖에서 들고 오면 자기검증이 환경에 따라
# 다른 답을 낸다 — `MIN_TOTAL=0` 이면 표본 부족 사례가 통과하고,
# `EXPECTED_TOTAL=600` 이면 비율 사례가 엉뚱한 이유로 끝난다. 판정이 무는지를
# 보는 자리에서 그 판정의 입력이 흔들리면 무엇을 봤는지 알 수 없다.
unset TOLERANCE MIN_TOTAL EXPECTED_TOTAL MAX_DEVIATION


# 허용치를 못 박고 부른다. 기본값에 기대면 기본값이 움직일 때 경계 사례가
# 조용히 다른 것을 재게 된다.
#
# **대조는 공통부가 한다.** 넷 중 이 하나만 제 사본을 들고 있었고, 사본이
# 있으면 그중 하나만 헐거워져도 알 수 없다 — 공통부를 뺀 이유가 그것이다.
check() {
    local name=$1 want_rc=$2 want_word=$3; shift 3
    MAX_DEVIATION="${TOLERANCE:-10}" MIN_TOTAL="${MIN_TOTAL:-100}" \
        EXPECTED_TOTAL="${EXPECTED_TOTAL:-}" \
        run_case "$name" "$want_rc" "$want_word" -- "$@"
}

echo "유입 비율 판정 자기검증"

# 실제로 잰 값 둘. 하나는 비율대로 왔고 하나는 안 왔다 — 판정이 이 둘을
# 갈라내지 못하면 무엇을 재도 같은 답이 나온다.
check "여유대로 온 실측은 충족" 0 "충족" \
    backend:200:333 backend-small:40:67 backend-mid:120:200
check "작은 대에 몰린 실측은 미달" 1 "미달" \
    backend:200:206 backend-small:40:188 backend-mid:120:206

# **경계를 양쪽에서 밟는다.** 한쪽만 보면 부등호 방향이 뒤집혀도 안 걸린다.
# 합계 300, 여유가 같으면 기대는 각 100 이다.
check "허용 안이면 충족" 0 "충족" a:1:110 b:1:100 c:1:90
check "허용 밖이면 미달" 1 "미달" a:1:112 b:1:100 c:1:88

# 표본이 적으면 한두 건이 편차 수십 %로 보인다. 못 잰 것을 미달로 적으면
# 없는 결함을 쫓게 된다.
check "표본이 모자라면 판정 불가" 2 "판정 불가" a:1:10 b:1:10

# 숫자가 아니면 셸 산술이 0 으로 읽어, 아무것도 안 온 것이 "기대와 같다" 로
# 나올 수 있다.
check "숫자가 아니면 막는다" 2 "숫자여야" a:1:100 b:xx:100 c:1:100
check "여유 합계가 0 이면 막는다" 2 "여유 합계" a:0:200 b:0:200
check "뒷단이 하나면 비율이 없다" 2 "둘 이상" a:1:500

# **절삭이 겹치면 게이트가 헐거워진다.** 기대값을 먼저 내림하고 백분율을 또
# 버리면 실제 -16% 가 -15% 로 찍혀 허용 안으로 들어온다. 리뷰가 든 값 그대로다.
TOLERANCE=15 MIN_TOTAL=1 check "경계 밖이 절삭으로 안에 들지 않는다" 1 "미달" a:1:28 b:2:72

# 여유가 0 인 곳은 후보에서 빠져야 한다. 0 으로 못 나눈다고 편차 0 으로 적으면
# 보내면 안 되는 곳으로 보낸 것이 묻힌다.
check "여유 0 인 곳에 도착하면 미달" 1 "여유 0" a:0:120 b:1:400

# 넣은 부하와 도착이 다르면 못 잰 것이다. 닿은 일부의 비율은 맞을 수 있다.
EXPECTED_TOTAL=600 check "일부만 닿으면 판정 불가" 2 "판정 불가" a:1:100 b:1:100

# 모양이 어긋난 자리들. 빈칸은 셸 산술이 0 으로 읽어 판정이 정상처럼 나온다.
check "여유가 비면 막는다" 2 "숫자여야" a::100 b:1:100
check "도착이 비면 막는다" 2 "숫자여야" a:1: b:1:100
check "칸이 넷이면 막는다" 2 "모양은" a:1:100:9 b:1:100

# **망가진 설정이 통과시키지 않는다.** 숫자가 아닌 허용치는 정수 비교에서
# 오류를 내고, errexit 이 없으면 그대로 흘러 마지막 "충족" 이 찍힌다.
TOLERANCE=oops check "허용치가 숫자가 아니면 막는다" 2 "정수여야" a:1:200 b:1:200
MIN_TOTAL=xx check "최소 표본이 숫자가 아니면 막는다" 2 "정수여야" a:1:200 b:1:200
# 부하 하네스가 넘기는 계약이라 여기도 같이 본다.
EXPECTED_TOTAL=oops check "기대 총량이 숫자가 아니면 막는다" 2 "정수여야" a:1:200 b:1:200

# **기본 허용치가 게이트와 같은지 본다.** 여기가 갈라지면 같은 실측을 놓고
# 스크립트와 계획서가 다른 답을 낸다. 편차 12% 는 게이트(±15%) 안이고 자기
# 검증이 쓰는 10% 밖이다 — 기본값이 무엇인지가 결과를 가른다.
run_case "기본 허용치가 게이트와 같다 (±15%)" 0 "충족" -- a:1:112 b:1:100 c:1:88

# --- 하네스 쪽 부분 (TS-9) ---
#
# **하네스가 고장 난 회차를 제품 미달로 적으면 안 된다.** 요약이 깨지면 빈 값이
# 흐르고, 자극이 안 심겼는데 회차가 그대로 돈다. 둘 다 실제로 났던 자리다.
BIG_CAP=${BIG_CAP:-900} SMALL_CAP=${SMALL_CAP:-100} MID_CAP=${MID_CAP:-300}
export BIG_CAP SMALL_CAP MID_CAP
. test/load/routing-lib.sh || exit 2

lib_work=$(mktemp -d) || exit 1
trap 'rm -rf "$lib_work"' EXIT


echo "하네스 부분 자기검증"

# 요약이 깨지면 보낸 건수가 빈 값으로 흐른다. 그 회차는 판정 불가여야 한다.
sent=600
lib_case "수면 그대로 쓴다" 0 "$(require_positive_int sent >/dev/null 2>&1; printf '%s' $?)"
sent=''
lib_case "빈 값이면 판정 불가" 2 "$(require_positive_int sent >/dev/null 2>&1; printf '%s' $?)"
lib_case "무엇을 못 쟀는지 말한다" 1 "$(require_positive_int sent 2>&1 | grep -c sent)"
sent=0
lib_case "0 이면 판정 불가" 2 "$(require_positive_int sent >/dev/null 2>&1; printf '%s' $?)"

# 요약 파싱. **깨진 요약을 넣어 본다** — 러너에 인라인이면 이 사례를 못 만든다.
summary() { printf '%s' "$2" > "$lib_work/$1.json"; printf '%s' "$lib_work/$1.json"; }
flat=$(summary flat '{"metrics":{"issue_200":{"count":7},"issue_202":{"count":2},"issue_other":{"count":1},"http_reqs":{"count":10},"iterations":{"count":10}}}')
nested=$(summary nested '{"metrics":{"issue_200":{"values":{"count":7}},"http_reqs":{"values":{"count":7}},"iterations":{"values":{"count":7}}}}')
broken=$(summary broken '{"metrics":')
lib_case "평평한 계수를 읽는다" "$(printf '200 7\n202 2\nother 1\ntotal 10\ndone 10')" "$(codes_from_summary "$flat")"
lib_case "한 겹 더 들어간 계수도 읽는다" "$(printf '200 7\n202 0\nother 0\ntotal 7\ndone 7')" "$(codes_from_summary "$nested")"
lib_case "더 달라는 계수도 낸다" "$(printf '200 0\n202 0\nother 0\ntotal 0\ndone 0\nissue_gw1 3')" \
    "$(codes_from_summary "$(summary gw '{"metrics":{"issue_gw1":{"count":3}}}')" issue_gw1)"
lib_case "깨진 요약은 끊는다" 1 "$(codes_from_summary "$broken" >/dev/null 2>&1; printf '%s' $?)"
lib_case "없는 파일도 끊는다" 1 "$(codes_from_summary "$lib_work/nope.json" >/dev/null 2>&1; printf '%s' $?)"

# 코드 합 대조. **완료 회차를 닻으로 쓴다** — http_reqs 는 중단된 회차의 요청까지 세어,
# 꼬리 몇 건이 그렇게 되면 멀쩡한 회차가 통째로 판정 불가가 된다.
codes() { local f=$lib_work/$1; shift; printf '%s\n' "$@" > "$f"; printf '%s' "$f"; }
ok_codes=$(codes ok "200 7" "202 2" "other 1" "total 10" "done 10")
short=$(codes short "200 0" "202 0" "other 0" "total 10" "done 10")
dropped=$(codes dropped "200 7" "202 2" "other 1" "total 12" "done 10")
lib_case "합이 완료 회차와 같으면 통과" 0 "$(require_codes_match "$ok_codes" >/dev/null 2>&1; printf '%s' $?)"
lib_case "계수가 다 0 이면 판정 불가" 2 "$(require_codes_match "$short" >/dev/null 2>&1; printf '%s' $?)"
lib_case "중단된 회차가 있어도 통과" 0 "$(require_codes_match "$dropped" >/dev/null 2>&1; printf '%s' $?)"
lib_case "무엇이 어긋났는지 말한다" 1 "$(require_codes_match "$short" 2>&1 | grep -c '코드별 합')"
empty=$(codes empty "200 0" "202 0" "other 0" "total 0" "done 0")
lib_case "빈 회차는 충족이 아니다" 2 "$(require_codes_match "$empty" >/dev/null 2>&1; printf '%s' $?)"
extra=$(codes extra "200 10" "202 0" "other 0" "total 400" "done 10")
lib_case "보낸 수가 크게 벌어지면 판정 불가" 2 \
    "$(CODES_SLACK=100 require_codes_match "$extra" >/dev/null 2>&1; printf '%s' $?)"
lib_case "판정 불가 사유는 표준 출력으로" 1 "$(require_codes_match "$empty" 2>/dev/null | grep -c '판정 불가')"
lib_case "허용 폭이 수가 아니면 끊는다" 2 \
    "$(CODES_SLACK=많이 require_codes_match "$ok_codes" >/dev/null 2>&1; printf '%s' $?)"
lib_case "허용 폭이 비면 기본값으로 떨어진다" 0 \
    "$(CODES_SLACK='' VUS='' require_codes_match "$ok_codes" >/dev/null 2>&1; printf '%s' $?)"

# 게이트웨이별 계수. 이름이 어긋나면 0 뿐인 값으로 쏠림을 판정하게 된다.
gws=$(summary gws '{"metrics":{"issue_200":{"count":4},"http_reqs":{"count":4},"iterations":{"count":4},"issue_gw0":{"count":3},"issue_gw1":{"count":1}}}')
lib_case "게이트웨이별 계수를 gw 이름으로 낸다" \
    "$(printf '200 4\n202 0\nother 0\ntotal 4\ndone 4\ngw0 3\ngw1 1')" \
    "$(codes_with_gateways "$gws" 2)"
lib_case "게이트웨이 수가 수가 아니면 끊는다" 2 \
    "$(codes_with_gateways "$gws" x >/dev/null 2>&1; printf '%s' $?)"

# 자극은 되읽어 확인한다. 안 하면 안 심긴 회차가 제품 미달로 적힌다.
redis_cli() {
    case "$1" in
        SET) printf '%s' "$3" > "$lib_work/$2" ;;
        GET) cat "$lib_work/$2" 2>/dev/null ;;
    esac
}
lib_case "심은 값이 되읽히면 통과" 0 \
    "$(redis_set_verified sim:credits:stub-1 900 >/dev/null 2>&1; printf '%s' $?)"
redis_cli() { case "$1" in GET) printf '엉뚱' ;; esac; }
lib_case "값이 다르면 판정 불가" 2 \
    "$(redis_set_verified sim:credits:stub-1 900 >/dev/null 2>&1; printf '%s' $?)"
# **쓰기가 조용히 실패했는데 앞 회차 잔값이 같으면** 되읽기가 통과한다. 지우고 쓰는 것이 그것을 막는다.
printf '900' > "$lib_work/sim:credits:stub-1"
redis_cli() {
    case "$1" in
        DEL) rm -f "$lib_work/$2" ;;
        SET) return 1 ;;                       # 쓰기가 조용히 실패한 회차
        GET) cat "$lib_work/$2" 2>/dev/null ;;
    esac
}
lib_case "안 지우고 통과하지 않는다" 2 \
    "$(redis_set_verified sim:credits:stub-1 900 >/dev/null 2>&1; printf '%s' $?)"
lib_case "무슨 키인지 말한다" 1 \
    "$(redis_set_verified sim:credits:stub-1 900 2>&1 | grep -c 'sim:credits:stub-1')"
unset -f redis_cli
unset sent

exit $selftest_failed

#!/usr/bin/env bash
# 죽은 주소 판정의 자기검증 (TS-9).
#
# **닿지 않은 회차를 통과로 적으면 이 게이트가 사라진다.** 죽은 주소가 목록에
# 안 들어왔거나 아무도 안 골랐으면 "유출 0" 은 그 주소와 무관한 값이다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1
. test/load/selftest-lib.sh

SELFTEST_JUDGE=$PWD/test/load/evaluate-dead-addr.sh

echo "죽은 주소 판정 자기검증"

#         보낸것  200   아닌것 산대도착 인스턴스 배제
run_case "다 200 이고 닿았으면 충족" 0 "충족" -- 7921 7921 0 7921 4 1
run_case "5xx 가 새면 미달" 1 "5xx 로 샌다" -- 7921 7900 21 7900 4 1
run_case "도착이 모자라면 미달" 1 "어디로 갔나" -- 7921 7921 0 7000 4 1
run_case "목록에 안 들어오면 판정 불가" 2 "목록에 안 들어왔다" -- 7921 7921 0 7921 3 1
run_case "배제가 안 오르면 판정 불가" 2 "간 요청이 없다" -- 7921 7921 0 7921 4 0
run_case "보낸 것이 0 이면 판정 불가" 2 "보낸 것이 0" -- 0 0 0 0 4 1
run_case "합이 보낸 것보다 크면 판정 불가" 2 "보낸" -- 100 90 20 90 4 1
run_case "숫자가 아니면 판정 불가" 2 "정수여야" -- 7921 일곱 0 7921 4 1
run_case "인자 수가 다르면 판정 불가" 2 "여섯" -- 7921 7921 0 7921 4

exit "$selftest_failed"

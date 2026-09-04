#!/usr/bin/env bash
# 배제 판정의 자기검증 (TS-9).
#
# **이 판정이 "즉시 실패하는 대를 끊고 안 튀게 되돌린다" 게이트를 연다.**
# 헐거우면 복귀가 절벽인 구현도 초록이 뜨는데, 그 절벽이 바로 이 기능을
# 만들면서 처음 만든 결함이다 — 판정이 그것을 못 잡으면 없는 것과 같다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1
. test/load/selftest-lib.sh

SELFTEST_JUDGE=$PWD/test/load/evaluate-eject.sh
work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

# 물려받은 값을 버린다. 판정이 무는지를 보는 자리에서 입력이 흔들리면
# 무엇을 본 것인지 알 수 없다.
unset EJECTED_SHARE EJECT_BUDGET_SEC SLACK RECOVER_BUDGET_SEC \
    SUSTAIN SPIKE_RATIO MIN_PER_SEC MIN_USABLE_SEC

check() {
    local name=$1 want_rc=$2 want_word=$3 fault=$4 recover=$5
    printf '%s\n' "$fault" > "$work/f.txt"
    printf '%s\n' "$recover" > "$work/r.txt"
    run_case "$name" "$want_rc" "$want_word" -- "$work/f.txt" "$work/r.txt" 33
}

echo "배제 판정 자기검증"

# 셋의 여유가 같으니 정상 몫은 33% 다. 고장 뒤 2초 만에 끊기고, 복귀는
# 램프를 타 55초에 걸쳐 0 에서 33 으로 오른다.
cut_ok='-2 33 99
-1 34 99
0 30 99
1 12 99
2 2 99
3 1 99
4 0 99
5 1 99'

back_ok='0 3 99
1 8 99
2 14 99
3 20 99
4 26 99
5 31 99
6 33 99
7 32 99
8 34 99'

check "끊기고 램프를 타고 돌아온다" 0 "충족" "$cut_ok" "$back_ok"

# **끊기지 않는다.** 배제가 아예 안 걸리면 이 모양이다.
no_cut='-2 33 99
-1 34 99
0 33 99
1 34 99
2 33 99
3 32 99
4 34 99
5 33 99'
check "안 끊기면 미달" 1 "머문 적이 없다" "$no_cut" "$back_ok"

# **예산을 넘겨 끊긴다.** 늦게라도 끊기지만 게이트가 요구하는 시간 밖이다.
late_cut='-2 33 99
-1 34 99
0 33 99
1 33 99
2 32 99
3 30 99
4 25 99
5 15 99
6 5 99
7 2 99
8 1 99
9 0 99'
check "늦게 끊기면 미달" 1 "예산을 넘었다" "$late_cut" "$back_ok"

# **복귀가 절벽이다.** 배제 동안 트래픽이 0 이라 그 대가 가장 한가해 보이고,
# 돌아오는 순간 제 몫을 훨씬 넘겨 받는다. 이 판정이 반드시 잡아야 하는 모양이다.
cliff='0 88 99
1 90 99
2 60 99
3 40 99
4 33 99
5 34 99
6 33 99'
check "복귀가 절벽이면 미달" 1 "절벽이다" "$cut_ok" "$cliff"

# **안 돌아온다.** 배제가 영구화되면 이 모양이다. 재기동 없이는 용량이 안 는다.
never_back='0 0 99
1 0 99
2 1 99
3 0 99
4 0 99
5 1 99'
check "안 돌아오면 미달" 1 "머문 적이 없다" "$cut_ok" "$never_back"

# **고장 전부터 안 가고 있었다.** 배선이 어긋났거나 고장을 넣는 명령이 조용히
# 실패한 실행이다. 이때 "0초 만에 끊겼다" 로 적으면 아무것도 안 잰 회차가
# 가장 좋은 성적으로 남는다.
never_sent='-2 1 99
-1 0 99
0 0 99
1 0 99
2 0 99
3 0 99'
check "원래 안 가고 있으면 판정 불가" 2 "잴 것이 없다" "$never_sent" "$back_ok"

# **직전 초를 못 믿는다.** 초당 도착이 기준 아래면 비율이 흔들려 무의미하다.
thin='-2 3 5
-1 2 4
0 0 3
1 0 3
2 0 3'
check "표본이 얇으면 판정 불가" 2 "판정 불가" "$thin" "$back_ok"

# **복귀 표본이 얇다.** 진입은 멀쩡한데 복귀를 못 재는 회차다.
thin_back='0 1 3
1 1 4
2 1 2'
check "복귀 표본이 얇으면 판정 불가" 2 "판정 불가" "$cut_ok" "$thin_back"

# 설정 검증. **0 이면 아래에서 0 으로 나뉘고**, 그때 종료 1 이 나가
# 하네스 설정 오류가 제품 미달로 적힌다.
printf '%s\n' "$cut_ok" > "$work/f.txt"
printf '%s\n' "$back_ok" > "$work/r.txt"
MIN_PER_SEC=0 run_case "초당 최소가 0 이면 판정 불가" 2 "1 이상" \
    -- "$work/f.txt" "$work/r.txt" 33
SPIKE_RATIO=90 run_case "절벽 기준이 100 미만이면 판정 불가" 2 "100 이상" \
    -- "$work/f.txt" "$work/r.txt" 33
SUSTAIN=0 run_case "머무는 초가 0 이면 판정 불가" 2 "1 이상" \
    -- "$work/f.txt" "$work/r.txt" 33
run_case "정상 몫이 0 이면 판정 불가" 2 "1 이상" -- "$work/f.txt" "$work/r.txt" 0
run_case "표본 파일이 없으면 판정 불가" 2 "표본 파일이 없다" \
    -- "$work/없다.txt" "$work/r.txt" 33

exit "$selftest_failed"

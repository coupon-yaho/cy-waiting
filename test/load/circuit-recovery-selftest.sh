#!/usr/bin/env bash
# 서킷 회복 판정의 자기검증 (TS-9).
#
# **이 판정이 서킷 해제 램프의 유일한 실측 근거다.** 램프는 게이트웨이 한 대
# 짜리 단위 시험으로만 잡혀 있었다. 여기가 조용히 틀리면, 회복이 계단인 회차가
# 충족으로 적힌다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

. test/load/selftest-lib.sh || exit 2
# 돌연변이를 넣은 사본을 겨눌 수 있어야 한다. 무조건 덮어쓰면 사본을 지목한
# 회차가 조용히 원본을 돌리고 전부 "안 잡힘" 으로 나온다.
SELFTEST_JUDGE=${SELFTEST_JUDGE:-$PWD/test/load/evaluate-circuit-recovery.sh}

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT

# 표본 한 줄: <시각ms> <발행 크레딧> <뒷단 누적 도착> <노드 수> <노드별 서킷 표>
#
# **표를 같이 뜨는 이유.** 크레딧 하나만 보면 "서킷이 아직 안 닫혔다" 와 "표는
# 닫혔는데 게이트가 안 풀렸다" 가 같은 그림이다. 둘은 고칠 자리가 다르다.
# 구간은 `#` 줄로 가른다. 러너가 자극을 준 시각에 그 줄을 쓴다.
NODES=2
# 노드 둘이면 한산 통과가 성립하는 최소가 4 다 (노드 수 × 유휴 나눗값).
# **정상 구간을 만든다.** 크레딧 300, 초당 100 건 도착. 이 둘이 기준선이다.
normal() {
    local t=$1 served=$2 n=${3:-8}
    for i in $(seq 0 $((n - 1))); do
        printf '%s 300 %s %s CLOSED\n' $((t + i * 200)) $((served + i * 20)) "$NODES"
    done
}

# 조인 구간. 크레딧이 노드 수 이하로 내려가 있고 도착도 멎는다.
# 조인 구간. 크레딧이 노드 수 이하로 내려가 있다.
#
# **도착 증분을 줄 수 있게 둔다.** 조인 동안에도 프로브는 나가므로 도착이 아주
# 안 멎지는 않는다. 0 으로 두면 회복 구간 꼬리가 평평해져, 부하가 먼저 끝난
# 회차를 막는 가드에 걸린다.
gated() {
    local t=$1 served=$2 credit=${3:-2} n=${4:-5} vote=${5:-OPEN} step=${6:-0}
    for i in $(seq 0 $((n - 1))); do
        printf '%s %s %s %s %s\n' $((t + i * 200)) "$credit" \
            $((served + i * step)) "$NODES" "$vote"
    done
}

# 회복 구간. 하한 4 에서 두 배씩 올라 300 에 닿는다. 도착은 크레딧을 따라간다.
recovering() {
    local t=$1 served=$2
    local i=0
    for credit in 4 8 16 32 64 128 256 300 300; do
        printf '%s %s %s %s CLOSED\n' $((t + i * 200)) "$credit" "$((served + i * 20))" "$NODES"
        i=$((i + 1))
    done
}

healthy_run() {
    {
        echo '# 정상'; normal 0 0
        echo '# 진입'; gated 1600 160 2 3
        echo '# 유지'; gated 2200 160 2 5
        echo '# 회복'; recovering 3200 160
        echo '# 해제'; echo '# 승계'; printf '%s 300 %s %s CLOSED\n' 5200 340 "$NODES"
    } > "$work/$1"
    printf '%s' "$work/$1"
}

echo "서킷 회복 자기검증"

run_case "정상 회차는 충족" 0 "충족" -- "$(healthy_run ok.txt)"

# **진입이 늦으면 조임이 안 걸린 것이다.** 뒷단이 못 받는데 몫이 그대로 나간다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 300 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'; recovering 3200 160
    echo '# 해제'; echo '# 승계'; printf '%s 300 %s %s CLOSED\n' 5200 340 "$NODES"
} > "$work/late.txt"
run_case "진입이 늦으면 미달" 1 "조이지 않았다" -- "$work/late.txt"

# 유지 구간에 한 번이라도 상한 위로 튀면 조임이 안 붙어 있는 것이다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 2
    printf '%s 300 160 %s CLOSED\n' 2600 "$NODES"
    echo '# 회복'; recovering 3200 160
    echo '# 해제'; echo '# 승계'; printf '%s 300 %s %s CLOSED\n' 5200 340 "$NODES"
} > "$work/leak.txt"
run_case "유지가 새면 미달" 1 "조임이 유지되지 않았다" -- "$work/leak.txt"

# **회복이 늦으면 램프가 게이트를 깬다.** 30초 안에 못 돌아오면 미달이다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'
    for i in $(seq 0 40); do
        printf '%s %s %s %s CLOSED\n' $((3200 + i * 1000)) $((4 + i)) $((160 + i * 4)) "$NODES"
    done
    # 승계 한 틱도 램프 안에 둔다. 밖에 두면 그 검사가 먼저 나서, 이 회차가
    # 회복 시간이 아니라 승계를 잰 것이 된다.
    echo '# 해제'; echo '# 승계'; printf '%s 80 %s %s CLOSED\n' 60000 400 "$NODES"
} > "$work/slow.txt"
run_case "회복이 안 끝나면 미달" 1 "회복이 안 끝났다" -- "$work/slow.txt"

# **끝나긴 했는데 늦은 회차.** 위 사례는 아예 안 끝나는 쪽이라 한계를 지워도
# 안 걸린다 — 한계를 실제로 쓰는지 보려면 닿기는 하되 늦게 닿아야 한다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'; printf '%s 4 160 %s CLOSED\n' 3200 "$NODES"
    echo '# 해제'; echo '# 승계'
    printf '%s 8 260 %s CLOSED\n' 13200 "$NODES"
    printf '%s 16 360 %s CLOSED\n' 18200 "$NODES"
    printf '%s 32 460 %s CLOSED\n' 23200 "$NODES"
    printf '%s 64 560 %s CLOSED\n' 28200 "$NODES"
    printf '%s 128 660 %s CLOSED\n' 33200 "$NODES"
    printf '%s 256 760 %s CLOSED\n' 36200 "$NODES"
    printf '%s 300 800 %s CLOSED\n' 38200 "$NODES"
} > "$work/slowdone.txt"
run_case "회복이 한계를 넘으면 미달" 1 "초 걸렸다" -- "$work/slowdone.txt"

# **완전히 열린 노드가 없으면 유예와 무관하게 잰다.** 재진입은 열림을 거쳐야만
# 나므로, 표에 `OPEN` 이 한 번도 없으면 섞일 구간이 없다. `HALF_OPEN` 을 부분
# 문자열로 보면 이 회차가 통째로 버려진다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'
    i=0
    while [ $i -lt 34 ]; do
        printf '%s 2 %s %s HALF_OPEN\n' $((3200 + i * 200)) $((160 + i)) "$NODES"
        i=$((i + 1))
    done
    printf '%s 4 200 %s CLOSED\n' 10000 "$NODES"
    echo '# 해제'; echo '# 승계'
    printf '%s 8 260 %s CLOSED\n' 13200 "$NODES"
    printf '%s 16 360 %s CLOSED\n' 18200 "$NODES"
    printf '%s 32 460 %s CLOSED\n' 23200 "$NODES"
    printf '%s 64 560 %s CLOSED\n' 28200 "$NODES"
    printf '%s 128 660 %s CLOSED\n' 33200 "$NODES"
    printf '%s 256 760 %s CLOSED\n' 36200 "$NODES"
    printf '%s 300 800 %s CLOSED\n' 38200 "$NODES"
} > "$work/halfopenonly.txt"
run_case "열린 노드가 없으면 유예를 안 탄다" 1 "잔여 6.8초" -- "$work/halfopenonly.txt"

# **한 노드가 먼저 나가도 안 끝난다.** 표는 노드별 상태를 접은 문자열이라 첫
# 변화로 끊으면 회차마다 제일 짧은 노드의 값이 적힌다. 먼저 나간 노드는 닫힌
# 것이지 열린 것이 아니다 — `CLOSED` 를 열림으로 읽으면 유예가 잘못 걸린다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'
    printf '%s 2 160 %s HALF_OPEN\n' 3200 "$NODES"
    printf '%s 2 180 %s CLOSED|HALF_OPEN\n' 3400 "$NODES"
    printf '%s 4 200 %s CLOSED\n' 3800 "$NODES"
    echo '# 해제'; echo '# 승계'
    printf '%s 8 260 %s CLOSED\n' 13200 "$NODES"
    printf '%s 16 360 %s CLOSED\n' 18200 "$NODES"
    printf '%s 32 460 %s CLOSED\n' 23200 "$NODES"
    printf '%s 64 560 %s CLOSED\n' 28200 "$NODES"
    printf '%s 128 660 %s CLOSED\n' 33200 "$NODES"
    printf '%s 256 760 %s CLOSED\n' 36200 "$NODES"
    printf '%s 300 800 %s CLOSED\n' 38200 "$NODES"
} > "$work/staggered.txt"
run_case "노드가 엇갈려 나가면 마지막까지 잰다" 1 "잔여 0.6초" -- "$work/staggered.txt"

# **유예 경계는 재진입이 시작되는 시각이다.** 열린 노드는 그 시각에 반쯤 열리므로
# 경계 자신이 이미 오염이다. 여기서 값을 적으면 재진입이 섞인 값을 적는다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'
    printf '%s 2 160 %s HALF_OPEN\n' 3200 "$NODES"
    i=0
    while [ $i -lt 25 ]; do
        printf '%s 2 %s %s HALF_OPEN|OPEN\n' $((3400 + i * 200)) $((161 + i)) "$NODES"
        i=$((i + 1))
    done
    # 열린 노드를 처음 본 3400 에서 정확히 열림 대기만큼 뒤다.
    printf '%s 4 200 %s OPEN\n' 8400 "$NODES"
    echo '# 해제'; echo '# 승계'
    printf '%s 8 260 %s CLOSED\n' 13200 "$NODES"
    printf '%s 16 360 %s CLOSED\n' 18200 "$NODES"
    printf '%s 32 460 %s CLOSED\n' 23200 "$NODES"
    printf '%s 64 560 %s CLOSED\n' 28200 "$NODES"
    printf '%s 128 660 %s CLOSED\n' 33200 "$NODES"
    printf '%s 256 760 %s CLOSED\n' 36200 "$NODES"
    printf '%s 300 800 %s CLOSED\n' 38200 "$NODES"
} > "$work/grace.txt"
run_case "유예 경계는 못 잰 쪽이다" 1 "잔여 -1.0초" -- "$work/grace.txt"

# **표가 재진입을 숨기면 못 잰 것으로 둔다.** 먼저 나간 노드가 열림 대기 뒤 다시
# 반쯤 열리면 그 구간이 처음 구간의 잔여에 섞여, 회차 비교가 부풀린 값으로 돈다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'
    printf '%s 2 160 %s HALF_OPEN\n' 3200 "$NODES"
    # 한 노드가 나간 뒤 열림 대기(5초)보다 오래 half-open 이 남는다.
    i=0
    while [ $i -lt 31 ]; do
        printf '%s 2 %s %s HALF_OPEN|OPEN\n' $((3400 + i * 200)) $((161 + i)) "$NODES"
        i=$((i + 1))
    done
    printf '%s 4 200 %s OPEN\n' 9800 "$NODES"
    echo '# 해제'; echo '# 승계'
    printf '%s 8 260 %s CLOSED\n' 13200 "$NODES"
    printf '%s 16 360 %s CLOSED\n' 18200 "$NODES"
    printf '%s 32 460 %s CLOSED\n' 23200 "$NODES"
    printf '%s 64 560 %s CLOSED\n' 28200 "$NODES"
    printf '%s 128 660 %s CLOSED\n' 33200 "$NODES"
    printf '%s 256 760 %s CLOSED\n' 36200 "$NODES"
    printf '%s 300 800 %s CLOSED\n' 38200 "$NODES"
} > "$work/reopen.txt"
run_case "재진입을 못 가르면 잔여는 -1" 1 "잔여 -1.0초" -- "$work/reopen.txt"

# 회복 구간에 닿기 전에 나는 실패도 층 수치를 싣는다. 그 자리의 0 은 잰 값이 아니다.
# **유지 구간에서 끊기는 표본이라야 한다.** 회복 표본을 다 읽고 끝에서 나는 실패는
# 초기화 자리를 늦게 잡아도 -1 이 찍혀, 고치려던 것을 안 문다.
run_case "회복 전에 실패해도 잔여는 -1" 1 "잔여 -1.0초" -- "$work/leak.txt"

# **충족 회차에도 잔여가 실린다.** 실측에서 사람이 읽는 자리가 그 줄이라, 칸이
# 어긋나 있으면 미달 회차에서만 맞는 값을 본다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'
    printf '%s 2 160 %s HALF_OPEN\n' 3200 "$NODES"
    printf '%s 2 180 %s HALF_OPEN\n' 3400 "$NODES"
    recovering 3600 200
    echo '# 해제'; echo '# 승계'; printf '%s 300 %s %s CLOSED\n' 5600 380 "$NODES"
} > "$work/passresidual.txt"
run_case "충족 회차도 잔여를 싣는다" 0 "잔여 *0.4초" -- "$work/passresidual.txt"

# **못 잰 것과 0 을 가른다.** 반쯤 열린 노드가 없으면 잴 잔여가 없는데, 0 으로
# 찍으면 "즉시 전이했다" 로 읽힌다.
run_case "잔여를 못 재면 -1 로 적는다" 0 "잔여 *-1.0초" -- "$(healthy_run noresidual.txt)"

# **봉우리가 기준선의 1.2 배를 넘으면 회복이 곧 2차 장애다** (RC4).
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'; printf '%s 2 160 %s CLOSED\n' 3200 "$NODES"
    echo '# 해제'
    printf '%s 4 168 %s CLOSED\n' 3400 "$NODES"
    printf '%s 300 308 %s CLOSED\n' 3600 "$NODES"
    echo '# 승계'; printf '%s 300 328 %s CLOSED\n' 3800 "$NODES"
} > "$work/burst.txt"
run_case "회복 봉우리가 크면 미달" 1 "봉우리" -- "$work/burst.txt"

# **풀린 뒤에는 한산 통과 상한이 1 이상이어야 한다** (R1). 노드 둘이면 크레딧
# 4 미만이다. 3 은 상한(노드 수 2)을 넘어 풀린 것으로 잡히면서도 최소에는 못 미친다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'; printf '%s 2 160 %s CLOSED\n' 3200 "$NODES"
    echo '# 해제'
    # 앞 1.5초는 유예다 — 해제 로그와 다음 배분 틱 사이의 간격이다.
    printf '%s 3 168 %s CLOSED\n' 3400 "$NODES"
    printf '%s 3 176 %s CLOSED\n' 4200 "$NODES"
    printf '%s 3 184 %s CLOSED\n' 5200 "$NODES"
    echo '# 승계'; printf '%s 6 192 %s CLOSED\n' 5400 "$NODES"
} > "$work/idle.txt"
run_case "풀린 뒤 한산 통과가 막히면 미달" 1 "한산 통과" -- "$work/idle.txt"

# **게이트가 안 풀리면 회복이 시작도 안 한 것이다.** 원인을 이름으로 부른다 —
# 그러지 않으면 램프의 기준이 대신 울려, 램프가 못 한 일처럼 적힌다. 실측에서
# 서킷이 프로브를 못 채워 이 자리가 먼저 걸렸다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'; gated 3200 160 2 10 OPEN 1
    printf '%s 1 175 1 OPEN\n' 5300
} > "$work/stuck.txt"
run_case "게이트가 안 풀리면 미달" 1 "안 닫혀" -- "$work/stuck.txt"
# 회복 첫 표본이 이미 활짝 열려 있으면 잴 잔여가 없다. 0 으로 찍으면 그 회차가
# "즉시 전이했다" 가 된다.
run_case "회복이 열린 채 시작하면 잔여는 -1" 1 "잔여 -1.0초" -- "$work/stuck.txt"

# **승계가 계단을 되살리면 안 된다.** 이어받은 노드는 조인 적이 없어 램프가
# 안 걸린다 — 게이트웨이가 둘 이상일 때만 열리는 구멍이라 여기서만 잡힌다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'
    printf '%s 4 160 %s CLOSED\n' 3200 "$NODES"
    printf '%s 8 168 %s CLOSED\n' 3400 "$NODES"
    echo '# 해제'; echo '# 승계'; printf '%s 300 188 %s CLOSED\n' 3600 "$NODES"
} > "$work/handover.txt"
run_case "승계가 계단을 되살리면 미달" 1 "승계" -- "$work/handover.txt"

# **리더를 죽이면 노드 수가 준다.** 그건 우리가 만든 자극이라 판정을 막지
# 않는다. 승계 뒤 최소도 그 줄어든 수를 따라가야 한다 — 시작 값을 박아 두면
# 문턱이 실제보다 두 배라 지킨 회차가 미달로 적힌다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'; printf '%s 2 160 %s CLOSED\n' 3200 "$NODES"
    echo '# 해제'; printf '%s 4 168 %s CLOSED\n' 3400 "$NODES"
    echo '# 승계'
    printf '%s 8 176 1 CLOSED\n' 3600
    printf '%s 16 184 1 CLOSED\n' 3800
    printf '%s 32 192 1 CLOSED\n' 4000
    printf '%s 64 200 1 CLOSED\n' 4200
    printf '%s 128 208 1 CLOSED\n' 4400
    printf '%s 256 216 1 CLOSED\n' 4600
    printf '%s 300 224 1 CLOSED\n' 4800
} > "$work/shrink.txt"
run_case "승계로 노드가 줄어도 충족" 0 "충족" -- "$work/shrink.txt"

# **서킷이 완전히 열렸다 돌아오는 회차.** 조인 동안의 몫이 0 이라, 승계가
# 회복의 첫 표본에 걸리면 앞 값이 0 이다 — 배수만으로는 허용이 0 이 되어, 한산
# 통과 최소를 지킨 회차가 미달로 적힌다. 그 자리를 하한이 받친다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 0 3
    echo '# 유지'; gated 2200 160 0 5
    echo '# 회복'
    echo '# 해제'; echo '# 승계'
    printf '%s 4 160 %s CLOSED\n' 3200 "$NODES"
    printf '%s 8 168 %s CLOSED\n' 3400 "$NODES"
    printf '%s 16 176 %s CLOSED\n' 3600 "$NODES"
    printf '%s 32 184 %s CLOSED\n' 3800 "$NODES"
    printf '%s 64 192 %s CLOSED\n' 4000 "$NODES"
    printf '%s 128 200 %s CLOSED\n' 4200 "$NODES"
    printf '%s 256 208 %s CLOSED\n' 4400 "$NODES"
    printf '%s 300 216 %s CLOSED\n' 4600 "$NODES"
} > "$work/fullopen.txt"
run_case "전면 정지에서 돌아와도 충족" 0 "충족" -- "$work/fullopen.txt"

# **첫 표본만 보면 계단을 놓친다.** 이어받은 노드는 제 스무더를 이월받고 첫
# 회차를 도느라, 계단이 둘째나 셋째 틱에 선다. 실측 리뷰가 정확히 이 모양으로
# 판정기를 뚫었다 — 16 을 지나 그다음 틱에 300 이 나갔는데 충족이 나왔다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'; printf '%s 4 160 %s CLOSED\n' 3200 "$NODES"
    echo '# 해제'; printf '%s 8 168 %s CLOSED\n' 3400 "$NODES"
    echo '# 승계'
    printf '%s 16 176 %s CLOSED\n' 3600 "$NODES"
    printf '%s 300 196 %s CLOSED\n' 3800 "$NODES"
} > "$work/second.txt"
run_case "승계 둘째 틱의 계단도 잡는다" 1 "승계" -- "$work/second.txt"

# **표가 닫혔는데 게이트가 안 풀리는 구간을 따로 잰다.** 크레딧만 보면 "서킷이
# 아직 안 닫혔다" 와 "표는 닫혔는데 게이트가 안 풀렸다" 가 같은 그림인데, 둘은
# 고칠 자리가 다르다 — 앞엣것은 서킷 설정이고 뒤엣것은 클러스터 표의 완화다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    # 전 노드가 닫혔다고 하는데도 십여 초 동안 조인 채다.
    echo '# 회복'; gated 3200 160 2 60 CLOSED 1
    echo '# 해제'; printf '%s 4 230 %s CLOSED\n' 15400 "$NODES"
    echo '# 승계'
    printf '%s 8 238 %s CLOSED\n' 15600 "$NODES"
    printf '%s 16 246 %s CLOSED\n' 15800 "$NODES"
    printf '%s 32 254 %s CLOSED\n' 16000 "$NODES"
    printf '%s 64 262 %s CLOSED\n' 16200 "$NODES"
    printf '%s 128 270 %s CLOSED\n' 16400 "$NODES"
    printf '%s 256 278 %s CLOSED\n' 16600 "$NODES"
    printf '%s 300 286 %s CLOSED\n' 16800 "$NODES"
} > "$work/votegap.txt"
run_case "표가 닫혔는데 게이트가 오래 조이면 미달" 1 "표는 닫혔는데" \
    -- "$work/votegap.txt"

# **표가 닫혔는데 끝내 안 풀린 회차는 완화 탓이다.** 표를 안 보면 그 회차가
# 서킷 탓으로 나가고, 다음 사람이 엉뚱한 데를 고친다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'; gated 3200 160 2 80 CLOSED 1
} > "$work/easestuck.txt"
run_case "표가 닫혔는데 끝내 안 풀리면 완화 탓" 1 "끝내 안 풀렸다" \
    -- "$work/easestuck.txt"

# **부하가 먼저 끝난 회차를 제품 미달로 안 내보낸다.** 앞선 회차가 그 꼬리를
# 분모에 넣고 결론을 냈다. 러너의 산술이 다시 어긋나도 여기서 끊긴다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    # 배분이 열려 있는데(크레딧 2) 열 초 넘게 도착이 없다.
    echo '# 회복'; gated 3200 160 2 60 OPEN 0
} > "$work/deadtail.txt"
run_case "부하가 먼저 끝나면 판정 불가" 2 "부하가 먼저 끝났다" -- "$work/deadtail.txt"

# **서킷이 활짝 열린 구간은 도착이 없는 것이 정상이다.** 크레딧 0 인 동안까지
# 세면 제품이 제 일을 한 회차를 판정 불가로 덮는다 — 이 하네스가 찾으려던
# 실패가 바로 거기 있다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 0 3
    echo '# 유지'; gated 2200 160 0 5
    echo '# 회복'; gated 3200 160 0 60 OPEN 0
} > "$work/openflat.txt"
run_case "활짝 열린 구간의 도착 0 은 정상" 1 "안 닫혀" -- "$work/openflat.txt"

# 유지 구간에 표본이 없으면 조임이 붙어 있었는지를 한 번도 안 본다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'
    echo '# 회복'; recovering 3200 160
    echo '# 해제'; echo '# 승계'; printf '%s 300 340 %s CLOSED\n' 5200 "$NODES"
} > "$work/nohold.txt"
run_case "유지 구간이 비면 판정 불가" 2 "유지 구간에 표본이 없다" -- "$work/nohold.txt"

# ── 판정 불가 ────────────────────────────────────────────────────────────────

# **느는 것은 자극이 아니라 오염이다.** 앞 회차의 등록이 살아나거나 다른 스택이
# 붙은 것이고, 그러면 한산 통과 문턱이 회차 중에 올라간다.
{
    echo '# 정상'; normal 0 0
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'
    printf '%s 4 160 %s CLOSED\n' 3200 "$NODES"
    printf '%s 300 180 3 CLOSED\n' 3400
    echo '# 해제'; echo '# 승계'; printf '%s 300 200 3 CLOSED\n' 3600
} > "$work/grow.txt"
run_case "노드가 늘면 판정 불가" 2 "늘었다" -- "$work/grow.txt"

# **옛 네 칸 형식을 조용히 받지 않는다.** 받으면 표 칸이 빈 채로 지나가고,
# 층을 가르는 검사가 통째로 잠든다.
{
    echo '# 정상'; printf '%s 300 0 %s\n' 0 "$NODES"; normal 200 20 3
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'; recovering 3200 160
    echo '# 해제'; echo '# 승계'; printf '%s 300 340 %s CLOSED\n' 5200 "$NODES"
} > "$work/fourcol.txt"
run_case "옛 네 칸 형식은 막는다" 2 "열이 5 개가 아니다" -- "$work/fourcol.txt"

# 표 칸이 상태 문자열이 아니면 무엇을 읽은 것인지 모른다.
{
    echo '# 정상'; printf '%s 300 0 %s closed!\n' 0 "$NODES"; normal 200 20 3
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'; recovering 3200 160
    echo '# 해제'; echo '# 승계'; printf '%s 300 340 %s CLOSED\n' 5200 "$NODES"
} > "$work/badvote.txt"
run_case "표가 상태 문자열이 아니면 막는다" 2 "상태 문자열이 아니다" -- "$work/badvote.txt"

: > "$work/empty.txt"
run_case "표본이 비면 판정 불가" 2 "표본이 비었다" -- "$work/empty.txt"
run_case "표본 파일이 없으면 판정 불가" 2 "표본이 비었다" -- "$work/없는파일.txt"

{
    normal 0 0
    recovering 3200 160
} > "$work/nomark.txt"
run_case "구간 표시가 없으면 판정 불가" 2 "구간 표시" -- "$work/nomark.txt"

# **한 대짜리 회차는 이 시나리오가 아니다.** 승계도 쏠림도 원리적으로 안 생긴다.
{
    echo '# 정상'; printf '%s 300 0 1 CLOSED\n' 0; printf '%s 300 20 1 CLOSED\n' 200
    echo '# 진입'; printf '%s 1 20 1 OPEN\n' 1600
    echo '# 유지'; printf '%s 1 20 1 OPEN\n' 2200
    echo '# 회복'; printf '%s 2 20 1 OPEN\n' 3200
    echo '# 해제'; printf '%s 4 30 1 CLOSED\n' 3400
    echo '# 승계'
    printf '%s 8 40 1 CLOSED\n' 3600
    printf '%s 16 50 1 CLOSED\n' 3800
    printf '%s 32 60 1 CLOSED\n' 4000
    printf '%s 64 70 1 CLOSED\n' 4200
    printf '%s 128 80 1 CLOSED\n' 4400
    printf '%s 256 90 1 CLOSED\n' 4600
    printf '%s 300 100 1 CLOSED\n' 4800
} > "$work/single.txt"
run_case "노드가 하나면 판정 불가" 2 "게이트웨이가" -- "$work/single.txt"

{
    echo '# 정상'; normal 0 0
    echo '# 진입'; printf '%s 오류 160 %s CLOSED\n' 1600 "$NODES"
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'; recovering 3200 160
    echo '# 해제'; echo '# 승계'; printf '%s 300 340 %s CLOSED\n' 5200 "$NODES"
} > "$work/bad.txt"
run_case "표본이 숫자가 아니면 판정 불가" 2 "표본이 숫자가 아니다" -- "$work/bad.txt"

# **기준선을 못 만들면 판정할 게 없다.** 정상 구간이 짧으면 그 회차의 목표를 모른다.
#
# 표본을 셋 준다. 하나만 주면 유입도 0 이 되어, 기준선 가드를 지워도 유입 가드가
# 대신 막는다 — 어느 가드가 일했는지 안 갈린다.
{
    echo '# 정상'; normal 0 0 3
    echo '# 진입'; gated 1600 160 2 3
    echo '# 유지'; gated 2200 160 2 5
    echo '# 회복'; recovering 3200 160
    echo '# 해제'; echo '# 승계'; printf '%s 300 340 %s CLOSED\n' 5200 "$NODES"
} > "$work/short.txt"
run_case "정상 구간이 짧으면 판정 불가" 2 "기준선 표본이" -- "$work/short.txt"

# **누적은 줄 수 없다.** 줄었으면 회차 중에 뒷단이 다시 떴다는 뜻이라, 도착
# 셈이 그 앞의 트래픽을 통째로 빼먹는다.
{
    echo '# 정상'; normal 0 500
    echo '# 진입'; gated 1600 100 2 3
    echo '# 유지'; gated 2200 100 2 5
    echo '# 회복'; recovering 3200 100
    echo '# 해제'; echo '# 승계'; printf '%s 300 280 %s CLOSED\n' 5200 "$NODES"
} > "$work/reset.txt"
run_case "도착이 줄면 판정 불가" 2 "다시 떴다" -- "$work/reset.txt"

# 기준선 유입이 0 이면 회복 봉우리를 어디에 견줄지가 없다.
{
    echo '# 정상'; gated 0 0 300 8
    echo '# 진입'; gated 1600 0 2 3
    echo '# 유지'; gated 2200 0 2 5
    echo '# 회복'; recovering 3200 0
    echo '# 해제'; echo '# 승계'; printf '%s 300 180 %s CLOSED\n' 5200 "$NODES"
} > "$work/noload.txt"
run_case "기준선 유입이 0 이면 판정 불가" 2 "부하가 안 닿았다" -- "$work/noload.txt"

[ "$selftest_failed" -eq 0 ] && echo "서킷 회복 자기검증 통과" || echo "서킷 회복 자기검증 실패"
exit "$selftest_failed"

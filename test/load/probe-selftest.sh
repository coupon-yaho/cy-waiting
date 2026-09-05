#!/usr/bin/env bash
# 레디스 프로브의 자기검증 (TS-9).
#
# **이 프로브가 Phase 10 착수를 정하는 값을 만든다.** 시계가 틀리면 판정이 늘
# "착수" 로 나오고, 그것이 계기 고장인지 진짜 부하인지 아무도 못 가른다.
# 실제로 컨테이너의 `date` 가 나노초를 안 내서 초를 나노초로 읽었고, 값이
# 10 억 배로 부푼 채 판정이 그대로 통과했다.
#
# **루프를 진짜로 돈다.** 표본만 먹이면 프로브가 컨테이너에서 **무엇을 부르는지**
# 가 안 검증된다 — 고친 자리가 바로 그 명령 문자열이라, 그것을 대체해 버리면
# 시계를 되돌려도 초록이다. 여기서는 `redis-cli` 만 가짜로 바꾼다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT
failed=0

# 가짜 `redis-cli`. `TIME` 이면 정해진 시각을, `INFO cpu` 면 정해진 CPU 누적을,
# `INFO stats` 면 정해진 카운터를 낸다. 한 회차가 한 줄씩 소비한다.
#
# **`INFO cpu` 는 소수 그대로 낸다.** 정수로 바꾸는 것은 프로브가 컨테이너
# 안에서 awk 로 하는 일이라, 여기서 미리 바꾸면 그 자리가 안 검증된다.
#
# **프로세스 전체 필드도 같이 낸다 — 값을 다르게 둔다.** 프로브가 그쪽을 읽으면
# 값이 어긋나 시험이 빨개진다. 안 그러면 필드를 되돌려도 초록이다.
mkdir -p "$work/bin"
cat > "$work/bin/redis-cli" <<'FAKE'
#!/usr/bin/env bash
d=$(dirname "$0")/..
n=$(cat "$d/turn" 2>/dev/null || echo 0)
case "$*" in
    TIME)
        line=$(sed -n "$((n + 1))p" "$d/times")
        # `-` 한 줄이면 TIME 이 한 줄만 온 경우다. `x` 면 아예 안 온 경우다.
        case "$line" in
            x) ;;
            *) printf '%s\n' $line ;;
        esac
        ;;
    *cpu*|*CPU*)
        line=$(sed -n "$((n + 1))p" "$d/cpus")
        case "$line" in
            x) ;;
            *) printf 'used_cpu_sys:99\nused_cpu_user:99\n' ;;&
            *) printf 'used_cpu_sys_main_thread:0\nused_cpu_user_main_thread:%s\n' "$line" ;;
        esac
        ;;
    *INFO*|*info*)
        printf 'total_commands_processed:%s\n' "$(sed -n "$((n + 1))p" "$d/counts")"
        echo $(( n + 1 )) > "$d/turn"
        ;;
esac
FAKE
chmod +x "$work/bin/redis-cli"

# 프로브가 부르는 자리를 **진짜 셸**로 둔다. 루프 문자열이 그대로 실행된다.
run_probe() {
    printf '%s\n' "$1" > "$work/times"
    printf '%s\n' "$2" > "$work/counts"
    # CPU 누적. 안 주면 0 초로 둔다 — 그 사례들이 보는 것은 명령 칸이다.
    printf '%s\n' "${3:-0
0
0
0}" > "$work/cpus"
    echo 0 > "$work/turn"
    : > "$work/out"
    # **걷는 자리도 가짜로 둔다.** 안 두면 종료 트랩이 기본값을 실제로 실행해
    # 진짜 도커 스택의 프로브 루프를 걷는다 — 다른 워크트리에서 회차가 도는
    # 중이면 그쪽 표본이 끊기고, 출력이 버려져 흔적도 안 남는다.
    PATH="$work/bin:$PATH" PROBE_CMD="bash -c" PROBE_INTERVAL_SEC=0.01 \
        PROBE_STOP="true" timeout 10 test/load/redis-probe.sh "$work/out" >/dev/null 2>&1
}

# 출력은 `<CPU×100> <창ms> <명령/초>` 세 칸이다. 기본으로 명령 칸을 본다.
expect() {
    local name=$1 want=$2 col=${3:-3}
    got=$(cut -d' ' -f"$col" "$work/out" 2>/dev/null | tr '\n' ' ' | sed 's/ $//')
    if [ "$got" = "$want" ]; then
        echo "  ✓ $name"
    else
        echo "  ✗ $name — 나온 값 '$got' (기대 '$want')"
        failed=1
    fi
}

echo "레디스 프로브 자기검증"

# 실제 `redis-cli TIME` 은 마이크로초를 0 으로 안 채운다. 그 모양 그대로 쓴다.
# 0.2 초에 200 건이면 초당 1000 건이다.
run_probe '1788587000 0
1788587000 200000' '5000
5200'
expect "차분을 실제 흐른 시간으로 나눈다" "1000"

# **초 단위 시계면 빨개진다.** 마이크로초가 늘 0 이면 1 초 창만 살아남는다.
run_probe '1788587000 0
1788587001 0' '5000
5008'
expect "1초에 8건이면 초당 8건" "8"

# 마이크로초가 다섯 자리 이하로 와도 8 진수로 안 읽는다.
run_probe '1788587000 12345
1788587000 62345' '5000
5050'
expect "앞자리 0 이 없어도 읽는다" "1000"

# 창이 0 이면 나눌 수 없다. 그 표본은 버린다.
run_probe '1788587000 500
1788587000 500' '5000
5100'
expect "창이 0 이면 안 적는다" ""

# 누적이 되돌아가면 안 적는다.
run_probe '1788587000 0
1788587000 500000' '5000
10'
expect "누적이 되돌아가면 안 적는다" ""

# **마이크로초 칸이 비면 버린다.** 이어 붙여 검사하면 그 빈 값이 0 으로 읽혀
# 다시 초 단위 시계가 된다 — 고친 버그가 조용히 돌아오는 자리다.
run_probe '1788587000
1788587001' '5000
5200'
expect "마이크로초가 없으면 안 적는다" ""

# **시각이 통째로 빠지면 그 표본을 버린다.** 안 버리면 낡은 시각과 새 카운터가
# 짝지어져 창이 두 배가 되고 값이 절반으로 나온다.
run_probe '1788587000 0
x
1788587001 0' '5000
5200
5400'
expect "시각이 빠진 표본은 안 적는다" "400"

# 표본이 셋이면 값도 둘이다.
run_probe '1788587000 0
1788587000 500000
1788587001 0' '0
50
150'
expect "표본마다 한 줄" "100 200"

# **창도 같이 적는다.** 비율만 있으면 계기 고장이 부하로 읽힌다.
run_probe '1788587000 0
1788587000 250000' '0
25'
window=$(cut -d' ' -f2 "$work/out" 2>/dev/null | tr -d '\n')
if [ "$window" = "250" ]; then
    echo "  ✓ 창을 밀리초로 같이 적는다"
else
    echo "  ✗ 창을 밀리초로 같이 적는다 — 나온 값 '$window' (기대 '250')"
    failed=1
fi

# **CPU 칸이 판정을 낸다.** 여기가 조용히 0 이면 게이트는 늘 "보류" 다 —
# 샤딩이 필요한 상황에서도 안 열린다.
#
# 0.2 초 창에서 CPU 가 0.1 초 늘면 50% 다. 백분율에 100 을 곱해 적으므로 5000.
run_probe '1788587000 0
1788587000 200000' '0
0' '0
0.1'
expect "CPU 사용률을 백분율×100 으로 적는다" "5000" 1

# 두 배 걸리면 두 배다. 한 사례만 두면 어떤 상수로 나눠도 시험이 초록이다.
run_probe '1788587000 0
1788587000 200000' '0
0' '0
0.05'
expect "절반이면 절반으로 적는다" "2500" 1

# **소수 자리를 안 버린다.** 컨테이너 안의 awk 가 마이크로초 정수로 바꾸는데,
# 그 자리를 잘라 쓰면 유휴 구간이 통째로 0% 로 찍힌다 — 그러면 평균이 낮아져
# 천장이 부풀고, 샤딩이 필요 없다는 결론으로 기운다.
run_probe '1788587000 0
1788587000 200000' '0
0' '0
0.0037'
expect "소수 자리를 버리지 않는다" "185" 1

# **CPU 가 되돌아가면 안 적는다.** 레디스가 재시작하면 누적이 0 으로 간다.
run_probe '1788587000 0
1788587000 200000
1788587000 400000' '0
0
0' '0
0.1
0.05'
expect "CPU 가 되돌아가면 안 적는다" "5000" 1

# **CPU 줄이 안 오면 그 표본을 버린다.** 안 버리면 낡은 누적과 새 시각이
# 짝지어져 사용률이 실제보다 낮게 나온다 — 조용히 "보류" 쪽으로 기운다.
run_probe '1788587000 0
1788587000 200000
1788587000 400000' '0
0
0' '0
x
0.2'
expect "CPU 가 빠진 표본은 안 적는다" "5000" 1

# **주기가 `sleep` 이 못 읽는 값이면 안 돌아야 한다.** 숫자와 점만 보는 검사는
# `1..2` 나 `.` 도 통과시키는데, 그러면 루프가 실패한 `sleep` 을 무시하고 레디스를
# 쉼 없이 친다 — 재려던 대상을 계기가 흔들고, stderr 가 버려져 안 보인다.
#
# **종료 코드만 보면 안 된다.** 가드를 없애도 루프가 시한에 걸려 끊기므로 종료
# 코드는 똑같이 0 이 아니다 — 그 사례는 가드가 없어도 초록이다. 거절 메시지를
# 직접 문다. 빈 값은 기본값 0.2 로 떨어지므로 잘못된 값이 아니다.
for bad in '1..2' '.' '0' '-1' 'abc' '1 2'; do
    err=$(PATH="$work/bin:$PATH" PROBE_CMD="bash -c" PROBE_STOP="true" \
        PROBE_INTERVAL_SEC="$bad" timeout 5 test/load/redis-probe.sh "$work/out" 2>&1 >/dev/null)
    if printf '%s' "$err" | grep -q '0 보다 큰 수여야 한다'; then
        echo "  ✓ 주기 '$bad' 를 막는다"
    else
        echo "  ✗ 주기 '$bad' 를 안 막는다 — 나온 것 '$err'"
        failed=1
    fi
done

# **정상 값은 막지 않는다.** 위 검사를 너무 좁히면 소수 주기가 안 돈다.
run_probe '1788587000 0
1788587000 200000' '0
25'
expect "정수 아닌 주기도 돈다" "125"

# **컨테이너 안의 루프까지 걷는지 본다.** `kill` 은 도커 클라이언트만 죽이고
# 안쪽 프로세스는 남는다 — 남은 루프는 계속 레디스를 쳐서 다음 회차의 누적
# 명령 수에 제 몫을 얹는다. 실제로 여덟 개가 살아 있었다.
printf '1788587000 0\n1788587000 250000\n' > "$work/times"
printf '0\n25\n' > "$work/counts"
echo 0 > "$work/turn"
: > "$work/out"
cat > "$work/bin/reap" <<'REAP'
#!/usr/bin/env bash
printf '%s
' "$*" >> "$REAP_LOG"
REAP
chmod +x "$work/bin/reap"
: > "$work/reaped"
PATH="$work/bin:$PATH" PROBE_CMD="bash -c" PROBE_INTERVAL_SEC=0.01 \
    REAP_LOG="$work/reaped" PROBE_STOP="reap" \
    timeout 10 test/load/redis-probe.sh "$work/out" >/dev/null 2>&1

if grep -q 'redis-probe-loop' "$work/reaped" 2>/dev/null; then
    echo "  ✓ 끝날 때 안쪽 루프를 걷는다"
else
    echo "  ✗ 끝날 때 안쪽 루프를 걷는다 — 부른 기록이 없다"
    failed=1
fi

# 걷을 때 쓰는 표식이 실제로 루프 문자열에 붙어 있어야 한다. 안 붙어 있으면
# 위 사례는 초록인데 컨테이너 안에서는 아무것도 안 걷힌다.
cat > "$work/bin/record" <<'REC'
#!/usr/bin/env bash
printf '%s
' "$*" > "$REC_LOG"
REC
chmod +x "$work/bin/record"
: > "$work/loopcmd"
PATH="$work/bin:$PATH" PROBE_CMD="record" REC_LOG="$work/loopcmd" \
    PROBE_STOP="true" timeout 10 test/load/redis-probe.sh "$work/out" >/dev/null 2>&1
if grep -q 'redis-probe-loop' "$work/loopcmd" 2>/dev/null; then
    echo "  ✓ 표식이 루프 문자열에 붙는다"
else
    echo "  ✗ 표식이 루프 문자열에 붙는다 — 루프에 표식이 없다"
    failed=1
fi

exit "$failed"

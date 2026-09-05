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

# 가짜 `redis-cli`. `TIME` 이면 정해진 시각을, `INFO` 면 정해진 카운터를 낸다.
# 한 회차가 한 줄씩 소비한다.
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
    echo 0 > "$work/turn"
    : > "$work/out"
    PATH="$work/bin:$PATH" PROBE_CMD="bash -c" PROBE_INTERVAL_SEC=0.01 \
        timeout 10 test/load/redis-probe.sh "$work/out" >/dev/null 2>&1
}

expect() {
    local name=$1 want=$2
    got=$(cut -d' ' -f1 "$work/out" 2>/dev/null | tr '\n' ' ' | sed 's/ $//')
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

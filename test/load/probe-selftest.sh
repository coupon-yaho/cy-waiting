#!/usr/bin/env bash
# 레디스 프로브의 자기검증 (TS-9).
#
# **이 프로브가 Phase 10 착수를 정하는 값을 만든다.** 시계가 틀리면 판정이
# 늘 "착수" 로 나오고, 그것이 계기 고장인지 진짜 부하인지 아무도 못 가른다.
# 실제로 컨테이너의 `date` 가 나노초를 안 내서 초를 나노초로 읽고 있었다 —
# 값이 10 억 배 부풀었고 판정은 그대로 통과했다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

work=$(mktemp -d) || exit 1
trap 'rm -rf "$work"' EXIT
failed=0

# 프로브가 부르는 자리를 가짜로 바꿔 넣는다. 인자는 루프 문자열인데 우리는
# 그것을 무시하고 정해진 표본을 낸다.
가짜() {
    printf '%s\n' "$1" > "$work/feed"
    PROBE_CMD="$work/fake.sh" INTERVAL=0.01 timeout 10 test/load/redis-probe.sh "$work/out" \
        >/dev/null 2>&1
}

cat > "$work/fake.sh" <<'FAKE'
#!/usr/bin/env bash
cat "$(dirname "$0")/feed"
FAKE
chmod +x "$work/fake.sh"

본다() {
    local name=$1 want=$2
    got=$(cat "$work/out" 2>/dev/null | tr '\n' ' ' | sed 's/ $//')
    if [ "$got" = "$want" ]; then
        echo "  ✓ $name"
    else
        echo "  ✗ $name — 나온 값 '$got' (기대 '$want')"
        failed=1
    fi
}

echo "레디스 프로브 자기검증"

# 0.2 초에 200 건이면 초당 1000 건이다.
가짜 'T1000:000000:
total_commands_processed:5000
T1000:200000:
total_commands_processed:5200'
본다 "차분을 실제 흐른 시간으로 나눈다" "1000"

# **초 단위 시계로 읽히면 안 된다.** 같은 표본이 10 억 배로 나오던 자리다.
가짜 'T1000:000000:
total_commands_processed:5000
T1001:000000:
total_commands_processed:5008'
본다 "1초에 8건이면 초당 8건" "8"

# 창이 0 이면 나눌 수 없다. 그 표본은 버린다.
가짜 'T1000:000000:
total_commands_processed:5000
T1000:000000:
total_commands_processed:5100'
본다 "창이 0 이면 안 적는다" ""

# 레디스가 재시작하면 누적이 되돌아간다. 음수 차분은 안 적는다.
가짜 'T1000:000000:
total_commands_processed:5000
T1000:500000:
total_commands_processed:10'
본다 "누적이 되돌아가면 안 적는다" ""

# 표본이 셋이면 값도 둘이다.
가짜 'T1000:000000:
total_commands_processed:0
T1000:500000:
total_commands_processed:50
T1001:000000:
total_commands_processed:150'
본다 "표본마다 한 줄" "100 200"

exit "$failed"

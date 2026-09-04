#!/usr/bin/env bash
# 죽은 주소를 섞었을 때의 유출 판정 (G9.11 · 9.3.11).
#
# 종료 0 충족 · 1 미달 · 2 판정 불가.
#
#   사용: evaluate-dead-addr.sh <보낸것> <200> <200아닌것> <산대도착> <인스턴스최고> <배제최고>
#
# **닿았는지부터 본다.** 죽은 주소를 아무도 안 골랐으면 이 회차는 재시도를 한
# 번도 안 밟은 것이고, 그 "유출 0" 은 죽은 주소와 무관하다 — 통과로 적으면
# 게이트가 사라진다.
set -uo pipefail

[ $# -eq 6 ] || { echo "인자가 여섯이어야 한다: $*" >&2; exit 2; }
for value in "$@"; do
    case "$value" in
        ''|*[!0-9]*) echo "판정 불가 — 인자는 0 이상의 정수여야 한다: '$value'" >&2; exit 2 ;;
    esac
done
sent=$1 ok=$2 bad=$3 live=$4 instances=$5 ejected=$6

[ "$sent" -gt 0 ] || { echo "판정 불가 — 보낸 것이 0 이다"; exit 2; }
[ "$(( ok + bad ))" -le "$sent" ] || {
    echo "판정 불가 — 200 $ok 건과 아닌 것 $bad 건의 합이 보낸 $sent 건보다 많다"; exit 2; }

# **넷이 안 보였으면 죽은 주소가 걸러진 것이다.** 산 셋만으로 돈 회차의 유출 0
# 은 이 게이트와 무관하다.
if [ "$instances" -lt 4 ]; then
    echo "판정 불가 — 인스턴스가 최고 $instances 대다. 죽은 주소가 목록에 안 들어왔다"
    exit 2
fi
if [ "$ejected" -lt 1 ]; then
    echo "판정 불가 — 배제가 한 번도 안 올랐다. 죽은 주소로 간 요청이 없다"
    exit 2
fi

if [ "$bad" -ne 0 ]; then
    echo "미달 — 보낸 $sent 건 중 $bad 건이 200 이 아니다. 죽은 주소가 5xx 로 샌다"
    exit 1
fi
# 재시도가 산 대로 넘겼으면 도착이 200 만큼은 된다. 모자라면 어딘가에서
# 사라진 것이고 그것도 유출이다.
if [ "$live" -lt "$ok" ]; then
    echo "미달 — 200 이 $ok 건인데 산 대에 닿은 것은 $live 건이다. 나머지는 어디로 갔나"
    exit 1
fi
echo "충족 — 죽은 주소를 섞어도 5xx 유출 0 이고, 재시도가 산 대로 넘긴다"

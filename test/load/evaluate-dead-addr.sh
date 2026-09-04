#!/usr/bin/env bash
# 죽은 주소를 섞었을 때의 유출 판정 (G9.11 · 9.3.11).
#
# 종료 0 충족 · 1 미달 · 2 판정 불가.
#
#   사용: evaluate-dead-addr.sh <보낸것> <200> <202> <나머지> <산대도착> <노출%> \
#           <배제최고> <배제켬:0|1>
#
# **202 는 미달이 아니라 판정 불가다.** 줄이 켜졌다는 것은 요청이 뒷단으로 안
# 갔다는 뜻이라 죽은 주소를 밟은 적이 없다 — 그것을 "5xx 로 샌다" 로 적으면
# 하네스 조건이 어긋난 회차를 제품 결함으로 읽는다.
#
# **닿았는지부터 본다.** 죽은 주소를 아무도 안 골랐으면 이 회차는 재시도를 한
# 번도 안 밟은 것이고, 그 "유출 0" 은 죽은 주소와 무관하다 — 통과로 적으면
# 게이트가 사라진다.
set -uo pipefail

# 도착이 적으면 다 200 인 것이 우연이다. 형제 판정기들과 같은 이유로 바닥을 둔다.
MIN_SENT="${MIN_SENT:-400}"
case "$MIN_SENT" in
    ''|*[!0-9]*) echo "MIN_SENT 는 0 이상의 정수여야 한다: '$MIN_SENT'" >&2; exit 2 ;;
esac

[ $# -eq 8 ] || { echo "인자가 여덟이어야 한다: $*" >&2; exit 2; }
for value in "$@"; do
    case "$value" in
        ''|*[!0-9]*) echo "판정 불가 — 인자는 0 이상의 정수여야 한다: '$value'" >&2; exit 2 ;;
    esac
done
sent=$1 ok=$2 queued=$3 other=$4 live=$5 exposure=$6 ejected=$7 ejecting=$8

[ "$sent" -ge "$MIN_SENT" ] || {
    echo "판정 불가 — 보낸 것이 $sent 건뿐이다 (최소 $MIN_SENT). 다 200 인 것이 우연일 수 있다"
    exit 2; }
[ "$(( ok + queued + other ))" -le "$sent" ] || {
    echo "판정 불가 — 코드별 합이 보낸 $sent 건보다 많다"; exit 2; }

# **줄이 켜졌으면 뒷단에 안 갔다.** 죽은 주소를 밟은 적이 없으므로 이 회차는
# 이 게이트와 무관하다 — 미달로 적으면 하네스 조건을 제품 결함으로 읽는다.
if [ "$queued" -ne 0 ]; then
    echo "판정 불가 — $queued 건이 줄로 갔다. 유입이 창을 벗어나 뒷단 경로를 안 밟았다"
    exit 2
fi

# **잠깐 스친 것으로는 못 잰다.** 죽은 주소가 회차의 대부분 동안 후보여야 그
# 몫만큼 재시도가 걸리고, 그때의 "전부 200" 이 재시도의 값이 된다. 노출이 낮으면
# 유출 0 은 그냥 안 갔다는 뜻이다.
MIN_EXPOSURE="${MIN_EXPOSURE:-80}"
case "$MIN_EXPOSURE" in
    ''|*[!0-9]*) echo "MIN_EXPOSURE 는 0 이상의 정수여야 한다: '$MIN_EXPOSURE'" >&2; exit 2 ;;
esac
if [ "$exposure" -lt "$MIN_EXPOSURE" ]; then
    echo "판정 불가 — 죽은 주소가 회차의 $exposure% 만 후보였다 (최소 $MIN_EXPOSURE%)"
    exit 2
fi
# **배제를 켠 회차에서만 이것이 증거다.** 끈 회차에서는 배제가 0 이 정상이고,
# 죽은 주소로 갔다는 증거는 다른 데서 온다 — 재시도를 끈 회차가 미달로 끊기는
# 것이 그것이다. 그 대조 없이 끈 회차의 "전부 200" 을 믿으면 안 된다.
if [ "$ejecting" -eq 1 ] && [ "$ejected" -lt 1 ]; then
    echo "판정 불가 — 배제가 한 번도 안 올랐다. 죽은 주소로 간 요청이 없다"
    exit 2
fi
if [ "$ejecting" -eq 0 ] && [ "$ejected" -ne 0 ]; then
    echo "판정 불가 — 배제를 껐는데 $ejected 대가 빠졌다. 설정이 안 섰다"
    exit 2
fi

if [ "$other" -ne 0 ]; then
    echo "미달 — 보낸 $sent 건 중 $other 건이 200 도 202 도 아니다. 죽은 주소가 5xx 로 샌다"
    exit 1
fi
# 재시도가 산 대로 넘겼으면 도착이 200 만큼은 된다. 모자라면 어딘가에서
# 사라진 것이고 그것도 유출이다.
if [ "$live" -lt "$ok" ]; then
    echo "미달 — 200 이 $ok 건인데 산 대에 닿은 것은 $live 건이다. 나머지는 어디로 갔나"
    exit 1
fi
if [ "$ejecting" -eq 0 ]; then
    echo "충족 — 배제를 끄고 죽은 주소를 회차 내내 후보로 둬도 5xx 유출 0 이다"
else
    echo "충족 — 죽은 주소를 섞어도 5xx 유출 0 이고, 배제가 그 대를 뺀다"
fi

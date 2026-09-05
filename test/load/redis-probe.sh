#!/usr/bin/env bash
# 부하가 도는 동안 레디스가 실제로 얼마나 바쁜지를 잰다.
#
# **응답 지표로는 못 잰다.** 게이트웨이가 빨리 답해도 레디스가 한계에 닿아 있으면
# 다음 단계에서 무너진다 — Phase 10 착수를 정하는 것은 그쪽 수치다.
#
# 사용: redis-probe.sh <출력파일> &   … 부하 … ; kill %1
set -uo pipefail

out=${1:?출력 파일}
interval=${PROBE_INTERVAL_SEC:-0.2}

# **`instantaneous_ops_per_sec` 를 안 쓴다.** 그 값은 레디스가 100ms 마다 찍은
# 표본 열여섯의 평균이라 **약 1.6 초 창**이다 — 아무리 자주 물어도 200ms 봉우리가
# 그 평균에 녹아 사라진다. 재려는 것이 정확히 그 봉우리다.
#
# 누적 명령 수의 차분을 직접 잰다. 창의 길이를 우리가 정한다.
#
# **표본마다 프로세스를 안 띄운다.** `docker compose exec` 를 매번 부르면 그 왕복이
# 창에 통째로 더해져, 200ms 를 재려는데 창이 그보다 훨씬 길어진다 — 봉우리가 그만큼
# 묽어져 착수 판정이 낮게 나온다.
#
# **시각을 표본 옆에서 찍는다.** 밖에서 받은 뒤에 찍으면 출력이 버퍼에 몰렸다
# 한꺼번에 나올 때 시각이 뭉쳐, 창이 0 에 가까워지고 값이 터무니없이 커진다.
probe=${PROBE_CMD:-docker compose -f test/load/compose.yml exec -T redis sh -c}
# **컨테이너 안의 루프는 따로 내려야 한다.** `kill` 은 도커 클라이언트만 죽이고
# 안쪽 프로세스는 그대로 남는다 — 실제로 회차를 거듭할수록 남은 루프가 쌓여
# 여덟 개가 살아 있었다. 그 루프들이 계속 레디스를 치므로 다음 회차의 누적
# 명령 수에 제 몫을 얹는다. 위의 주석이 걱정한 그대로인데 바깥만 막고 있었다.
PROBE_MARK=${PROBE_MARK:-redis-probe-loop}
probe_stop=${PROBE_STOP:-docker compose -f test/load/compose.yml exec -T redis pkill -f}
# **시계는 레디스에게 묻는다.** 컨테이너의 `date` 가 `%N` 을 안 받아 초만 내는데,
# 그것을 나노초로 읽으면 창이 1 이 되어 값이 10 억 배로 부푼다 — 실제로 그렇게
# 돌았고 판정은 늘 "착수" 였다. `TIME` 은 초와 마이크로초를 준다.
#
# **표식을 앞에 단다.** 아래에서 이 루프를 컨테이너 안에서 찾아 내리는 데 쓴다.
loop=": ${PROBE_MARK}; while :; do printf 'T%s\\n' \"\$(redis-cli TIME | tr '\\n' ':')\"; redis-cli INFO stats | grep total_commands_processed; sleep $interval; done"

: > "$out"
# **자식을 짚어서 내린다.** 파이프라인으로 두면 부르는 쪽이 이 스크립트만 죽이고
# 자식은 남는다 — 배시는 자식까지 신호를 안 보낸다. 그러면 다음 시나리오가 도는
# 내내 레디스를 계속 치고, 그 부하가 다음 측정에 섞인다.
#
# `kill 0` 은 안 쓴다. 프로세스 그룹을 통째로 내려 부른 쪽까지 같이 끝난다.
#
# 자식을 **제 프로세스 그룹**으로 떼어 그 그룹만 내린다. 그냥 죽이면 그 아래
# 손자(잠자는 프로세스 등)가 남는다.
fifo=$(mktemp -u)
mkfifo "$fifo"
if command -v setsid >/dev/null 2>&1; then
    setsid $probe "$loop" > "$fifo" 2>/dev/null &
else
    $probe "$loop" > "$fifo" 2>/dev/null &
fi
producer=$!
trap 'kill -- -"$producer" 2>/dev/null || kill "$producer" 2>/dev/null;
      $probe_stop "$PROBE_MARK" >/dev/null 2>&1; rm -f "$fifo"' \
    EXIT INT TERM

prev_cmds=""
prev_ns=""
now_ns=""
while IFS= read -r line; do
    case "$line" in
        total_commands_processed:*) cmds=${line#total_commands_processed:} ;;
        T[0-9]*) stamp=${line#T}
            sec=${stamp%%:*}; rest=${stamp#*:}; usec=${rest%%:*}
            # **두 칸을 따로 본다.** 이어 붙여 보면 마이크로초 칸이 비어도
            # 숫자로 통과하고, 빈 값이 0 으로 읽혀 다시 초 단위 시계가 된다 —
            # 고친 그 버그가 조용히 돌아오는 자리다.
            case "$sec" in ''|*[!0-9]*) continue ;; esac
            case "$usec" in ''|*[!0-9]*) continue ;; esac
            # 앞자리 0 은 8진수로 안 읽는다.
            now_ns=$(( 10#$sec * 1000000000 + 10#$usec * 1000 )); continue ;;
        *) continue ;;
    esac
    # 뒤에 붙는 캐리지 리턴을 뗀다. 안 떼면 산술이 터진다.
    cmds=${cmds%%[!0-9]*}
    [ -n "$cmds" ] || continue
    # **시각 없이 온 카운터는 버린다.** 안 버리면 낡은 시각과 새 카운터가
    # 짝지어져 창이 두 배가 되고 값이 절반으로 나온다 — 부하 최고점에서 한쪽만
    # 실패하기 쉬우므로, 하필 피크가 가장 필요한 순간에 묽어진다.
    if [ -z "$now_ns" ]; then
        continue
    fi
    if [ -n "$prev_cmds" ]; then
        # **실제로 흐른 시간으로 나눈다.** 주기를 가정하면 한 표본을 놓쳤을 때
        # 그 구간의 명령이 짧은 창에 실려 봉우리가 부풀어 보인다.
        elapsed_ns=$((now_ns - prev_ns))
        delta=$((cmds - prev_cmds))
        # 레디스가 재시작하면 누적이 되돌아간다. 음수 차분은 안 적는다.
        if [ "$elapsed_ns" -gt 0 ] && [ "$delta" -ge 0 ]; then
            # **창도 같이 적는다.** 비율만 적으면 계기가 고장 났을 때 그것이
            # 부하인지 창인지 산출물만 보고 못 가른다 — 원래 버그가 "창이 1ns"
            # 라는 1 차 증상이었는데, 비율만 보면 2 차 증상으로만 드러난다.
            printf '%s %s\n' $(( delta * 1000000000 / elapsed_ns )) \
                $(( elapsed_ns / 1000000 )) >> "$out"
        fi
    fi
    prev_cmds=$cmds
    prev_ns=$now_ns
    now_ns=""
done < "$fifo"

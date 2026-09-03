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
loop="while :; do date +%s%N; redis-cli INFO stats | grep total_commands_processed; sleep $interval; done"

: > "$out"
prev_cmds=""
prev_ns=""
$probe "$loop" 2>/dev/null | while IFS= read -r line; do
    case "$line" in
        total_commands_processed:*) cmds=${line#total_commands_processed:} ;;
        [0-9]*) now_ns=$line; continue ;;
        *) continue ;;
    esac
    # 뒤에 붙는 캐리지 리턴을 뗀다. 안 떼면 산술이 터진다.
    cmds=${cmds%%[!0-9]*}
    [ -n "$cmds" ] || continue
    if [ -n "$prev_cmds" ]; then
        # **실제로 흐른 시간으로 나눈다.** 주기를 가정하면 한 표본을 놓쳤을 때
        # 그 구간의 명령이 짧은 창에 실려 봉우리가 부풀어 보인다.
        elapsed_ns=$((now_ns - prev_ns))
        delta=$((cmds - prev_cmds))
        # 레디스가 재시작하면 누적이 되돌아간다. 음수 차분은 안 적는다.
        if [ "$elapsed_ns" -gt 0 ] && [ "$delta" -ge 0 ]; then
            printf '%s\n' $(( delta * 1000000000 / elapsed_ns )) >> "$out"
        fi
    fi
    prev_cmds=$cmds
    prev_ns=$now_ns
done

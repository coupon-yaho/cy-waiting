#!/usr/bin/env bash
# 부하가 도는 동안 레디스가 실제로 얼마나 바쁜지를 잰다 (10.0.2~10.0.4).
#
# **응답 지표로는 못 잰다.** 게이트웨이가 빨리 답해도 레디스가 한계에 닿아 있으면
# 다음 판에서 무너진다 — Phase 10 착수를 정하는 것은 그쪽 수치다.
#
# 사용: redis-probe.sh <출력파일> &   … 부하 … ; kill %1
set -uo pipefail

out=${1:?출력 파일}
interval=${PROBE_INTERVAL_SEC:-0.2}
cli=${REDIS_CLI:-docker compose -f test/load/compose.yml exec -T redis redis-cli}

# **표본 주기를 짧게 둔다.** 스파이크가 200ms 창이라 1 초 주기로는 봉우리를
# 통째로 놓친다 — 그 봉우리가 정확히 재려는 값이다.
: > "$out"
while :; do
    ops=$($cli INFO stats 2>/dev/null \
        | sed -n 's/^instantaneous_ops_per_sec:\([0-9]*\).*/\1/p')
    # 못 읽은 판은 0 으로 안 적는다. 0 은 "한가했다" 라는 뜻이라 봉우리를 깎는다.
    [ -n "$ops" ] && printf '%s\n' "$ops" >> "$out"
    sleep "$interval"
done

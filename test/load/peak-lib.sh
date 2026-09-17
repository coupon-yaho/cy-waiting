#!/usr/bin/env bash
# 최대치 회차의 러너가 쓰는 부분.
#
# **왜 뺐나.** 요약을 읽는 자리와 회차 길이를 초로 푸는 자리와 k6 종료 코드를
# 판정으로 옮기는 자리가 러너 안에만 있어, 그 셋이 틀려도 회차를 다 돌린 뒤에야
# 표가 이상해진다. 함수로 빼면 자기검증이 가짜 요약으로 직접 잰다 (TS-9).

[ -n "${PEAK_LIB_LOADED:-}" ] && return 0
PEAK_LIB_LOADED=1

# **요약의 두 모양을 다 본다.** k6 판에 따라 값이 `metrics.X.rate` 로도,
# `metrics.X.values.rate` 로도 온다. 하나만 보면 판이 바뀌는 순간 전 회차가
# 조용히 판정 불가가 되고, 그것이 하네스 천장으로 읽힌다.
peak_summary_value() {
    python3 - "$1" "$2" <<'PY'
import json, sys
try:
    d = json.load(open(sys.argv[1]))
except Exception:
    sys.exit(0)
m = d.get('metrics', {})
name, key = ('http_reqs', 'rate') if sys.argv[2] == 'rate' else ('http_req_duration', 'p(99)')
node = m.get(name, {})
v = node.get('values', {}).get(key, node.get(key))
if isinstance(v, (int, float)):
    print(f'{v:.4f}')
PY
}

# **회차 길이를 초로 푼다.** `${D%s}` 로 끝 글자만 떼면 `1m` 이 1 이 되어, 기대
# 건수가 60 분의 1 로 내려가고 "부하가 안 닿았다" 가드가 사실상 사라진다.
# 못 읽는 형식은 0 을 내고 부르는 쪽이 회차를 돌리기 전에 끊는다.
peak_duration_sec() {
    printf '%s\n' "$1" | awk '
        {
            s = $0
            if (s !~ /^([0-9]+h)?([0-9]+m)?([0-9]+(\.[0-9]+)?s)?$/ || s == "") { print 0; exit }
            total = 0
            if (match(s, /[0-9]+h/))            total += substr(s, RSTART, RLENGTH - 1) * 3600
            if (match(s, /[0-9]+m/))            total += substr(s, RSTART, RLENGTH - 1) * 60
            if (match(s, /[0-9]+(\.[0-9]+)?s/)) total += substr(s, RSTART, RLENGTH - 1)
            printf "%d", total
        }'
}

# **VU 풀은 VU 당 100 회가 되게 잡는다.** 표가 VU 마다 하나라 VU 당 회차가 적으면 폴링 갈래가 표 없이
# 진입으로 돌고, 그 몫은 대략 VU 당 회차에 반비례한다. 하한 50 은 낮은 유입의 동시 요청 몫이다.
# 못 읽는 입력은 0 을 내고 부르는 쪽이 회차를 돌리기 전에 끊는다.
peak_vus() {
    case "${1:-}" in ''|*[!0-9]*) echo 0; return ;; esac
    case "${2:-}" in ''|*[!0-9]*) echo 0; return ;; esac
    if [ "$1" -eq 0 ] || [ "$2" -eq 0 ]; then echo 0; return; fi
    local vus=$(( $1 * $2 / 100 ))
    echo $(( vus > 50 ? vus : 50 ))
}

# **예열이 됐는지는 그 회차의 p99 로 본다.** 한 번 돌렸다는 것만 보면 무너진 예열 뒤의 첫 칸이 예열 노릇을
# 한다. 0 수렴 · 1 덜 됐다 · 2 못 읽는다.
peak_warm_converged() {
    local p99
    printf '%s' "${2:-}" | grep -Eq '^[0-9]+(\.[0-9]+)?$' || return 2
    p99=$(peak_summary_value "$1" p99)
    [ -n "$p99" ] || return 2
    awk -v p="$p99" -v m="$2" 'BEGIN{ exit (p <= m) ? 0 : 1 }'
}

# **깨진 임계를 가려 읽는다.** k6 는 어느 임계가 깨져도 99 로 끝난다. 통째로
# 정상으로 읽으면 게이트웨이가 연결을 끊은 회차가 `ok` 로 표에 남고, 그 수가
# "현재 최대치" 로 계획서에 간다.
#
#   dropped_iterations  하네스가 유입을 못 만들었다 — 그것이 곧 하네스 천장이다
#   http_req_failed · peak_off_judgement · checks  게이트웨이가 판정 밖 응답을 냈다
#   peak_poll_bootstrap  섞은 비율이 어긋났다 — 이 회차가 재려던 것이 아니다
peak_verdict_from_k6() {
    local rc=$1 summary=$2
    [ "$rc" = 0 ] && { printf 'ok'; return 0; }
    [ "$rc" = 99 ] || { printf 'unmeasurable'; return 0; }
    python3 - "$summary" <<'PY'
import json, sys
try:
    m = json.load(open(sys.argv[1])).get('metrics', {})
except Exception:
    print('unmeasurable'); raise SystemExit
def broken(entry):
    # k6 판에 따라 모양이 둘이다. 평면형은 참이 "깨졌다" 는 뜻이고(드롭 0 인
    # 예열 회차가 거짓으로 온다), 중첩형은 `{"ok": false}` 로 온다.
    # **비어 있지 않다는 것만 보면 안 된다** — 통과한 중첩형도 참이 되어,
    # 드롭 하나로 99 가 난 회차의 멀쩡한 임계까지 깨진 것으로 읽힌다.
    if isinstance(entry, bool):
        return entry
    if isinstance(entry, dict):
        return not entry.get('ok', True)
    return bool(entry)

crossed = {n for n, v in m.items()
           if isinstance(v, dict)
           and any(broken(e) for e in v.get('thresholds', {}).values())} \
    if isinstance(m, dict) else set()
if not crossed:
    print('unmeasurable')          # 99 인데 깨진 것이 안 보인다 — 요약을 못 믿는다
elif crossed & {'http_req_failed', 'peak_off_judgement', 'checks'}:
    print('under')
elif crossed - {'dropped_iterations'}:
    print('unmeasurable')          # 섞은 비율이 어긋난 회차는 재려던 것이 아니다
else:
    print('ok')
PY
}

# docker stats 한 벌을 표본 줄로 옮긴다 (10.7.4). **우리 프로젝트 컨테이너만** 남긴다 — 같은 호스트의 남의
# 컨테이너 CPU 가 천장 원인에 끼면 안 된다. `%` 를 뗀다 — 판정기는 숫자만 받는다.
#
#   사용: docker stats --no-stream --format '{{.Name}}\t{{.CPUPerc}}' | peak_cpu_lines <프로젝트>
peak_cpu_lines() {
    awk -F '\t' -v p="$1-" 'index($1, p) == 1 { v = $2; sub(/%$/, "", v); printf "cpu\t%s\t%s\n", $1, v }'
}

# 호스트 CPU 유휴 백분율. 한 초 사이 `/proc/stat` 두 번을 차분한다 — 부팅 이후 누적값을 그대로 쓰면
# 회차와 무관한 평균이 나온다. 유휴에 iowait 를 넣는다: 코어가 놀고 있는 것이다.
peak_host_idle_pct() {
    local a b
    a=$(awk '/^cpu /{ print $2+$3+$4+$5+$6+$7+$8+$9, $5+$6 }' /proc/stat)
    sleep 1
    b=$(awk '/^cpu /{ print $2+$3+$4+$5+$6+$7+$8+$9, $5+$6 }' /proc/stat)
    awk -v a="$a" -v b="$b" 'BEGIN{ split(a, x, " "); split(b, y, " "); t = y[1] - x[1];
        printf "%.1f", (t > 0) ? 100 * (y[2] - x[2]) / t : 0 }'
}

# **회차 동안 CPU 를 쌓는다** (10.7.4). 천장이 났을 때 그것이 하네스의 것인지 게이트웨이의 것인지 가를
# 재료다. 백그라운드로 띄우고 회차가 끝나면 죽인다 — 한 바퀴가 docker stats 표집과 유휴 측정으로 2초쯤 든다.
#
#   사용: peak_sample_cpu <표본 파일> <프로젝트> & sampler=$!
peak_sample_cpu() {
    local out=$1 project=$2
    : > "$out"
    while :; do
        docker stats --no-stream --format '{{.Name}}\t{{.CPUPerc}}' 2>/dev/null \
            | peak_cpu_lines "$project" >> "$out"
        printf 'idle\thost\t%s\n' "$(peak_host_idle_pct)" >> "$out"
    done
}

# 대마다 긁은 판정 계수를 품질별로 합친다 (10.7.3). **합의 비율이어야 한다** — 대마다 비율을 내 평균하면 적게
# 받은 대가 많이 받은 대만큼 무겁게 섞인다. 이어 붙이면 판정기가 같은 시계열이 둘이라고 멈춘다.
#
#   사용: peak_merge_judgement <대 1 지표> <대 2 지표> ...
peak_merge_judgement() {
    awk '
        /^waiting_judgement_total\{/ {
            if (match($0, /quality="[^"]*"/) == 0) next
            q = substr($0, RSTART + 9, RLENGTH - 10)
            if (!(q in sum)) order[++n] = q
            sum[q] += $NF
        }
        END {
            for (i = 1; i <= n; i++)
                printf "waiting_judgement_total{application=\"waiting\",quality=\"%s\"} %.1f\n", order[i], sum[order[i]]
        }
    ' "$@"
}


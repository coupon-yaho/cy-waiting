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
# 생성기 여럿의 요약을 하나로 합친다. 첫 인자가 `rate` 면 더하고 `p99` 면 큰 쪽을 든다.
#
# **분위수는 못 더한다.** 나눠서 잰 p99 를 합치는 식은 없다 — 버킷을 합쳐야 나온다. 그래서
# 가장 나쁜 대의 값을 든다. 그것은 함대 p99 의 **상한**이다 — 대마다 제 p99 를 넘는 표본이
# 1% 아래이므로 그 최댓값을 넘는 표본도 전체의 1% 아래다. 작은 대의 느린 꼬리가 전체에서는
# 1% 에 못 미칠 수 있어 **실제보다 나쁘게 나온다.** 보수적인 대용값으로 읽고, 참값이 필요하면
# 버킷이나 원시 표본을 합쳐야 한다.
#
# **하나라도 못 읽으면 아무것도 안 낸다.** 0 으로 세면 죽은 생성기가 "유입이 적었다" 로 읽혀,
# 하네스 고장이 제품의 천장으로 적힌다.
peak_merged_value() {
    local kind=$1
    shift
    python3 - "$kind" "$@" <<'MERGE'
import json, sys
kind = sys.argv[1]
vals = []
for path in sys.argv[2:]:
    try:
        d = json.load(open(path))
    except Exception:
        sys.exit(0)
    m = d.get('metrics', {})
    name, key = ('http_reqs', 'rate') if kind == 'rate' else ('http_req_duration', 'p(99)')
    node = m.get(name, {})
    v = node.get('values', {}).get(key, node.get(key))
    if not isinstance(v, (int, float)):
        sys.exit(0)
    vals.append(float(v))
if not vals:
    sys.exit(0)
print(f'{sum(vals) if kind == "rate" else max(vals):.4f}')
MERGE
}
# 원격 셸에 끼울 값을 한 낱말로 묶는다.
#
# **작은따옴표 안에 값을 그냥 끼우면 안 된다.** 값에 작은따옴표가 하나만 섞여도 거기서 인용이
# 끝나고 뒤가 명령으로 읽힌다. 주소에 실수로 따옴표가 들어간 경우까지 조용히 안 깨지게 한다.
peak_sh_quote() {
    printf "'%s'" "$(printf '%s' "${1-}" | sed "s/'/'\\\\''/g")"
}
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
# 못 읽는 입력은 0 을 내고 부르는 쪽이 회차를 돌리기 전에 끊는다. 아홉 자리까지만 받는다 — 곱이 bash 정수를
# 넘으면 음수가 되어 하한으로 조용히 줄어든다.
peak_vus() {
    case "${1:-}" in ''|*[!0-9]*|??????????*) echo 0; return ;; esac
    case "${2:-}" in ''|*[!0-9]*|??????????*) echo 0; return ;; esac
    # 선행 0 을 10진수로 읽는다. bash 산술은 008 을 오류로, 010 을 8 로 읽는다.
    local rate=$((10#$1)) sec=$((10#$2))
    if [ "$rate" -eq 0 ] || [ "$sec" -eq 0 ]; then echo 0; return; fi
    local vus=$(( rate * sec / 100 ))
    echo $(( vus > 50 ? vus : 50 ))
}

# **예열이 됐는지는 그 회차의 p99 로 본다.** 한 번 돌렸다는 것만 보면 무너진 예열 뒤의 첫 칸이 예열 노릇을
# 한다. 0 수렴 · 1 덜 됐다 · 2 못 읽는다.
peak_warm_converged() {
    local p99
    printf '%s' "${2:-}" | grep -Eq '^[0-9]+(\.[0-9]+)?$' || return 2
    awk -v m="$2" 'BEGIN{ exit (m > 0) ? 0 : 1 }' || return 2
    p99=$(peak_summary_value "$1" p99)
    [ -n "$p99" ] || return 2
    awk -v p="$p99" -v m="$2" 'BEGIN{ exit (p <= m) ? 0 : 1 }'
}

# **걸린 요청을 가린다.** 흘린 회차 없이 보낸 건수는 찼는데 실측 유입만 모자라면, 끝나지 않은 요청이 회차를
# 늘린 것이다. k6 는 유입률을 전체 시간으로 나누므로 그대로 두면 생성기 한계로 읽힌다. 0 걸렸다 · 1 아니다 · 2 못 읽는다.
#
#   사용: peak_hung <요약> <요청 유입> <회차 초> <허용 오차>
peak_hung() {
    python3 - "$@" <<'PY'
import json, sys
try:
    m = json.load(open(sys.argv[1])).get('metrics', {})
    rate, sec, tol = float(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4])
except Exception:
    sys.exit(2)
def val(name, key):
    node = m.get(name, {})
    v = node.get('values', {}).get(key, node.get(key))
    return v if isinstance(v, (int, float)) else None
count, actual = val('iterations', 'count'), val('http_reqs', 'rate')
if count is None or actual is None:
    sys.exit(2)
dropped = val('dropped_iterations', 'count') or 0
sys.exit(0 if dropped == 0 and count >= rate * sec * tol and actual < rate * tol else 1)
PY
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

# docker stats 한 벌을 표본 줄로 옮긴다. **우리 프로젝트 컨테이너만** 남긴다 — 같은 호스트의 남의
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

# **회차 동안 CPU 를 쌓는다.** 천장이 났을 때 그것이 하네스의 것인지 게이트웨이의 것인지 가를 재료다.
# `<표본 파일>.stop` 이 생기거나, 러너가 사라지거나, 기한(epoch 초)이 지나면 그 바퀴를 마치고 멈춘다 — 한 바퀴가
# 2초쯤 든다. **기한은 회차 끝이다.** k6 가 걸린 요청으로 더 도는 동안의 한가한 표본이 붙었던 자리를 가린다.
# 정지 파일은 부르는 쪽이 띄우기 전에 지운다.
#
#   사용: peak_sample_cpu <표본 파일> <프로젝트> <러너 PID> [기한] & sampler=$!
peak_sample_cpu() {
    local out=$1 project=$2 owner=$3 deadline=${4:-}
    : > "$out"
    while [ ! -e "$out.stop" ] && kill -0 "$owner" 2>/dev/null \
            && { [ -z "$deadline" ] || [ "$(date +%s)" -lt "$deadline" ]; }; do
        docker stats --no-stream --format '{{.Name}}\t{{.CPUPerc}}' 2>/dev/null \
            | peak_cpu_lines "$project" >> "$out"
        printf 'idle\thost\t%s\n' "$(peak_host_idle_pct)" >> "$out"
    done
}

# 대마다 낸 판정 비율을 모은다. **합산하지 않는다** — 한 대가 다시 떴거나 못 긁은 칸이 다른 대의 계수에 묻힌다.
# 가장 나쁜 것을 낸다. 판정 불가가 미달보다 앞이다 — 한 대를 못 잰 칸을 제품 탓으로 못 읽는다.
#
#   사용: peak_worst_verdict <판정>...
peak_worst_verdict() {
    local v worst=ok
    [ $# -gt 0 ] || { printf 'unmeasurable'; return; }
    for v in "$@"; do
        case "$v" in
            ok) ;;
            under) [ "$worst" = ok ] && worst=under ;;
            *) worst=unmeasurable ;;
        esac
    done
    printf '%s' "$worst"
}


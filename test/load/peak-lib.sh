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
# 요약의 참은 "임계가 깨졌다" 는 뜻이다. 예열 회차(드롭 0)가 거짓으로 온다.
crossed = {n for n, v in m.items()
           if any(v.get('thresholds', {}).values())} if isinstance(m, dict) else set()
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

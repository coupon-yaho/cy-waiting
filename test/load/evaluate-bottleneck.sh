#!/usr/bin/env bash
# 천장 원인.
#
# **천장이 났다는 것과 어디서 났는지는 다르다.** 현재 최대치 러너는 회차가 안 서면 멈추는데, 하네스가
# 못 만든 것과 게이트웨이가 못 버틴 것이 같은 멈춤으로 보인다. 그 둘을 안 가르면 증설 효율이
# 하네스의 한계를 나눈 수가 된다.
#
# 표본은 회차 동안 쌓인 `종류<TAB>이름<TAB>값` 이다. CPU 는 한 코어가 100, 호스트는 유휴 백분율.
#
# **원인은 이 순서로 가른다.** 호스트가 말랐으면 게이트웨이가 한도에 붙은 것도 k6 와 코어를 다툰 결과일 수
# 있어 먼저 본다. LB 를 지나는 회차(`LB_CPUS`)면 그다음 LB — 앞단이 막히면 뒤로 부하가 안 간다. 그다음 공유
# 자원인 레디스(한 스레드라 한 코어가 한도, 그다음 초당 명령 수) — 붙었으면 게이트웨이를 늘려도 안 풀린다.
# 그다음 네트워크, 그다음 게이트웨이 — 전 대가 붙어야 한다. 한 대만 붙은 것은 고르게 안 나뉜 하네스 쪽이다.
set -uo pipefail

# 2 는 계기를 고치라는 뜻이다. 원인을 가른 회차는 원인과 상관없이 0 이다 — 원인이 제품이라는 것이 이
# 판정기의 실패가 아니다.
UNMEASURABLE=2

if [ $# -lt 1 ]; then
    echo "::error title=천장 원인::표본 파일이 필요하다 — 판정 불가"
    exit "$UNMEASURABLE"
fi
samples=$1
if [ ! -s "$samples" ]; then
    echo "::error title=천장 원인::표본 파일이 없거나 비었다: $samples — 판정 불가"
    exit "$UNMEASURABLE"
fi

cpus=${GATEWAY_CPUS:-2}
if ! printf '%s' "$cpus" | grep -Eq '^[0-9]+(\.[0-9]+)?$' \
        || awk -v c="$cpus" 'BEGIN{ exit (c > 0) ? 1 : 0 }'; then
    echo "::error title=천장 원인::게이트웨이 코어 한도가 양수가 아니다: '$cpus' — 판정 불가"
    exit "$UNMEASURABLE"
fi

# **붙었다고 보는 선.** 한도를 정확히 채우는 일은 드물다 — 스케줄러 몫과 표집 간격이 있어 90% 에서 끊는다.
saturation=${SATURATION_PCT:-90}
# **호스트 유휴가 이보다 적으면 말랐다.** k6 가 같은 호스트에서 돌아 이 선 아래서는 유입 자체가 흔들린다.
host_floor=${HOST_IDLE_FLOOR_PCT:-10}
# 회차 중간의 한 순간을 천장 원인으로 적지 않는다.
min_samples=${MIN_SAMPLES:-3}
# LB 를 지나는 회차의 LB 코어 한도. 비우면 LB 를 안 본다.
lb_cpus=${LB_CPUS:-}
# **LB 는 CPU 말고 연결 한도에서도 막힌다.** 러너가 회차 중 nginx 가 낸 연결 한도·accept 실패 줄 수를 넘긴다.
lb_errors=${LB_CONN_ERRORS:-0}
case "$lb_errors" in
    ''|*[!0-9]*) echo "::error title=천장 원인::LB 연결 오류 수가 0 이상의 정수가 아니다: '$lb_errors' — 판정 불가"
        exit "$UNMEASURABLE" ;;
esac
if [ -n "$lb_cpus" ] && { ! printf '%s' "$lb_cpus" | grep -Eq '^[0-9]+(\.[0-9]+)?$' \
        || awk -v c="$lb_cpus" 'BEGIN{ exit (c > 0) ? 1 : 0 }'; }; then
    echo "::error title=천장 원인::LB 코어 한도가 양수가 아니다: '$lb_cpus' — 판정 불가"
    exit "$UNMEASURABLE"
fi
# **CPU 로는 안 보이는 천장 둘** (CY-990). 레디스는 명령이 짧으면 한 스레드가 코어를 안 채운 채 초당 명령 수에서
# 먼저 붙고, 네트워크는 CPU 가 한가한 채로 막힌다. 천장은 호스트마다 달라 적었을 때만 판정에 쓴다 — 비우면 표본만
# 보여 준다. 레디스 명령 수는 `redis-benchmark` 로 잰 초당 수, 네트워크는 대마다 받고 보낸 합의 Mbit/s 다.
ops_ceiling=${REDIS_OPS_CEILING:-}
net_ceiling=${NET_CEILING_MBPS:-}
for axis in "명령 수 천장:$ops_ceiling" "망 천장:$net_ceiling"; do
    value=${axis#*:}
    [ -z "$value" ] && continue
    if ! printf '%s' "$value" | grep -Eq '^[0-9]+(\.[0-9]+)?$' \
            || awk -v c="$value" 'BEGIN{ exit (c > 0) ? 1 : 0 }'; then
        echo "::error title=천장 원인::${axis%%:*}이 양수가 아니다: '$value' — 판정 불가"
        exit "$UNMEASURABLE"
    fi
done
# **뜬 대수를 표본에서 세지 않는다.** 한 대가 표집에 안 잡히면 남은 대만 붙어도 "모두" 가 된다.
expected=${GATEWAYS:-1}

# **기준도 확인한다.** 오타가 awk 에서 0 이 되면 모든 대가 붙은 것으로 나와 게이트웨이 천장이 된다.
for pct in "붙음 선:$saturation" "마름 선:$host_floor"; do
    if ! printf '%s' "${pct#*:}" | grep -Eq '^[0-9]+(\.[0-9]+)?$' \
            || awk -v v="${pct#*:}" 'BEGIN{ exit (v > 0 && v <= 100) ? 1 : 0 }'; then
        echo "::error title=천장 원인::${pct%%:*}이 0 초과 100 이하의 백분율이 아니다: '${pct#*:}' — 판정 불가"
        exit "$UNMEASURABLE"
    fi
done
for count in "최소 표본:$min_samples" "기대 대수:$expected"; do
    case "${count#*:}" in
        ''|*[!0-9]*|0) echo "::error title=천장 원인::${count%%:*}가 양의 정수가 아니다: '${count#*:}' — 판정 불가"
            exit "$UNMEASURABLE" ;;
    esac
done

# **평균이 아니라 가운데 값으로 가른다.** 표집은 k6 가 VU 를 띄우기 전과 끝난 뒤에 걸쳐, 앞뒤 한가한 표본 몇
# 개가 평균을 선 너머로 옮긴다.
awk -F '\t' -v cpus="$cpus" -v sat="$saturation" -v floor="$host_floor" -v need="$min_samples" \
        -v expected="$expected" -v lb_cpus="$lb_cpus" -v lb_errors="$lb_errors" \
        -v ops_ceiling="$ops_ceiling" -v net_ceiling="$net_ceiling" '
    function num(v) { return v ~ /^-?[0-9]+(\.[0-9]+)?$/ }
    function median(key, n,    i, j, t, a) {
        for (i = 1; i <= n; i++) a[i] = vals[key, i]
        for (i = 2; i <= n; i++) { t = a[i]; for (j = i - 1; j >= 1 && a[j] > t; j--) a[j + 1] = a[j]; a[j + 1] = t }
        return (n % 2) ? a[(n + 1) / 2] : (a[n / 2] + a[n / 2 + 1]) / 2
    }
    function add(key, v) { vals[key, ++cnt[key]] = v
        if (!((key) in hi) || v > hi[key]) hi[key] = v
        if (!((key) in lo) || v < lo[key]) lo[key] = v }
    $0 ~ /^#/ || NF < 3 { next }
    !num($3) { bad = 1; next }
    # 범위 밖 값은 계기 고장이다. 유휴 -1 이 마름으로 읽히면 고장이 원인으로 적힌다.
    $1 == "cpu" && $3 < 0 { bad = 1; next }
    $1 == "idle" && ($3 < 0 || $3 > 100) { bad = 1; next }
    ($1 == "ops" || $1 == "net") && $3 < 0 { bad = 1; next }
    # 서비스 자리(`<프로젝트>-<서비스>-<번호>`)로만 가른다. 프로젝트 이름에 같은 말이 들어가도 안 섞인다.
    $1 == "cpu" && $2 ~ /-gateway-[0-9]+$/ { if (!(("gw " $2) in cnt)) gws[++ng] = $2; add("gw " $2, $3); next }
    $1 == "cpu" && $2 ~ /-redis-[0-9]+$/ { add("redis", $3); next }
    $1 == "cpu" && $2 ~ /-lb-[0-9]+$/ { add("lb", $3); next }
    $1 == "idle"                  { add("idle", $3); next }
    $1 == "ops"                   { add("ops", $3); next }
    $1 == "net"                   { if (!(("net " $2) in cnt)) nets[++nn] = $2; add("net " $2, $3); next }
    END {
        if (bad) { print "::error title=천장 원인::숫자가 아니거나 범위 밖인 표본이 있다 — 판정 불가"; exit 2 }
        if (ng == 0) { print "::error title=천장 원인::게이트웨이 표본이 없다 — 판정 불가"; exit 2 }
        if (ng != expected) { printf "::error title=천장 원인::게이트웨이 표본이 %d 대다 (기대 %d 대) — 판정 불가\n", ng, expected; exit 2 }
        saturated = 0; limit = cpus * 100 * sat / 100
        for (i = 1; i <= ng; i++) {
            g = gws[i]; n = cnt["gw " g]
            if (n < need) { printf "::error title=천장 원인::%s 표본이 %d 개다 — 판정 불가\n", g, n; exit 2 }
            m = median("gw " g, n)
            printf "  %s CPU 가운데 %.1f%% (최대 %.1f%%, 한도 %s 코어, 붙음 선 %.1f%%)\n", g, m, hi["gw " g], cpus, limit
            if (m >= limit) saturated++
        }
        if (cnt["idle"] < need) { printf "::error title=천장 원인::호스트 표본이 %d 개다 — 판정 불가\n", cnt["idle"]; exit 2 }
        idle = median("idle", cnt["idle"])
        printf "  호스트 유휴 가운데 %.1f%% (최저 %.1f%%, 마름 선 %.1f%%)\n", idle, lo["idle"], floor
        # 레디스를 먼저 보는데 그 표집이 빠지면 순서가 조용히 사라진다.
        if (cnt["redis"] < need) { printf "::error title=천장 원인::레디스 표본이 %d 개다 — 판정 불가\n", cnt["redis"]; exit 2 }
        redis = median("redis", cnt["redis"])
        printf "  레디스 CPU 가운데 %.1f%% (최대 %.1f%%)\n", redis, hi["redis"]
        # 명령 수와 망. 천장을 적었으면 표본이 모자란 것은 계기 고장이다 — 조용히 건너뛰면 그 축이 없는 채로 판정한다.
        if (ops_ceiling != "" && cnt["ops"] < need) { printf "::error title=천장 원인::명령 수 표본이 %d 개다 — 판정 불가\n", cnt["ops"]; exit 2 }
        if (cnt["ops"] >= need) {
            ops = median("ops", cnt["ops"])
            printf "  레디스 명령 수 가운데 %.0f/초 (최대 %.0f/초, %s)\n", ops, hi["ops"],
                ops_ceiling == "" ? "천장 미설정 — 판정에 안 쓴다" : "천장 " ops_ceiling "/초"
        }
        if (net_ceiling != "" && nn == 0) { print "::error title=천장 원인::망 표본이 없다 — 판정 불가"; exit 2 }
        net_hot = ""; net_hot_v = -1
        for (i = 1; i <= nn; i++) {
            k = "net " nets[i]
            if (cnt[k] < need) { printf "::error title=천장 원인::%s 망 표본이 %d 개다 — 판정 불가\n", nets[i], cnt[k]; exit 2 }
            v = median(k, cnt[k])
            printf "  %s 망 가운데 %.1f Mbit/s (최대 %.1f, %s)\n", nets[i], v, hi[k],
                net_ceiling == "" ? "천장 미설정 — 판정에 안 쓴다" : "천장 " net_ceiling " Mbit/s"
            if (v > net_hot_v) { net_hot_v = v; net_hot = nets[i] }
        }
        if (lb_cpus != "") {
            if (cnt["lb"] < need) { printf "::error title=천장 원인::LB 표본이 %d 개다 — 판정 불가\n", cnt["lb"]; exit 2 }
            lb = median("lb", cnt["lb"]); lb_limit = lb_cpus * 100 * sat / 100
            printf "  LB CPU 가운데 %.1f%% (최대 %.1f%%, 한도 %s 코어, 붙음 선 %.1f%%)\n", lb, hi["lb"], lb_cpus, lb_limit
        }

        if (idle < floor)                  { print "원인: 호스트 — 하네스와 코어를 다퉈 이 천장은 게이트웨이의 것이 아니다"; exit 0 }
        if (lb_cpus != "" && lb_errors > 0) { printf "원인: LB — 연결 한도에서 막혔다 (오류 %d 건)\n", lb_errors; exit 0 }
        if (lb_cpus != "" && lb >= lb_limit) { print "원인: LB — 앞단이 코어 한도에 붙었다"; exit 0 }
        if (redis >= sat)                  { print "원인: 레디스 — 한 스레드가 한 코어에 붙었다"; exit 0 }
        if (ops_ceiling != "" && ops >= ops_ceiling * sat / 100) {
            printf "원인: 레디스 — 명령 수가 천장에 붙었다 (가운데 %.0f/초, 천장 %s/초)\n", ops, ops_ceiling; exit 0 }
        if (net_ceiling != "" && net_hot_v >= net_ceiling * sat / 100) {
            printf "원인: 네트워크 — %s 가 망 천장에 붙었다 (가운데 %.1f Mbit/s, 천장 %s)\n", net_hot, net_hot_v, net_ceiling; exit 0 }
        if (saturated == ng)               { printf "원인: 게이트웨이 — %d 대 모두 코어 한도에 붙었다\n", ng; exit 0 }
        printf "원인: 가르지 못함 — 붙은 자리가 없다 (게이트웨이 %d/%d 대)\n", saturated, ng
        exit 0
    }
' "$samples"

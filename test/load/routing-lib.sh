#!/usr/bin/env bash
# 라우팅 하네스 넷이 함께 쓰는 부분.
#
# **왜 뺐나.** 넷이 스택 세우기·램프 기다리기·줄 모드 확인·뒷단 계수 읽기를
# 똑같이 하고 있었고, 실질 코드의 절반이 중복이었다. 그 중복이 실제로 결함을
# 냈다 — 이미지 재빌드 가드를 네 곳에 각각 넣다가 한 곳이 조용히 빠졌고,
# 그 상태로 **낡은 바이너리를 재고** 그 값을 실측으로 적을 뻔했다.
#
# 고칠 곳이 하나면 그런 일이 안 난다.
#
#   사용: . test/load/routing-lib.sh   (source 한다. 실행하지 않는다)

# 여러 번 읽혀도 한 번만 선다.
[ -n "${ROUTING_LIB_LOADED:-}" ] && return 0
ROUTING_LIB_LOADED=1

COMPOSE="docker compose -f test/load/compose.yml -f test/load/compose.routing.yml"

# 뒷단 이름. **씨더가 쓰는 값과 같아야 한다** — 갈리면 기대값이 실제 보고와
# 달라져, 맞게 도착한 것이 미달로 적힌다.
NAMES=(backend backend-small backend-mid)

# **여유는 여기서 기본값을 안 준다.** 주면 이 파일을 읽는 순간 값이 서고,
# 부르는 쪽이 그 뒤에 `${BIG_CAP:-2000}` 으로 올리려 해도 이미 채워져 있어
# 무시된다. 그러면 판정이 허용하는 유입이 낮은 채로 돌아 요청이 뒷단에 안
# 닿고, **아무것도 안 잰 실행이 나온다.** 실제로 그렇게 한 번 돌았다.
#
# 부르는 쪽이 이 파일을 읽기 **전에** 셋을 정한다.
#
# **0 도 안 받는다.** 여유가 0 인 대는 고르개가 후보에서 빼므로, 그 값으로
# 세운 겹침은 재려던 것을 아예 안 잰다.
for _cap in BIG_CAP SMALL_CAP MID_CAP; do
    _value=$(eval "printf '%s' \"\${$_cap:-}\"")
    case "$_value" in
        ''|*[!0-9]*|0)
            echo "$_cap 은 1 이상의 정수여야 한다. routing-lib.sh 를 읽기 전에 정한다: '$_value'" >&2
            return 2 ;;
    esac
done
unset _cap _value

COUPON="${COUPON:-c1}"
GATEWAY="${GATEWAY:-http://localhost:18080}"
STRATEGY="${STRATEGY:-round-robin}"
STUB_LATENCY_MS="${STUB_LATENCY_MS:-5}"
# 큰 대의 지연은 따로 둘 수 있다 — 열화를 지연으로 넣을 때 쓴다. 겹침이 그렇게
# 읽으므로 여기서도 같은 기본값을 둔다.
BIG_LATENCY_MS="${BIG_LATENCY_MS:-$STUB_LATENCY_MS}"

# 실행 조건 한 줄. **이 문장이 저널과 게이트 표에 그대로 옮겨 붙는다** — 배너와
# 실제 겹침이 갈리면 조건을 잘못 적은 실측이 남는다. 그래서 한 곳에서 만든다.
banner() {
    local latency="${STUB_LATENCY_MS}ms"
    [ "$BIG_LATENCY_MS" = "$STUB_LATENCY_MS" ] \
        || latency="${STUB_LATENCY_MS}ms(큰 대 ${BIG_LATENCY_MS}ms)"
    printf '전략 %s · 여유 %s/%s/%s · 뒷단 지연 %s' \
        "$STRATEGY" "$BIG_CAP" "$SMALL_CAP" "$MID_CAP" "$latency"
}

# 설정값이 정수인지 본다. 숫자가 아니면 셸 산술이 0 으로 읽어, 못 잰 것이
# 그럴듯한 값으로 나온다.
#
# **둘로 나눠 둔다.** 하나로 두면 이름이 "양수" 라고 말하면서 0 을 통과시키게
# 되고, 그러면 부르는 쪽이 이름을 믿고 `-gt 0` 을 따로 안 적는다. 실제로
# `WORKERS=0` 이 그렇게 지나가 요청을 한 건도 안 보내는 실행이 나올 수 있었다.
require_non_negative_int() {
    local name value
    for name in "$@"; do
        value=$(eval "printf '%s' \"\$$name\"")
        case "$value" in
            ''|*[!0-9]*) echo "$name 은 0 이상의 정수여야 한다: '$value'" >&2; return 2 ;;
        esac
    done
}

# 0 이면 재는 것 자체가 성립하지 않는 값에 쓴다 — 보낼 건수, 일꾼 수, 창 길이.
require_positive_int() {
    local name value
    require_non_negative_int "$@" || return 2
    for name in "$@"; do
        value=$(eval "printf '%s' \"\$$name\"")
        [ "$value" -gt 0 ] || { echo "$name 은 1 이상이어야 한다: '$value'" >&2; return 2; }
    done
}

# 뒷단이 누적으로 센 처리 건수.
#
# **못 읽으면 거기서 멈춘다.** 오류를 삼키고 빈 값을 돌려주면 그 값이 산술로
# 흘러 들어가 엉뚱한 도착 수가 나오고, 그 수로 게이트를 적게 된다.
served() {
    local raw count
    raw=$($COMPOSE exec -T "$1" sh -c 'wget -qO- http://localhost:8090/stub/health')
    count=$(printf '%s' "$raw" | sed -n 's/.*"served":\([0-9][0-9]*\).*/\1/p')
    case "$count" in
        ''|*[!0-9]*) echo "[$1] 처리 건수를 못 읽었다: ${raw:-응답 없음}" >&2; return 1 ;;
    esac
    # 앞자리 0 은 셸 산술이 8진수로 읽는다. 십진임을 명시한다.
    printf '%d\n' "$((10#$count))"
}

# 겹침을 세운다. **이미지가 실행 JAR 보다 낡았으면 다시 짓는다** — compose 는
# JAR 이 바뀌어도 있는 이미지를 그대로 쓰므로, 고친 코드가 아니라 옛 바이너리를
# 재게 된다. 실제로 "고쳐도 값이 그대로다" 를 한 번 겪었다.
bring_up() {
    local log=$1 jar=${WAITING_JAR:-build/libs/waiting.jar}
    if [ ! -f "$jar" ]; then
        echo "실행 JAR 이 없다: $jar — ./gradlew build 를 먼저 돌린다" >&2
        return 2
    fi
    if ! $COMPOSE build gateway > "$log" 2>&1; then
        echo "게이트웨이 이미지를 못 지었다" >&2; tail -20 "$log" | sed 's/^/  /' >&2
        return 2
    fi
    # 앞 실행이 낮춰 둔 여유가 볼륨에 남아 있으면 예열이 영영 안 끝난다.
    # 레디스만 먼저 띄워 되돌린 뒤 나머지를 세운다.
    $COMPOSE up -d redis >> "$log" 2>&1
    local _
    for _ in $(seq 1 30); do
        $COMPOSE exec -T redis redis-cli SET sim:credits:stub-1 "$BIG_CAP" >/dev/null 2>&1 && break
        sleep 1
    done
    $COMPOSE exec -T redis redis-cli SET sim:credits:stub-2 "$SMALL_CAP" >/dev/null 2>&1
    $COMPOSE exec -T redis redis-cli SET sim:credits:stub-3 "$MID_CAP" >/dev/null 2>&1
    # 앞 실행이 바꿔 둔 식별자로 시작하면 이번 갈아 끼우기가 램프를 안 탄다.
    $COMPOSE exec -T redis redis-cli SET sim:id:stub-1 stub-1 >/dev/null 2>&1

    if ! ROUTING_STRATEGY="$STRATEGY" STUB_LATENCY_MS="$STUB_LATENCY_MS" \
         BIG_LATENCY_MS="$BIG_LATENCY_MS" \
         BIG_CAP="$BIG_CAP" SMALL_CAP="$SMALL_CAP" MID_CAP="$MID_CAP" \
         $COMPOSE up -d --wait >> "$log" 2>&1; then
        echo "겹침을 못 세웠다" >&2; tail -20 "$log" | sed 's/^/  /' >&2
        return 2
    fi
}

# 램프가 다 오를 때까지 기다린다.
#
# **겹침의 warmup 은 크레딧이 하한을 넘기만 하면 통과시킨다.** 램프는 60초
# 짜리라 그 시점의 크레딧은 목표의 몇 분의 일이고, 그 상태로 부하를 넣으면
# 유입이 그때의 몫을 넘어 줄이 켜진다 — 요청이 뒷단에 안 닿는다.
wait_for_ramp() {
    local target=$(( (BIG_CAP + SMALL_CAP + MID_CAP) * 9 / 10 )) credit _
    for _ in $(seq 1 120); do
        credit=$($COMPOSE exec -T redis redis-cli HGET gw:snapshot '#credit' 2>/dev/null)
        case "$credit" in ''|*[!0-9]*) sleep 1; continue ;; esac
        [ "$credit" -ge "$target" ] && return 0
        sleep 1
    done
    echo "크레딧이 ${credit:-0} 에서 안 오른다 (목표 $target) — 이 상태로는 못 잰다" >&2
    return 2
}

# 줄을 비우고 **줄 모드가 꺼진 것을 보고** 시작한다.
#
# 지우자마자 보내면 게이트웨이가 아직 옛 스냅샷을 들고 있어, 요청이 뒷단으로
# 안 가고 줄로 간다.
wait_for_idle_queue() {
    local state _
    $COMPOSE exec -T redis redis-cli DEL "queue:{$COUPON}" >/dev/null 2>&1
    for _ in $(seq 1 30); do
        state=$($COMPOSE exec -T redis redis-cli HGET gw:snapshot "$COUPON" 2>/dev/null)
        case "$state" in *QUEUEING*) sleep 1 ;; *) return 0 ;; esac
    done
    echo "줄 모드가 안 꺼진다 ($state) — 이 상태로는 못 잰다" >&2
    return 2
}

# 요청 하나를 보내고 응답 코드를 낸다.
#
# **주소를 흩는다.** 한 주소로 몰아 보내면 주소별 한도에 걸려 429 가 나고,
# 그 요청은 뒷단에 안 닿아 표본에서 통째로 빠진다.
issue() {
    local member=$1
    curl -s -o /dev/null --max-time 5 -w '%{http_code}\n' -X POST \
        "$GATEWAY/api/v1/coupons/$COUPON/issue" \
        -H "X-Member-Id: $member" \
        -H "X-Member-Grade: GOLD" \
        -H "X-Forwarded-For: 10.$(( member / 62500 % 200 + 20 )).$(( member / 250 % 250 + 1 )).$(( member % 250 + 1 ))"
}

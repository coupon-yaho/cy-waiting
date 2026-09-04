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

# **죽일 때 부하 루프까지 걷는다.** 시나리오는 부하를 서브셸로 띄우는데, 밖에서
# `kill` 로 죽이면 bash 는 EXIT trap 을 안 돌리고 그 루프가 살아남는다. 살아남은
# 루프는 게이트웨이에 계속 요청을 보내 줄을 채우고, 다음 회차는 첫 요청부터
# 202 를 받는다 — 실제로 몇 시간 살아남아 여섯 회차를 오염시켰다.
# INT·TERM 을 잡아 exit 로 바꾸면 EXIT trap 이 돌고, 거기서 자식을 걷는다.
# bash 는 앞에서 도는 명령이 끝난 뒤에야 trap 을 돌린다 — 시나리오의 표본
# 간격이 1초라 그 안에 걷힌다. 긴 sleep 을 앞에 두면 그만큼 늦는다.
trap 'exit 130' INT TERM
reap_children() {
    pkill -P $$ 2>/dev/null
    return 0
}

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

# 스텁이 누적으로 센 값 하나. 이름은 served·faulted·rejected 다.
#
# **못 읽으면 거기서 멈춘다.** 오류를 삼키고 빈 값을 돌려주면 그 값이 산술로
# 흘러 들어가 엉뚱한 도착 수가 나오고, 그 수로 게이트를 적게 된다.
counter() {
    local raw count
    raw=$($COMPOSE exec -T "$1" sh -c 'wget -qO- http://localhost:8090/stub/health')
    count=$(printf '%s' "$raw" | sed -n "s/.*\"$2\":\([0-9][0-9]*\).*/\1/p")
    case "$count" in
        ''|*[!0-9]*) echo "[$1] $2 를 못 읽었다: ${raw:-응답 없음}" >&2; return 1 ;;
    esac
    printf '%d\n' "$((10#$count))"
}

# **닿은 요청 수다.** 즉시 실패한 것은 served 에 안 들어가므로, 유입이 줄었는지
# 보려면 둘을 더해야 한다 — 안 더하면 고장 난 대가 트래픽을 다 받고 있는데도
# 0 으로 보인다.
arrived() {
    local raw ok bad
    # **한 번만 친다.** 두 번 읽으면 도커 명령이 배로 들고, 그 비용이 곧
    # 판정기의 해상도가 된다 — 예산보다 표본 간격이 길어지면 맞게 도는
    # 구현이 미달로 적힌다.
    raw=$($COMPOSE exec -T "$1" sh -c 'wget -qO- http://localhost:8090/stub/health')
    ok=$(printf '%s' "$raw" | sed -n 's/.*"served":\([0-9][0-9]*\).*/\1/p')
    bad=$(printf '%s' "$raw" | sed -n 's/.*"faulted":\([0-9][0-9]*\).*/\1/p')
    case "$ok$bad" in
        ''|*[!0-9]*) echo "[$1] 도착 수를 못 읽었다: ${raw:-응답 없음}" >&2; return 1 ;;
    esac
    printf '%d\n' "$(( 10#$ok + 10#$bad ))"
}

# 뒷단이 누적으로 센 처리 건수. 즉시 실패한 것은 안 든다.
served() {
    counter "$1" served
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
    # **뒷단 스텁도 같이 짓는다.** 게이트웨이만 지으면 스텁을 고쳐도 옛 이미지가
    # 그대로 돌고, 새로 넣은 자극이 조용히 없는 것이 된다 — 그 회차는 재려던 것
    # 대신 아무것도 안 잰 것이 되는데 초록으로 끝난다.
    if ! $COMPOSE build gateway backend backend-small backend-mid > "$log" 2>&1; then
        echo "이미지를 못 지었다" >&2; tail -20 "$log" | sed 's/^/  /' >&2
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

    # 예열 컨테이너는 크레딧이 이 값에 닿아야 건강하다. 여유 합의 90% 로 두되
    # 200 을 넘기지 않는다 — 큰 여유 회차는 지금까지와 같고, 작은 여유 회차는
    # 영영 못 닿는 200 대신 제 여유에 맞는 값을 기다린다.
    local warm=$(( (BIG_CAP + SMALL_CAP + MID_CAP) * 9 / 10 ))
    [ "$warm" -gt 200 ] && warm=200
    [ "$warm" -lt 1 ] && warm=1
    if ! ROUTING_STRATEGY="$STRATEGY" STUB_LATENCY_MS="$STUB_LATENCY_MS" \
         WARMUP_CREDIT="$warm" \
         BIG_LATENCY_MS="$BIG_LATENCY_MS" \
         BIG_CAP="$BIG_CAP" SMALL_CAP="$SMALL_CAP" MID_CAP="$MID_CAP" \
         BIG_INFLIGHT="${BIG_INFLIGHT:-$BIG_CAP}" \
         SMALL_INFLIGHT="${SMALL_INFLIGHT:-$SMALL_CAP}" \
         MID_INFLIGHT="${MID_INFLIGHT:-$MID_CAP}" \
         ROUTING_PER_INSTANCE_CAP="${ROUTING_PER_INSTANCE_CAP:-200}" \
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
#
# **줄 키만 지우면 안 된다.** 입장 커서와 최대 순번이 남으면 리더가 줄을
# 비었다고 안 보고 쿠폰을 다시 QUEUEING 으로 돌리며, 판정은 IDLE 이 아니면
# 무조건 줄에 세운다(추월 금지). 그러면 첫 요청부터 202 다 — 여유가 작아 줄
# 모드에 한 번이라도 들어간 회차는 전부 그렇게 죽었다. 셋을 같이 지운다.
wait_for_idle_queue() {
    local state _
    $COMPOSE exec -T redis redis-cli DEL "queue:{$COUPON}" "admitted:{$COUPON}" \
        "maxscore:{$COUPON}" >/dev/null 2>&1
    for _ in $(seq 1 30); do
        state=$($COMPOSE exec -T redis redis-cli HGET gw:snapshot "$COUPON" 2>/dev/null)
        # IDLE 을 봐야 한다. QUEUEING 이 아닌 것으로 두면 PASSING 같은 중간 상태에서
        # 나가고, 그 상태의 첫 요청이 다시 줄 모드를 켠다.
        case "$state" in *:IDLE:*) return 0 ;; *) sleep 1 ;; esac
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

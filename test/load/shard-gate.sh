#!/usr/bin/env bash
# Phase 10 착수 판정 회차 (10.0.5).
#
# **판정을 못 믿게 만든 것은 판정기가 아니라 회차였다.** 같은 유입으로 세 번
# 돌려 피크가 13,230 · 265 · 292 ops/s 로 갈렸는데, 원인은 앞 회차가 남긴
# 대기열이었다. 줄이 차 있으면 첫 요청부터 QUEUE_FULL 이라 등록 경로를 한 번도
# 안 밟고, 그때 찍히는 세 자릿수 ops 는 제어 평면 몫이다 — 그것을 "여유 있다"
# 로 읽으면 샤딩이 필요한 상황에서도 게이트가 안 열린다.
#
# 그래서 회차는 반드시 **빈 줄에서** 시작한다.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1

# **레디스를 전용 코어에 고정할 수 있다.** 기본은 안 한다 — 지금까지 잰 값들과
# 견주려면 조건이 같아야 한다. 착수를 다시 정할 때만 켠다.
#
#   PINNED=1 test/load/shard-gate.sh
# **`PINNED=0` 은 끈 것이다.** `${PINNED:+...}` 는 "비어 있지 않음" 만 보므로
# 0 을 줘도 켜졌다. 끄려고 준 값이 켜는 자리가 된다.
case "${PINNED:-}" in
    ''|0|false|no) pinned="" ;;
    *) pinned=" -f test/load/compose.pinned.yml" ;;
esac
# **왕복을 대기와 실행으로 가르는 계기를 켠다.** 기본은 안 한다 — 실측으로 등록
# 한 건에 기록이 1.02 회 돈다. 부풀지는 않지만 공짜도 아니라, 켠 회차의 수를
# 인용할 때는 켜져 있었다는 것을 같이 적는다.
#
#   LATENCY=1 test/load/shard-gate.sh
case "${LATENCY:-}" in
    ''|0|false|no) latency="" ;;
    *) latency=" -f test/load/compose.latency.yml" ;;
esac
COMPOSE="docker compose -f test/load/compose.yml$pinned$latency"
# **시나리오까지 같이 간다.** 여기만 바꾸면 한 쿠폰을 비우고 다른 쿠폰을 때려 빈 줄
# 보증이 통째로 다른 쿠폰 얘기가 된다. `open-spike.js` 가 `COUPON` 을 읽으므로 자식
# 환경에 실어서 두 쪽이 같은 값을 본다.
export COUPON=c2
# 예열 쿠폰. **재는 쿠폰과 같으면 안 된다** — 크레딧이 올라간 채 본 회차가 시작해
# 전원이 통과하고, 정리에서 재는 쿠폰의 키를 회차 직전에 지운다.
WARMUP_COUPON="${WARMUP_COUPON:-c1}"
if [ "$WARMUP_COUPON" = "$COUPON" ]; then
    echo "WARMUP_COUPON 은 재는 쿠폰과 달라야 한다: '$WARMUP_COUPON'"; exit 2
fi
# **숫자가 아니면 조용히 어긋난다.** `set -e` 가 없어서 `sleep abc` 는 실패만 하고
# 넘어간다 — 가라앉힘 없이 본 회차가 시작하는데 산출물에 흔적이 없다.
WARMUP_USERS="${WARMUP_USERS:-3000}"
WARMUP_SETTLE_SEC="${WARMUP_SETTLE_SEC:-8}"
for pair in "WARMUP_USERS:$WARMUP_USERS" "WARMUP_SETTLE_SEC:$WARMUP_SETTLE_SEC"; do
    case "${pair#*:}" in
        ''|*[!0-9]*) echo "${pair%%:*} 는 음이 아닌 정수여야 한다: '${pair#*:}'"; exit 2 ;;
    esac
done
OUT_OPS="${OUT_OPS:-redis-ops${pinned:+-pinned}${latency:+-latency}.txt}"
# **산출물 이름에 조건을 싣는다.** 고정한 회차와 안 한 회차가 같은 파일에
# 덮이면 나중에 어느 조건에서 나온 값인지 못 가른다 — 그 둘을 한 표에 넣는
# 것이 정확히 이 페이즈가 되풀이한 오류다.
OUT_SUMMARY="${OUT_SUMMARY:-k6-summary${pinned:+-pinned}${latency:+-latency}.json}"
# 아래에서 앞 회차의 요약을 지운다. 환경에서 온 값을 그대로 지우므로 무엇을
# 지우는지는 확인하고 간다.
case "$OUT_SUMMARY" in
    *.json) ;;
    *) echo "OUT_SUMMARY 는 .json 이어야 한다: '$OUT_SUMMARY'"; exit 2 ;;
esac
# 등록 왕복 산출물도 지우기 전에 검사한다. 뒤에서 검사하면 오타 하나가 엉뚱한 파일을
# 지운 사실이 회차를 다 돌린 뒤에 드러난다.
OUT_ENQUEUE="${OUT_ENQUEUE:-${OUT_SUMMARY%.json}-enqueue.txt}"
case "$OUT_ENQUEUE" in
    *.txt) ;;
    *) echo "OUT_ENQUEUE 는 .txt 여야 한다: '$OUT_ENQUEUE'"; exit 2 ;;
esac
OUT_ENQUEUE_BASE="${OUT_ENQUEUE%.txt}-base.txt"
# 아래에서 `tee` 로 덮어쓴다. 요약과 같은 이유로 무엇을 지우는지 보고 간다.
OUT_LOG="${OUT_LOG:-k6-spike${pinned:+-pinned}${latency:+-latency}.log}"
case "$OUT_LOG" in
    *.log) ;;
    *) echo "OUT_LOG 는 .log 여야 한다: '$OUT_LOG'"; exit 2 ;;
esac

command -v k6 >/dev/null || { echo "k6 가 없다"; exit 2; }

echo "착수 판정 회차 · 쿠폰 ${COUPON}"
# **이미지를 먼저 짓는다.** compose 는 JAR 이 바뀌어도 있는 이미지를 그대로 쓴다 —
# 컨테이너만 지우면 낡은 바이너리를 재고 그 값이 계획서에 적힌다. 이 브랜치가
# 되돌린 판정들이 전부 그 계열이다.
jar=${WAITING_JAR:-build/libs/waiting.jar}
if [ ! -f "$jar" ]; then
    echo "실행 JAR 이 없다: $jar — ./gradlew build 를 먼저 돌린다"; exit 2
fi
$COMPOSE build gateway backend >/dev/null 2>&1 || { echo "이미지를 못 지었다"; exit 2; }
# **게이트웨이도 새로 만든다.** 앞 회차가 깎아 둔 회복 램프를 그대로 들고
# 있으면 크레딧이 1 에서 안 오르고, 예열이 3 분을 기다리다 죽는다. 예열
# 컨테이너는 unhealthy 로 남으면 `--wait` 가 기다리지 않고 그대로 실패로 읽는다.
#
# **겹침이 남긴 스텁은 그냥 둔다.** `--remove-orphans` 를 쓰면 컴포즈 프로젝트
# 이름이 디렉터리 basename 이라 워크트리들이 같은 `load` 를 공유하는 탓에, 다른
# 워크트리에서 도는 라우팅 회차의 컨테이너까지 지운다. 스텁이 살아 있어도
# 게이트웨이는 레디스에 등록된 노드만 보므로 이 회차에 안 섞인다.
$COMPOSE rm -sf gateway warmup >/dev/null 2>&1
$COMPOSE up -d --wait --wait-timeout 240 || { echo "스택을 못 세웠다"; exit 2; }

# **부하 생성기도 레디스 코어를 피한다.** 2 만 VU 를 띄우는 쪽이 호스트를 다
# 먹으면 레디스만 격리한 뜻이 없다 — 실제로 그 회차에서 응답 중앙값이 10.6 초로
# 늘고 제어 평면이 250ms 안에 못 읽어 타임아웃이 났다. 레디스 CPU 는 낮은데
# 나머지가 밀린 것이고, 그러면 재는 것이 또 레디스가 아니다.
#
# **다만 생성기와 게이트웨이는 서로 안 갈린다.** 아래 목록은 `compose.pinned.yml`
# 의 게이트웨이 몫과 **글자 그대로 같다** — 레디스 코어만 비우고 둘은 열한 개를
# 통째로 나눠 쓴다. 그래서 게이트웨이가 쓴 CPU 중 얼마가 생성기와 다툰 몫인지
# 이 회차로는 못 가른다. 같은 조건의 두 회차가 유입 1,390 과 2,127 로 갈린 것이
# 그 징후다 (AIJ-0349).
#
# **좁혀서 가르는 길은 막혀 있다.** 게이트웨이 쪽을 좁히면 그쪽이 병목이 되어
# 재려던 것이 또 바뀐다 — `compose.pinned.yml` 이 그 실측을 든다. 열두 코어
# 한 대에서는 못 가르고, 생성기가 다른 기계로 나가야 갈린다.
runner=""
if [ -n "$pinned" ]; then
    # **없으면 말한다.** 조용히 안 묶으면 격리했다고 믿는 회차가 안 격리된
    # 조건으로 돌고, 그 값이 "고정해서 쟀다" 로 기록된다.
    if command -v taskset >/dev/null 2>&1; then
        # 코어 지도는 `compose.pinned.yml` 이 든다 — 거기를 고치면 여기도
        # 고친다. 이 기계(12 코어)를 못 박은 값이다.
        runner="taskset -c 1-11"
    else
        echo "::error title=착수 판정::taskset 이 없다 — 생성기를 격리 못 한다"
        exit 2
    fi
fi


# **트래픽으로 예열한다** (CY-975). 예열 컨테이너는 크레딧이 문턱에 닿기를 기다릴 뿐
# 요청을 한 건도 안 보낸다. 그대로 재면 등록 경로의 JIT 가 식은 채 스파이크를 맞아,
# 이 회차가 제품이 아니라 계기가 식었다는 사실을 잰다 — 실측으로 등록 평균이 166ms 와
# 12ms 로 갈렸다 (AIJ-0353).
#
# **회차를 비우고 프로브를 띄우기 전에 돈다.** 판정은 레디스 CPU 봉우리를 표본 파일
# 전체의 최댓값으로 집으므로, 예열 봉우리가 그 파일에 남으면 착수 여부를 예열이
# 정한다 — 등록 기준선에서 고친 것과 똑같은 오염이다.
case "${WARMUP_SPIKE:-0}" in
    0|false|no) : ;;
    *)
        echo "예열 스파이크 · 쿠폰 ${WARMUP_COUPON}"
        warm_summary=$(mktemp)
        warm_log=$(mktemp)
        # **다른 쿠폰으로 데운다.** 같은 쿠폰으로 돌리면 크레딧이 올라간 채 본 회차가
        # 시작해 전원이 통과하고 줄에 선 것이 0 이 된다 — 실측에서 뒷단 실패가 58.8%
        # 였다. JIT 는 JVM 몫이라 어느 쿠폰으로 데워도 같은 경로가 데워진다.
        # **가볍게 데운다.** 본 회차와 같은 크기로 돌리면 제어 평면이 흔들려 —
        # 실측에서 리더 확인이 한 번 실패하고 뒷단 실패가 58.8% 였다 — 그 상태로
        # 본 회차가 시작한다. JIT 는 수천 번이면 붙으므로 그만큼만 친다.
        COUPON="$WARMUP_COUPON" SPIKE_USERS="$WARMUP_USERS" \
            $runner k6 run --summary-export="$warm_summary" \
            test/load/open-spike.js >"$warm_log" 2>&1
        warm_rc=$?
        # **종료 코드를 본다.** 안 보면 예열이 한 번도 안 돌아도 조용히 지나가고,
        # 그러면 "예열을 붙였는데 느려졌다" 같은 엉뚱한 결론이 난다 — 실제로 났다.
        # `--no-summary` 는 이 k6 판에 없어서 예열이 통째로 안 돌았다.
        #
        # **99 는 돌았다는 뜻이다.** 임계 위반이고, 예열은 임계로 판단할 회차가 아니다.
        # 안 돈 것(플래그 오류·k6 없음)과 갈라야 "예열했다" 가 사실이 된다.
        case "$warm_rc" in
            0|99) ;;
            *) echo "::error title=착수 판정::예열 스파이크가 ${warm_rc} 로 끝났다 — 안 돈 것이라 이 회차는 예열 없이 잰 것이 된다"
               tail -20 "$warm_log" >&2
               exit 2 ;;
        esac
        # **돌았다는 것으로는 모자란다.** 데우려는 것이 등록 경로인데, 예열이 통과
        # 경로로만 빠지면 그 경로를 한 번도 안 지나고 종료 코드는 그대로 99 다 —
        # 막으려던 조용한 통과가 한 칸 뒤에 다시 서는 셈이다.
        warm_queued=$(jq -r '(.metrics.queued_responses.values.count
            // .metrics.queued_responses.count) // 0' "$warm_summary" 2>/dev/null)
        case "$warm_queued" in
            ''|0|null) echo "::error title=착수 판정::예열이 줄에 한 건도 안 세웠다 — 등록 경로가 안 데워졌다"
                       tail -20 "$warm_log" >&2
                       rm -f "$warm_summary" "$warm_log"
                       exit 2 ;;
        esac
        echo "예열 등록 ${warm_queued} 건"
        rm -f "$warm_summary" "$warm_log"
        # 예열 쿠폰의 줄을 치운다. 재는 쿠폰은 아래에서 따로 비우고 IDLE 을 확인한다.
        $COMPOSE exec -T redis redis-cli DEL \
            "queue:{$WARMUP_COUPON}" "admitted:{$WARMUP_COUPON}" \
            "maxscore:{$WARMUP_COUPON}" "grace:{$WARMUP_COUPON}" \
            "alive:{$WARMUP_COUPON}" "dropfence:{$WARMUP_COUPON}" \
            "applyfence:{$WARMUP_COUPON}" >/dev/null 2>&1
        # 제어 평면이 가라앉기를 기다린다. 스냅샷 한 주기로는 모자란다.
        sleep "$WARMUP_SETTLE_SEC"
        ;;
esac


# **`routing-lib.sh` 의 `wait_for_idle_queue` 와 같은 절차다.** 그것을 안 부르는
# 이유는 그 라이브러리가 라우팅 겹침(`compose.routing.yml`)과 스텁 셋의 여유
# 값을 전제하는데, 이 회차는 CI 와 같은 모양이어야 해서 `compose.yml` 하나로만
# 돌기 때문이다. **지우는 키 목록이 두 곳에 있다** — `RedisKeys` 에 쿠폰별 키가
# 늘면 여기와 `routing-lib.sh` 를 둘 다 고쳐야 한다.
#
# **줄 키만 지우면 안 된다.** 입장 커서와 최대 순번이 남으면 리더가 줄을
# 비었다고 안 보고 쿠폰을 QUEUEING 으로 되돌리며, 판정은 IDLE 이 아니면 무조건
# 줄에 세운다(추월 금지) — 첫 요청부터 202 이거나 QUEUE_FULL 이다.
# **쿠폰별 키 일곱을 다 지운다.** 셋만 지우면 이탈 기록과 생존 신호와 배분
# 펜스가 앞 회차 값을 들고 넘어가, 새 회차의 첫 배분이 앞 회차의 펜스를 본다.
# 재고(`stock:`)는 시더가 관리하므로 안 건드린다.
empty_and_wait_idle() {
    $COMPOSE exec -T redis redis-cli DEL \
        "queue:{$COUPON}" "admitted:{$COUPON}" "maxscore:{$COUPON}" \
        "grace:{$COUPON}" "alive:{$COUPON}" "dropfence:{$COUPON}" \
        "applyfence:{$COUPON}" >/dev/null 2>&1

    local state _
    for _ in $(seq 1 30); do
        state=$($COMPOSE exec -T redis redis-cli HGET gw:snapshot "$COUPON" 2>/dev/null)
        case "$state" in *:IDLE:*) return 0 ;; esac
        sleep 1
    done
    return 1
}

if ! empty_and_wait_idle; then
    echo "::error title=착수 판정::줄 모드가 안 꺼진다 — 이 상태로는 못 잰다"
    exit 2
fi

# **발행자가 IDLE 이어도 노드는 아직 아니다.** 각 게이트웨이는 스냅샷을 주기로
# 받아 가므로, 여기서 본 IDLE 이 노드에 닿기까지 한 주기가 걸린다. 200ms 램프
# 짜리 스파이크에서는 그 창이 회차 전체다 — 넉넉히 한 주기를 더 준다.
sleep "${SNAPSHOT_SETTLE_SEC:-2}"

# **앞 회차의 산출물이 남으면 안 된다.** k6 가 요약을 못 남기고 죽거나 프로브가
# 뜨기 전에 끝나면, 앞 회차의 것이 이번 회차 것과 짝지어져 판정을 낸다 — 앞
# 회차가 남긴 것 때문에 판정이 갈렸다는 것이 바로 이 러너를 만든 이유다.
rm -f "$OUT_SUMMARY" "$OUT_OPS" "$OUT_ENQUEUE" "$OUT_ENQUEUE_BASE"

# **부하 전 개수를 먼저 적어 둔다** (CY-936). 예열은 이 줄보다 앞에서 끝나므로
# 예열 트래픽은 차분에 안 섞인다 — 등록 지표에 쿠폰 구분이 없어, 앞뒤가 바뀌면
# 2 만 요청 회차의 등록 건수가 21,808 로 찍힌다. 개수는 누적이고 분위수 창은 10 분마다 도므로,
# 증분을 안 보면 이번 회차에 등록이 0 건이어도 예열 때의 값이 회차 값으로 인용된다.
#
# **명령별 지연도 같이 긁는다.** `LATENCY=1` 이 아니면 그 줄이 아예 안 나오므로, 없다는
# 것이 곧 안 켠 회차라는 뜻이다 — 빈 값을 0 으로 읽지 않는다.
scrape_enqueue() {
    $COMPOSE exec -T gateway wget -qO- http://localhost:8081/actuator/prometheus 2>/dev/null \
        | grep -E '^(waiting_queue_enqueue_latency_seconds|lettuce_command_)' > "$1" || true
}
# **파일이 비었는지로 가르면 안 된다.** 계기를 켠 회차는 lettuce 줄이 들어차므로, 등록 지표가
# 통째로 사라져도 파일이 안 빈다 — 갈라 두려던 "못 긁음" 과 "0 건" 이 다시 붙는다.
has_enqueue() {
    grep -q '^waiting_queue_enqueue_latency_seconds' "$1"
}
scrape_enqueue "$OUT_ENQUEUE_BASE"
if ! has_enqueue "$OUT_ENQUEUE_BASE"; then
    echo "::warning title=착수 판정::회차 전 등록 지표를 못 긁었다 — 등록 p99 는 안 적는다"
fi

test/load/redis-probe.sh "$OUT_OPS" &
probe=$!
trap 'kill "$probe" 2>/dev/null' EXIT

# **프로브가 떴는지 본다.** 주기 검증에 걸려 즉사하면 표본이 한 줄도 안 생기는데,
# 그 사실이 회차가 끝난 뒤 "표본이 비었다" 로만 드러난다 — 12 초를 버린 뒤다.
command sleep 1
if ! kill -0 "$probe" 2>/dev/null; then
    echo "::error title=착수 판정::프로브가 안 떴다 — 부하를 넣지 않는다"
    exit 2
fi

# **k6 의 출력을 남긴다.** 요약만 남기면 임계가 깨졌을 때 어떤 응답이 섞였는지
# 를 못 본다 — 요약은 실패 건수를 안 싣는 판이 있어서, 깨진 사실만 알고 원인은
# 모르는 상태가 된다.
rc=0
$runner k6 run --summary-export="$OUT_SUMMARY" test/load/open-spike.js 2>&1 \
    | tee "$OUT_LOG"
# **둘을 한 번에 집는다.** 앞 줄에 대입을 하나만 끼워도 PIPESTATUS 가 그 대입의 것으로 바뀌어,
# 둘째를 읽는 순간 미설정으로 죽는다 — 판정기까지 못 가고 회차가 통째로 버려졌다.
pipe_rc=("${PIPESTATUS[@]}")
rc=${pipe_rc[0]}
tee_rc=${pipe_rc[1]}

# **로그가 안 남았으면 그렇다고 말한다.** 로그를 남기는 것이 이 줄의 목적인데
# 실패를 넘기면, 임계가 깨졌을 때 무엇이 섞였는지 다시 못 본다 — 그것 때문에
# 두 회차를 버렸다.
if [ "$tee_rc" -ne 0 ]; then
    echo "::error title=착수 판정::k6 로그를 못 남겼다: $OUT_LOG"
    exit 2
fi
# **신호만 보내고 판정하면 안 된다.** 프로브는 신호를 받고 나서 안쪽 루프를
# 걷고 파이프를 닫고 마지막 표본을 적는다. `kill` 은 그 일이 끝나기를 안
# 기다리므로, 그대로 판정하면 마지막 쓰기와 읽기가 겹친다.
kill "$probe" 2>/dev/null
wait "$probe" 2>/dev/null
trap - EXIT

# **등록 왕복의 분위수를 스택 내리기 전에 긁는다** (CY-936). 응답 분위수에는 판정·라우팅·뒷단이
# 섞여 있어 착수 게이트가 보라는 값이 아니다. 스택을 내리면 이 값도 같이 사라진다.
scrape_enqueue "$OUT_ENQUEUE"
# **못 긁은 것과 등록이 0 건인 것은 다르다.** 둘 다 "없음" 으로 적히므로 여기서 갈라 둔다.
if ! has_enqueue "$OUT_ENQUEUE"; then
    echo "::warning title=착수 판정::등록 왕복 지표를 못 긁었다 — 노드가 여럿이거나 관리 포트가 바뀌었다"
fi

echo "k6=$rc · 레디스 고정 ${pinned:+켬}${pinned:-끔}"
# **k6 가 빨개진 회차는 판정하지 않는다.** 임계 위반(99)은 줄이 안 섰거나 다
# 못 던졌다는 뜻이고, 그 회차의 봉우리는 재려던 것이 아니다.
if [ "$rc" -ne 0 ]; then
    echo "::error title=착수 판정::k6 가 ${rc} 로 끝났다 — 이 회차로는 판정하지 않는다"
    exit 1
fi
test/load/evaluate-shard-gate.sh "$OUT_OPS" "$OUT_SUMMARY" "$OUT_ENQUEUE" "$OUT_ENQUEUE_BASE"

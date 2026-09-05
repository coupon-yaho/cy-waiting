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
COMPOSE="docker compose -f test/load/compose.yml$pinned"
# **쿠폰은 노브가 아니다.** `open-spike.js` 가 `c2` 를 박아 두고 있어서, 여기만
# 바꾸면 c3 을 비우고 c3 이 IDLE 인 것을 본 뒤 c2 를 때린다 — 빈 줄 보증이
# 통째로 다른 쿠폰 얘기가 된다. 시나리오를 고칠 때 같이 고친다.
COUPON=c2
OUT_OPS="${OUT_OPS:-redis-ops${pinned:+-pinned}.txt}"
# **산출물 이름에 조건을 싣는다.** 고정한 회차와 안 한 회차가 같은 파일에
# 덮이면 나중에 어느 조건에서 나온 값인지 못 가른다 — 그 둘을 한 표에 넣는
# 것이 정확히 이 페이즈가 되풀이한 오류다.
OUT_SUMMARY="${OUT_SUMMARY:-k6-summary${pinned:+-pinned}.json}"
# 아래에서 앞 회차의 요약을 지운다. 환경에서 온 값을 그대로 지우므로 무엇을
# 지우는지는 확인하고 간다.
case "$OUT_SUMMARY" in
    *.json) ;;
    *) echo "OUT_SUMMARY 는 .json 이어야 한다: '$OUT_SUMMARY'"; exit 2 ;;
esac
# 아래에서 `tee` 로 덮어쓴다. 요약과 같은 이유로 무엇을 지우는지 보고 간다.
OUT_LOG="${OUT_LOG:-k6-spike${pinned:+-pinned}.log}"
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

# **`routing-lib.sh` 의 `wait_for_idle_queue` 와 같은 절차다.** 그것을 안 부르는
# 이유는 그 라이브러리가 라우팅 겹침(`compose.routing.yml`)과 스텁 셋의 여유
# 값을 전제하는데, 이 회차는 CI 와 같은 모양이어야 해서 `compose.yml` 하나로만
# 돌기 때문이다. **지우는 키 목록이 두 곳에 있다** — `RedisKeys` 에 쿠폰별 키가
# 늘면 여기와 `routing-lib.sh` 를 둘 다 고쳐야 한다.
#
# **줄 키만 지우면 안 된다.** 입장 커서와 최대 순번이 남으면 리더가 줄을
# 비었다고 안 보고 쿠폰을 QUEUEING 으로 되돌리며, 판정은 IDLE 이 아니면 무조건
# 줄에 세운다(추월 금지) — 첫 요청부터 202 이거나 QUEUE_FULL 이다.
# **쿠폰별 키 여섯을 다 지운다.** 셋만 지우면 이탈 기록과 생존 신호와 배분
# 펜스가 앞 회차 값을 들고 넘어가, 새 회차의 첫 배분이 앞 회차의 펜스를 본다.
# 재고(`stock:`)는 시더가 관리하므로 안 건드린다.
$COMPOSE exec -T redis redis-cli DEL \
    "queue:{$COUPON}" "admitted:{$COUPON}" "maxscore:{$COUPON}" \
    "grace:{$COUPON}" "alive:{$COUPON}" "dropfence:{$COUPON}" >/dev/null 2>&1

state=""
for _ in $(seq 1 30); do
    state=$($COMPOSE exec -T redis redis-cli HGET gw:snapshot "$COUPON" 2>/dev/null)
    case "$state" in *:IDLE:*) break ;; *) state=""; sleep 1 ;; esac
done
if [ -z "$state" ]; then
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
rm -f "$OUT_SUMMARY" "$OUT_OPS"

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
# **부하 생성기도 레디스 코어를 피한다.** 2 만 VU 를 띄우는 쪽이 호스트를 다
# 먹으면 레디스만 격리한 뜻이 없다 — 실제로 그 회차에서 응답 중앙값이 10.6 초로
# 늘고 제어 평면이 250ms 안에 못 읽어 타임아웃이 났다. 레디스 CPU 는 낮은데
# 나머지가 밀린 것이고, 그러면 재는 것이 또 레디스가 아니다.
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

rc=0
$runner k6 run --summary-export="$OUT_SUMMARY" test/load/open-spike.js 2>&1 \
    | tee "$OUT_LOG"
rc=${PIPESTATUS[0]}
# **신호만 보내고 판정하면 안 된다.** 프로브는 신호를 받고 나서 안쪽 루프를
# 걷고 파이프를 닫고 마지막 표본을 적는다. `kill` 은 그 일이 끝나기를 안
# 기다리므로, 그대로 판정하면 마지막 쓰기와 읽기가 겹친다.
kill "$probe" 2>/dev/null
wait "$probe" 2>/dev/null
trap - EXIT

echo "k6=$rc · 레디스 고정 ${pinned:+켬}${pinned:-끔}"
# **k6 가 빨개진 회차는 판정하지 않는다.** 임계 위반(99)은 줄이 안 섰거나 다
# 못 던졌다는 뜻이고, 그 회차의 봉우리는 재려던 것이 아니다.
if [ "$rc" -ne 0 ]; then
    echo "::error title=착수 판정::k6 가 ${rc} 로 끝났다 — 이 회차로는 판정하지 않는다"
    exit 1
fi
test/load/evaluate-shard-gate.sh "$OUT_OPS" "$OUT_SUMMARY"

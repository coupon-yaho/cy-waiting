#!/usr/bin/env bash
# 서킷 진입·유지·회복 실측 — **게이트웨이 여러 대.**
#
# 서킷 해제 램프는 게이트웨이 한 대짜리 단위 시험으로만 잡혀 있었다. 두 가지가
# 한 대로는 원리적으로 안 잡힌다.
#
#   - 램프의 하한이 `노드 수 × 유휴 나눗값` 이라 노드 수가 곧 그 항의 값이다.
#     한 대면 2 인데 스무 대면 40 이다.
#   - 승계로 이어받은 노드는 조인 적이 없어 램프가 아예 안 걸린다. 리더가
#     하나뿐이면 이어받을 상대가 없어 그 구멍이 열리지도 않는다.
#
# 그래서 이 하네스는 **게이트웨이를 둘 이상 띄우고, 회복 도중에 리더를 죽인다.**
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1

# **합성 프로브는 켜서도 꺼서도 잰다.** RC3 과 RC4 가 폴링 간격 하나로 반대로
# 밀리는 것이 이 회차가 드러낸 것이고 (AIJ-0249), 프로브는 그 얽힘을 끊으려는
# 장치다. 두 조건의 값을 같은 표에 섞지 않으려고 산출물 이름에 조건을 싣는다.
case "${PROBE:-}" in
    ''|0|false|no) probe="" ;;
    *) probe=" -f test/load/compose.probe.yml" ;;
esac
COMPOSE="docker compose -f test/load/compose.yml -f test/load/compose.multi.yml \
-f test/load/compose.limits.yml$probe"

# **예열 문턱을 상한에 맞춘다.** 겹침의 기본값은 200 이라, 상한을 그보다 낮게
# 잡은 회차는 예열이 영영 안 끝나고 스택이 기동에서 죽는다.
BACKEND_CREDITS="${BACKEND_CREDITS:-300}"
export BACKEND_CREDITS
export WARMUP_CREDIT="${WARMUP_CREDIT:-$BACKEND_CREDITS}"

GATEWAYS="${GATEWAYS:-2}"
# **한산 통과 상한 아래로 둔다.** 넘으면 줄이 서고, 줄이 한 번 서면 추월 금지
# 때문에 그 뒤로 아무도 뒷단에 안 닿는다 — 회복 구간의 도착이 0 이라 재려던
# 봉우리를 못 잰다. 실측에서 유입 200 이 정확히 그렇게 끝났다.
RATE="${RATE:-60}"
# 각 구간의 길이(초). 정상은 기준선을 만들 만큼, 유지는 조임이 붙어 있는지 볼
# 만큼, 회복은 램프가 끝날 만큼이면 된다.
NORMAL_SEC="${NORMAL_SEC:-20}"
HOLD_SEC="${HOLD_SEC:-15}"
RECOVER_SEC="${RECOVER_SEC:-40}"
# 승계를 언제 넣을지. **램프가 걸려 있는 동안이라야 한다** — 램프가 끝난 뒤에
# 죽이면 직전 값이 이미 상한이라 계단 검사가 항등적으로 통과한다. 배수를 4 로
# 올려 램프가 3~4초로 짧아졌으므로 그 안쪽으로 잡는다.
HANDOVER_AFTER_SEC="${HANDOVER_AFTER_SEC:-2}"
# **죽이는 시각과 표본을 거두는 시각은 다른 값이다.** 승계를 램프 안으로 당기느라
# 이 둘을 같이 줄였더니 램프가 끝나기 전에 표본이 멎어, 맞게 도는 회차가
# "회복이 안 끝났다" 로 적혔다.
TAIL_SEC="${TAIL_SEC:-15}"
SAMPLE_MS="${SAMPLE_MS:-200}"
# **자극은 지연이다. 5xx 가 아니다.** 게이트웨이는 상태 코드를 서킷에 안 물린다 —
# 5xx 는 뒷단이 요청을 받은 뒤에 낸 답이라, 그걸 실패로 세고 재시도하면 그 한 건이
# 곧 초과 발급이기 때문이다. 그래서 서킷을 여는 길은 느린 호출뿐이고, 이 값은
# 느린 호출 문턱(1.9초)보다 넉넉히 커야 한다.
FAULT_LATENCY_MS="${FAULT_LATENCY_MS:-3000}"
# 서킷이 10초 창을 채우고, 클러스터 투표가 돌고, 배분이 그 값을 읽는 데 몇 틱이
# 든다. 느린 호출은 그 지연만큼 늦게 창에 들어가므로 자극 지연도 같이 센다.
SETTLE_SEC=$((12 + FAULT_LATENCY_MS / 1000))
COUPON="${COUPON:-c1}"
# **산출물 이름에 조건을 싣는다.** 프로브를 켠 회차와 안 켠 회차가 같은 파일에
# 덮이면 나중에 어느 조건에서 나온 값인지 못 가른다.
OUT="${OUT:-circuit-recovery${probe:+-probe}.txt}"

case "$OUT" in
    *.txt) ;;
    *) echo "OUT 은 .txt 여야 한다: '$OUT'"; exit 2 ;;
esac
# **이름을 직접 대도 조건은 남아야 한다.** 조건을 파일 이름에만 실었으므로,
# 표식이 없으면 두 조건의 값이 같은 이름으로 덮인다.
# **양쪽을 다 본다.** 켠 회차가 표식 없는 이름을 쓰는 것만 막으면, 안 켠 회차가
# 표식 있는 이름으로 앞 회차의 산출물을 덮는다 — 조건이 뒤바뀐 채로 남는다.
case "$probe:$OUT" in
    ?*:*probe*) ;;
    ?*:*) echo "PROBE 를 켰으면 OUT 이름에 probe 가 들어가야 한다: '$OUT'"; exit 2 ;;
    :*probe*) echo "PROBE 를 안 켰으면 OUT 이름에 probe 가 들어가면 안 된다: '$OUT'"; exit 2 ;;
esac

for n in GATEWAYS RATE NORMAL_SEC HOLD_SEC RECOVER_SEC HANDOVER_AFTER_SEC TAIL_SEC SAMPLE_MS FAULT_LATENCY_MS; do
    v=$(eval "printf '%s' \"\$$n\"")
    case "$v" in
        ''|*[!0-9]*) echo "$n 은 양의 정수여야 한다: '$v'"; exit 2 ;;
    esac
    [ "$v" -gt 0 ] || { echo "$n 은 0 보다 커야 한다: $v"; exit 2; }
done

# **한 대짜리로는 이 시나리오가 성립 안 한다.** 승계가 원리적으로 안 생긴다.
# 판정기도 같은 이유로 거절하지만, 여기서 먼저 끊어야 40 초를 안 버린다.
[ "$GATEWAYS" -ge 2 ] || { echo "게이트웨이는 둘 이상이어야 한다: $GATEWAYS"; exit 2; }
[ "$HANDOVER_AFTER_SEC" -lt "$RECOVER_SEC" ] || {
    echo "승계 시각이 회복 구간 밖이다: $HANDOVER_AFTER_SEC >= $RECOVER_SEC"; exit 2; }
command -v k6 >/dev/null || { echo "k6 가 없다 — 고정 유입 실행기가 필요하다"; exit 2; }

work=$(mktemp -d) || exit 1
sampler=""
memsampler=""
loadpid=""
cleanup() {
    [ -n "$sampler" ] && kill "$sampler" 2>/dev/null
    [ -n "$memsampler" ] && kill "$memsampler" 2>/dev/null
    [ -n "$loadpid" ] && kill "$loadpid" 2>/dev/null
    # 자극을 남기지 않는다. 남으면 다음 회차가 시작부터 조인 채로 돈다.
    $COMPOSE exec -T backend wget -qO- 'http://localhost:8090/stub/latency?ms=0' \
        >/dev/null 2>&1
    rm -rf "$work"
}
trap cleanup EXIT INT TERM

r() { $COMPOSE exec -T redis redis-cli "$@" 2>/dev/null | tr -d '\r'; }

echo "게이트웨이 ${GATEWAYS}대 · 유입 ${RATE}/s · 정상 ${NORMAL_SEC}s · 유지 ${HOLD_SEC}s · 회복 ${RECOVER_SEC}s"

jar=${WAITING_JAR:-build/libs/waiting.jar}
[ -f "$jar" ] || { echo "실행 JAR 이 없다: $jar — ./gradlew build 를 먼저 돌린다"; exit 2; }

# **뒷단 스텁도 같이 짓는다.** 게이트웨이만 지으면 스텁을 고쳐도 옛 이미지가
# 그대로 돌고, 새로 넣은 자극이 조용히 없는 것이 된다 — 그 회차는 재려던 것
# 대신 아무것도 안 잰 것이 되는데 초록으로 끝난다.
if ! $COMPOSE build gateway backend >"$work/build.log" 2>&1; then
    echo "이미지를 못 지었다"; tail -20 "$work/build.log" | sed 's/^/  /'; exit 2
fi

# **스택을 새로 세운다.** 앞 회차의 등록이 남으면 노드 수 분모가 갈리고, 그
# 회차는 적힌 것과 다른 조건에서 잰 것이 된다. 고장 표시도 같이 사라진다.
$COMPOSE down --remove-orphans >/dev/null 2>&1
if ! $COMPOSE up -d --wait --wait-timeout 180 --scale gateway="$GATEWAYS" \
        >"$work/up.log" 2>&1; then
    echo "스택을 못 세웠다 — $work 의 up.log 를 본다"
    tail -20 "$work/up.log"
    exit 2
fi

# **실제로 열린 포트를 찾는다.** 범위로 열면 어느 컨테이너가 어느 포트를 받는지
# 순서가 안 정해진다. 박아 두면 한 대에만 전부 보내면서 여럿에 나눠 보냈다고 적는다.
bases=""
for idx in $(seq 1 "$GATEWAYS"); do
    port=$($COMPOSE port --index "$idx" gateway 8080 2>/dev/null | sed 's/.*://')
    case "$port" in
        ''|*[!0-9]*) echo "게이트웨이 $idx 의 포트를 못 찾았다"; exit 2 ;;
    esac
    bases="${bases:+$bases,}http://localhost:$port"
done
echo "게이트웨이: $bases"

# **정말 여러 대가 붙었는지 본다.** 한 대만 등록되면 분모가 안 갈리고, 그 회차는
# 한 대짜리를 여러 대라고 적은 것이 된다. 등록부에는 `#` 으로 시작하는 예약
# 항목(서킷 표·통과 수)이 같이 들어 있어 그것까지 세면 한 대가 여럿으로 보인다.
registered=$(r --raw HKEYS gw:instances | grep -cv '^#')
case "$registered" in
    ''|*[!0-9]*) echo "등록부를 못 읽었다"; exit 2 ;;
esac
[ "$registered" -eq "$GATEWAYS" ] || {
    echo "등록된 게이트웨이가 $registered 대다 — $GATEWAYS 대를 기대했다"; exit 2; }
echo "등록된 게이트웨이: $registered 대"

# **여유 램프가 다 오를 때까지 기다린다.** 수집기는 처음 본 뒷단에 60초 램프를
# 건다. 그 도중에 기준선을 뜨면 목표가 실제보다 낮게 잡히고, 회복이 그 낮은
# 값에 닿는 것만으로 충족이 된다 — 재려던 복귀를 안 잰 회차가 초록으로 끝난다.
#
# **목표를 모르는 채로 기다린다.** 값이 더 안 오르면 다 오른 것이다. 목표를
# 박아 두면 스텁의 여유를 바꾸는 날 이 대기가 조용히 영영 안 끝난다.
echo "여유 램프를 기다린다"
plateau=0
last=-1
for _ in $(seq 1 120); do
    credit=$(r HGET gw:snapshot '#credit')
    case "$credit" in
        ''|*[!0-9]*) sleep 1; continue ;;
    esac
    if [ "$credit" -le "$last" ] && [ "$credit" -gt 0 ]; then
        plateau=$((plateau + 1))
        [ "$plateau" -ge 5 ] && break
    else
        plateau=0
    fi
    last=$credit
    sleep 1
done
[ "$plateau" -ge 5 ] || { echo "여유 램프가 안 끝났다 (마지막 크레딧 $last)"; exit 2; }
echo "여유 램프 완료 — 크레딧 $last"

# **유입이 한산 통과 상한 안인지 본다.** 넘으면 줄이 서고, 그 회차는 회복이
# 아니라 줄을 잰 것이 된다 — 그것도 도착이 0 이라 아무것도 못 잰다.
# 유휴 비율의 기본값이 0.7 이라 0.6 이면 여유가 있다.
headroom=$(( last * 6 / 10 ))
if [ "$RATE" -ge "$headroom" ]; then
    echo "::error title=서킷 회복::유입 ${RATE}/s 가 한산 통과 여유 ${headroom}/s 이상이다 — 줄이 서면 뒷단이 요청을 못 받는다"
    exit 2
fi

# **자극이 스텁의 동시 한도 안이어야 한다.** 넘으면 스텁이 즉시 503 을 내는데,
# 그 503 은 서킷에 안 물리므로 느린 호출 비율을 희석해 서킷이 안 열린다 —
# 진입을 못 만든 회차가 나온다. 새 계수도 그 503 은 안 세므로 표본에서도 사라진다.
# **반쯤 열린 허가도 같이 든다.** 서킷이 열린 뒤 창마다 노드당 그 수만큼 더
# 들어간다. 안 더하면 기본값이 정확히 한도에 앉아, 스크립트가 스스로 금지한
# 경계에서 도는데 사전 검사는 통과한다.
half_open=${HALF_OPEN_PERMITS:-10}
depth=$(( RATE * FAULT_LATENCY_MS / 1000 + GATEWAYS * half_open ))
stub_cap=$($COMPOSE exec -T backend printenv MAX_INFLIGHT 2>/dev/null | tr -d '\r')
case "$stub_cap" in ''|*[!0-9]*) stub_cap=0 ;; esac
if [ "$stub_cap" -gt 0 ] && [ "$depth" -ge "$stub_cap" ]; then
    echo "::error title=서킷 회복::기대 물림 ${depth} 이 스텁 동시 한도 ${stub_cap} 이상이다 — 즉시 503 이 느린 호출을 희석한다"
    exit 2
fi

# ── 표본 뜨기 ────────────────────────────────────────────────────────────────
#
# **발행 크레딧을 읽는다. 게이지가 아니다.** `waiting.capacity.credit` 은 보고를
# 합친 값이라 조임·램프 구간에 실제 발행분과 갈린다 (AIJ-0245). 그 게이지로
# 재면 이 판정이 "아무 일도 없었다" 로 자동 통과한다.
: > "$work/samples.txt"
#
# **노드 수도 매번 읽는다.** 리더를 죽이면 그 수가 줄고, 한산 통과의 최소가
# 그 수를 따라간다 — 시작 값을 박아 두면 승계 뒤 회차가 실제보다 두 배 높은
# 문턱으로 판정된다. 지킨 회차가 미달로 적히는 자리다.
#
# **왕복을 줄인다.** 표본 하나에 도커를 세 번 치면 그 비용이 곧 표본 간격이 되고,
# 그 간격은 회복 봉우리를 나누는 분모다 — 계기가 굵어지면 봉우리가 뭉개진다.
# 크레딧과 노드 수는 스크립트 하나로 한 번에 받고, 스텁은 밖으로 열린 포트에서
# 직접 읽는다.
STUB_URL="${STUB_URL:-http://localhost:18090}"
#
# **노드별 서킷 표도 같이 받는다.** 크레딧 하나만 보면 "서킷이 아직 안 닫혔다" 와
# "표는 닫혔는데 게이트가 안 풀렸다" 가 같은 그림인데, 둘은 고칠 자리가 다르다.
# 등록부가 `#c:<id>` 에 그 노드가 본 뒷단 서킷을 싣는다.
SAMPLE_LUA="local n = 0
local votes = {}
for _, k in ipairs(redis.call('HKEYS', KEYS[2])) do
  if string.sub(k, 1, 3) == '#c:' then
    votes[#votes + 1] = redis.call('HGET', KEYS[2], k) or '-'
  elseif string.sub(k, 1, 1) ~= '#' then
    n = n + 1
  end
end
table.sort(votes)
local seen, uniq = {}, {}
for _, v in ipairs(votes) do
  if not seen[v] then seen[v] = true; uniq[#uniq + 1] = v end
end
return { redis.call('HGET', KEYS[1], '#credit') or '',
         tostring(n),
         table.concat(uniq, '|') }"

sample_loop() {
    local triple credit nodes votes served
    while :; do
        triple=$(r --raw EVAL "$SAMPLE_LUA" 2 gw:snapshot gw:instances)
        credit=$(printf '%s' "$triple" | sed -n 1p)
        nodes=$(printf '%s' "$triple" | sed -n 2p)
        votes=$(printf '%s' "$triple" | sed -n 3p)
        [ -n "$votes" ] || votes='-'
        # **받은 수를 센다. 처리 완료 수가 아니다.** 느린 구간에 밀린 것이
        # 회복 순간에 한꺼번에 끝나면 완료 수가 봉우리처럼 보인다 — 재려던
        # 유입이 아니라 밀린 일을 잰다 (RC4 는 수신 수로 잰다).
        served=$(curl -sf -m 2 "$STUB_URL/stub/health" 2>/dev/null \
            | sed 's/.*"accepted":\([0-9]*\).*/\1/')
        # **칸마다 따로 본다.** 붙여서 검사하면 크레딧이 빈 표본이 옆 칸의
        # 숫자에 묻혀 통과하고, 네 칸짜리 줄이 찍혀 회차 전체가 판정 불가가
        # 된다 — 표본 하나만 건너뛰면 될 일이었다.
        if printf '%s' "$credit" | grep -Eq '^[0-9]+$' \
                && printf '%s' "$served" | grep -Eq '^[0-9]+$' \
                && printf '%s' "$nodes" | grep -Eq '^[0-9]+$'; then
            printf '%s %s %s %s %s\n' "$(date +%s%3N)" \
                "$credit" "$served" "$nodes" "$votes"
        fi
        sleep "$(awk -v ms="$SAMPLE_MS" 'BEGIN{ printf "%.3f", ms / 1000 }')"
    done
}

mark() { printf '# %s\n' "$1" >> "$work/samples.txt"; }

# **상한에 얼마나 붙었는지 남긴다.** 봉우리는 정상 구간이 아니라 게이트가 풀려
# 억눌린 줄이 한꺼번에 나가는 순간에 온다. 표본은 회차 옆에 같이 둔다 — 봉우리
# 한 줄만 남기면 그것이 언제였는지를 나중에 못 본다.
report_memory() {
    if [ ! -s "$work/mem.txt" ]; then
        echo "메모리를 못 떴다 — 상한에 얼마나 붙었는지는 이 회차로 모른다"
        return
    fi
    cp "$work/mem.txt" "${OUT%.txt}-mem.txt"
    awk '$2 ~ /gateway/ {
           v=$3; u=v; sub(/[0-9.]+/, "", u); sub(/[A-Za-z]+$/, "", v)
           m = (u=="GiB") ? 1024 : (u=="KiB") ? 1/1024 : (u=="B") ? 1/1048576 : 1
           if (v * m > peak) { peak = v * m; lim = $4 } }
         END{ if (peak) printf "게이트웨이 메모리 봉우리: %.1fMiB / %s\n", peak, lim
              else print "메모리 표본에 게이트웨이 행이 없다" }' "$work/mem.txt"
}

# **노드별 허가 수를 구간별로 낸다.** 클러스터 합에서 나눠 짐작하면 열린 구간의
# 0 이 평균을 눌러, 재려던 half-open 의 공급이 아니라 회차 전체의 평균이 나온다.
#
# 구간을 갈라도 유지 쪽에는 서킷이 열려 있는 시간이 섞인다. 그 평균은 공급의
# 하한이지 half-open 이 받는 속도가 아니다 — 그 값은 회복 로그의 프로브 수가 든다.
report_calls() {
    if [ ! -s "$work/calls.txt" ]; then
        echo "노드별 서킷 호출을 못 떴다"
        return
    fi
    cp "$work/calls.txt" "${OUT%.txt}-calls.txt"
    span() {
        awk -v t0="$2" -v t1="$3" -v label="$1" '
            t1 <= 0 { next }
            # **모양이 어긋난 줄은 버린다.** 지표를 못 긁은 회차는 칸이 비어,
            # 그대로 더하면 통과 수가 뒤로 간다.
            NF != 9 { next }
            $1 >= t0 && $1 <= t1 {
                # **프로브 몫을 서킷 계수에서 뺀다.** 통과·실패는 같은 서킷에
                # 기록되므로 안 빼면 프로브를 켠 회차의 "초당 건수" 가 그만큼
                # 부풀어 두 조건이 비교가 안 된다. 바쁘다는 답은 자리를 돌려주고
                # 세기만 하므로 애초에 서킷 계수에 없다 — 따로 낸다.
                pr = $7 + $8
                calls = $3 + $4 - pr
                if (!(($2) in first)) {
                    first[$2] = calls; ft[$2] = $1; fa[$2] = $6
                    fn[$2] = $5; fp[$2] = pr; fb[$2] = $9
                }
                # 계수는 단조다. 줄면 컨테이너가 다시 뜬 것이므로 안 센다.
                if (calls < last[$2] || $6 < la[$2]) { next }
                last[$2] = calls; lt[$2] = $1; la[$2] = $6
                ln[$2] = $5; lp[$2] = pr; lb[$2] = $9
            }
            END {
                for (n in first) {
                    d = (lt[n] - ft[n]) / 1000
                    if (d <= 0) { continue }
                    printf "  %s %s 줄이 채운 서킷 표본 초당 %.2f건 (%d건 / %.1f초)\n",
                            n, label, (last[n] - first[n]) / d, last[n] - first[n], d
                    # 표시한 수는 리더 것만 는다. 노드별 구간이 조금씩 어긋나므로
                    # 분모는 그 노드 자신의 구간으로 나눈다.
                    if (la[n] > fa[n]) {
                        printf "  %s %s 배분이 표시한 수 초당 %.2f건 (%d건)\n",
                                n, label, (la[n] - fa[n]) / d, la[n] - fa[n]
                    }
                    # **못 들어간 요청과 프로브 몫을 같이 낸다.** 프로브가 반쯤
                    # 열린 자리를 먹으면 차례가 온 사람이 폴백으로 떨어지는데,
                    # 그 수가 안 나오면 예산을 정할 재료가 회차를 돌려도 안 생긴다.
                    if (ln[n] > fn[n] || lp[n] > fp[n] || lb[n] > fb[n]) {
                        printf "  %s %s 못 들어간 요청 %d건 · 프로브 표본 %d건 · "
                                "프로브가 만난 포화 %d건\n",
                                n, label, ln[n] - fn[n], lp[n] - fp[n], lb[n] - fb[n]
                    }
                }
            }' "$work/calls.txt"
    }
    span "유지" "${hold_at:-0}" "${recover_ms:-0}"
    span "회복" "${recover_ms:-0}" "${release_ms:-0}"
}

# **죽은 대는 그냥 없는 것이 된다.** 해제 판정이 살아 있는 컨테이너의 로그만 보고
# 판정기는 노드 수가 주는 것을 우리가 만든 자극으로 읽는다. 그러면 계기 사망이
# 제품 미달로 적힌다.
#
# **사인을 안 가린다.** OOMKilled 는 커널이 죽인 경우만 참이라, JVM 의
# OutOfMemoryError 도 기동 실패도 거짓이다. 그래서 살아 있는 대수로 본다.
# 우리가 죽인 리더는 id 로 견줘 뺀다.
check_deaths() {
    local expected="$GATEWAYS" alive
    [ -n "${leader:-}" ] && expected=$((GATEWAYS - 1))
    alive=$($COMPOSE ps -q gateway 2>/dev/null | grep -c .)
    if [ "$alive" -ne "$expected" ]; then
        echo "::error title=서킷 회복::게이트웨이가 $alive 대 남았다 — $expected 대를 기대했다. 이 회차로는 판정하지 않는다"
        for cid in $($COMPOSE ps -aq gateway); do
            printf '  %s %s\n' "${cid:0:12}" \
                "$(docker inspect --format '{{.State.Status}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}}' "$cid" 2>/dev/null)"
        done
        exit 2
    fi
}

# **메모리도 표본이다.** 파드 상한을 걸어 두고 실제 사용을 안 남기면, 조건이
# 깨져도 다음 회차가 모른다. 초당 여러 번은 못 뜬다 — `docker stats` 한 번이
# 수백 ms 다. 봉우리는 게이트가 풀리는 순간이라 2초면 잡힌다.
# **노드별로 서킷이 허가한 수를 뜬다.** 뒷단 도착률은 클러스터 합이라, 한 노드의
# 서킷이 half-open 에서 몇 건을 통과시켰는지와 재는 자리가 다르다. 그 둘이 화해가
# 안 돼 "공급이 병목" 의 근거가 반쪽으로 남아 있었다 (AIJ-0246).
#
# 관리 포트는 밖으로 안 열려 있다. 컨테이너 안에서 긁는다.
calls_of() {
    docker exec "$1" wget -qO- http://localhost:8081/actuator/prometheus 2>/dev/null \
        | awk '/^resilience4j_circuitbreaker_calls_seconds_count/ {
                 k = "?"; if ($0 ~ /kind="successful"/) k = "ok"
                 else if ($0 ~ /kind="failed"/) k = "fail"
                 else if ($0 ~ /kind="ignored"/) k = "ignored"
                 sum[k] += $NF }
               /^resilience4j_circuitbreaker_not_permitted_calls_total/ { np += $NF }
               # 프로브 몫. **서킷 계수에 섞여 있으므로 따로 안 빼면 프로브를 켠
               # 회차의 "초당 건수" 가 그만큼 부풀어 두 조건이 비교가 안 된다.
               /^waiting_probe_passed_total/ { pp += $NF }
               /^waiting_probe_failed_total/ { pf += $NF }
               /^waiting_probe_busy_total/ { pb += $NF }
               # 배분이 표시한 수. 리더만 는다 — 클러스터 합으로 본다.
               /^waiting_allocation_admitted_total/ { adm += $NF }
               END { printf "%d %d %d %d %d %d %d",
                       sum["ok"], sum["fail"], np, adm, pp, pf, pb }'
}

mem_loop() {
    local ids
    while :; do
        # **목록이 비면 안 부른다.** 인자가 없으면 docker stats 는 호스트의 모든
        # 컨테이너를 찍고, 그 값이 게이트웨이 봉우리로 적힌다.
        ids=$($COMPOSE ps -q gateway 2>/dev/null)
        if [ -n "$ids" ]; then
            # 컨테이너가 여럿이라 쪼개지는 게 맞다.
            # shellcheck disable=SC2046
            docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' $ids 2>/dev/null \
                | awk -v t="$(date +%s%3N)" '{ print t, $1, $2, $4 }' >> "$work/mem.txt"
        fi
        for cid in $ids; do
            printf '%s %s %s\n' "$(date +%s%3N)" "${cid:0:12}" "$(calls_of "$cid")" \
                >> "$work/calls.txt"
        done
        sleep 2
    done
}

# ── 부하 ─────────────────────────────────────────────────────────────────────
#
# **부하가 회차 전체를 덮어야 한다.** 구간 길이만 더해 놓고 진입 안정화와 해제
# 대기를 빼먹었더니, 회복 구간 뒤쪽 27초가 유입 0 으로 떴다. 그 표본으로 잰
# 도착률을 "서킷이 프로브를 못 채운다" 의 근거로 쓸 뻔했다 — 하네스가 부하를
# 멈춘 것과 제품이 요청을 못 받는 것은 다르다.
#
# 최악은 게이트가 끝내 안 풀리는 길이다. 그쪽이 더 길면 그 값을 쓴다.
tail_sec=$((HANDOVER_AFTER_SEC + TAIL_SEC))
[ $((RECOVER_SEC / 2)) -gt "$tail_sec" ] && tail_sec=$((RECOVER_SEC / 2))
total_sec=$((NORMAL_SEC + SETTLE_SEC + HOLD_SEC + RECOVER_SEC + tail_sec + 5))

# **동시 실행자를 자극에 맞춰 잡는다.** 물림은 유입 × 지연이다. 모자라면 회차가
# 통째로 판정 불가로 끝나고, 그때 고친 값이 조건도 같이 바꾼다.
# **재시도 간격만큼 회차가 더 물린다.** 자극 지연만 보면 예산이 모자라 흘린
# 회차가 나고, 흘리면 그 회차는 통째로 판정 불가다.
#
# **기계가 모자라면 밖에서 늘린다.** 다만 **늘려서 안 풀리는 회차가 있다** —
# 2026-09-09 이 기계에서 500 으로 1,020 회를, 1,500 으로 168 회를 흘렸는데 두 번
# 다 풀을 **전부** 썼다. 용량이 아니라 리더를 죽이는 창에서 VU 가 물려 있는
# 것이라, 늘리는 것으로는 안 없어진다. 흘린 회차는 통째로 판정 불가다.
vus=${VUS:-$(( RATE * (FAULT_LATENCY_MS / 1000 + 2) * 3 / 2 + 50 ))}
case "$vus" in
    ''|*[!0-9]*) echo "VUS 는 양의 정수여야 한다: '$vus'"; exit 2 ;;
esac
[ "$vus" -gt 0 ] || { echo "VUS 는 양의 정수여야 한다: '$vus'"; exit 2; }
# **표를 쓰러 오는 사람 수가 배수량의 천장이다.** 이들은 제품이 준 재시도 간격을
# 지키므로 한 사람이 초당 한 번꼴이다 — 천장이 곧 `HOLDERS` 다. 유입보다 적으면
# 기준선이 거기서 멎어, 봉우리 비의 분모가 상한과 멀어진다.
holders=${HOLDERS:-$(( RATE + 20 ))}
BASE_URLS="$bases" RATE="$RATE" DURATION="${total_sec}s" COUPON="$COUPON" VUS="$vus" \
    HOLDERS="$holders" \
    k6 run --quiet --summary-export="$work/k6.json" test/load/circuit-recovery.js \
    >"$work/k6.log" 2>&1 &
loadpid=$!

mark 정상
sample_loop >> "$work/samples.txt" &
sampler=$!
mem_loop &
memsampler=$!
sleep "$NORMAL_SEC"

# ── 진입 — 뒷단을 고장 낸다 ──────────────────────────────────────────────────
mark 진입
if ! $COMPOSE exec -T backend wget -qO- \
        "http://localhost:8090/stub/latency?ms=$FAULT_LATENCY_MS" >/dev/null 2>&1; then
    echo "자극을 못 넣었다"; exit 2
fi
sleep "$SETTLE_SEC"

mark 유지
hold_at=$(date +%s%3N)
sleep "$HOLD_SEC"

# ── 회복 — 고장을 걷고, 도중에 리더를 죽인다 ─────────────────────────────────
mark 회복
recover_ms=$(date +%s%3N)
if ! $COMPOSE exec -T backend wget -qO- 'http://localhost:8090/stub/latency?ms=0' \
        >/dev/null 2>&1; then
    echo "자극을 못 걷었다"; exit 2
fi
# **게이트가 풀린 순간을 제품에게 묻는다.** 크레딧으로 유추하면 승계로 노드
# 수가 줄 때 같은 값이 갑자기 상한 위로 보여, 안 풀린 회차를 풀렸다고 적는다.
# 배분은 그 전이를 로그로 남기므로 그것을 본다.
# **회복이 시작한 시각에 앵커한다.** 창을 넉넉히 잡으면 예열이나 정상 구간에
# 서킷이 한 번 흔들렸다 닫힌 줄을 주워, 안 풀린 회차가 풀린 것으로 적힌다.
# **존을 붙인다.** 없이 주면 도커가 로컬 시간으로 읽어, UTC 로 찍은 값이 이
# 호스트에서는 아홉 시간 앞을 가리킨다 — 창이 로그 전체가 되어 예열 구간에
# 한 번 흔들렸다 닫힌 줄을 주워, 안 풀린 회차가 풀린 것으로 적힌다.
recover_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
echo "게이트가 풀리기를 기다린다"
# **벽시계로 묶는다.** 반복 횟수로 잡으면 도커 명령이 빠른 호스트에서 예산이
# 실제 회복 시간보다 짧아져, 풀릴 회차를 "안 풀렸다" 로 적는다. 실측 해제가
# 42초인데 40회 × 0.5초는 그 아래다.
released=0
deadline=$(( $(date +%s) + RECOVER_SEC ))
while [ "$(date +%s)" -lt "$deadline" ]; do
    # **배분 쪽 짝을 본다.** 등록부의 게이트 로그는 노드마다 제 메모리로
    # 찍는 줄이라, 비리더가 먼저 풀면 리더는 아직 조인 값을 발행 중이다 —
    # 그 시점에 표시하면 맞게 도는 제품이 미달로 적힌다. 이 줄은 배분이
    # 실제로 조임을 푼 자리의 짝이라 크레딧과 같은 시각을 가리킨다.
    for cid in $($COMPOSE ps -q gateway); do
        # **`grep -q` 를 안 쓴다.** pipefail 아래서 grep 이 먼저 끝나면 앞쪽이
        # SIGPIPE 로 죽어 파이프라인이 실패로 읽힌다 — 로그가 커지는 회차에서만
        # 나고, 맞게 찾은 줄이 못 찾은 것이 된다.
        if [ "$(docker logs --since "$recover_at" "$cid" 2>&1 \
                | grep -c "서킷 회복 —")" -gt 0 ]; then
            released=1
            break
        fi
    done
    [ "$released" = 1 ] && break
    sleep 0.5
done
if [ "$released" != 1 ]; then
    # 표시를 안 쓴다. 판정기가 "안 풀렸다" 로 끊고 원인을 이름으로 부른다.
    echo "게이트가 안 풀렸다 — 승계는 건너뛴다"
    sleep "$((RECOVER_SEC / 2))"
    kill "$sampler" 2>/dev/null; sampler=""
    wait "$loadpid" 2>/dev/null; k6rc=$?
    loadpid=""
    cp "$work/samples.txt" "$OUT"
    kill "$memsampler" 2>/dev/null; memsampler=""
    report_memory
    report_calls
    check_deaths
    # **부하를 못 만든 회차를 제품 미달로 내보내지 않는다.** 정상 경로에만 이
    # 검사를 두었더니, 실측에서 정확히 이쪽 경로가 그것 없이 나갔다.
    if [ "$k6rc" -ne 0 ]; then
        echo "::error title=서킷 회복::k6 가 $k6rc 로 끝났다 — 이 회차로는 판정하지 않는다"
        # **로그를 남긴다.** 다섯 줄만 찍고 지우면 왜 흘렸는지를 다음 회차에
        # 다시 재현해야 한다.
        cp "$work/k6.log" "${OUT%.txt}-k6.log" 2>/dev/null
        tail -5 "$work/k6.log" | sed 's/^/  /'
        exit 2
    fi
    test/load/evaluate-circuit-recovery.sh "$OUT"
    exit $?
fi
mark 해제
release_ms=$(date +%s%3N)
echo "게이트가 풀렸다"
sleep "$HANDOVER_AFTER_SEC"

# **리더를 짚어서 죽인다.** 소유자 식별자는 기동마다 새로 만드는 UUID 라 밖에서
# 못 맞춘다. 대신 리더가 될 때 그 값을 로그에 남기므로, 잠금에 든 값과 같은 줄을
# 찍은 컨테이너가 리더다. 아무 대나 죽이면 승계가 안 일어난 회차를 승계라고 적는다.
# 잠금 값의 형식은 `<펜스 번호>|<소유자>` 다. 번호까지 넣어 찾으면 로그의
# 소유자와 절대 안 맞아, 리더가 멀쩡히 있는데 "못 짚었다" 로 끝난다.
lock=$(r GET scheduler:leader)
owner=${lock##*|}
# **컨테이너를 식별자로 짚는다.** `compose kill` 에는 인덱스가 없고, 인덱스와
# 컨테이너의 짝을 다른 명령으로 다시 맞추면 그 사이 순서가 바뀌었을 때 엉뚱한
# 대를 죽인다 — 그 회차는 승계가 아닌 것을 승계라고 적는다.
leader=""
if [ -n "$owner" ]; then
    for cid in $($COMPOSE ps -q gateway); do
        # **파일로 받아 찾는다.** 파이프로 바로 받으면 `grep -q` 가 앞쪽을
        # SIGPIPE 로 죽여 pipefail 이 실패로 읽고, `grep -c` 는 조기 종료를
        # 잃어 전량을 읽는다 — 그 지연이 승계 시각에 그대로 들어간다.
        docker logs "$cid" > "$work/leader.log" 2>&1
        if grep -q "리더가 됐다 — owner=$owner" "$work/leader.log"; then
            leader=$cid
            break
        fi
    done
fi
if [ -z "$leader" ]; then
    echo "::error title=서킷 회복::리더를 못 짚었다 (잠금 '$lock') — 승계를 못 만든다"
    exit 2
fi
echo "리더는 ${leader} — 죽인다"
# **죽인 뒤에 표시한다.** 앞에 쓰면 그 뒤 첫 표본이 아직 죽기 전 리더의 값이라,
# 이 하네스가 존재하는 이유 — 이어받은 노드에 램프가 안 걸린다 — 를 원리적으로
# 못 잰다. 리스가 초 단위라 그 한 표본이 늘 죽기 전 값이다.
docker kill --signal SIGKILL "$leader" >/dev/null 2>&1 || {
    echo "리더를 못 죽였다: $leader"; exit 2; }
mark 승계

sleep "$TAIL_SEC"

kill "$sampler" 2>/dev/null; sampler=""
kill "$memsampler" 2>/dev/null; memsampler=""
wait "$loadpid" 2>/dev/null; k6rc=$?
loadpid=""

cp "$work/samples.txt" "$OUT"
report_memory
report_calls
check_deaths

echo
if [ "$k6rc" -ne 0 ]; then
    echo "::error title=서킷 회복::k6 가 $k6rc 로 끝났다 — 이 회차로는 판정하지 않는다"
    cp "$work/k6.log" "${OUT%.txt}-k6.log" 2>/dev/null
    tail -5 "$work/k6.log" | sed 's/^/  /'
    exit 2
fi
echo "표본은 $OUT 에 있다. 판정은 test/load/evaluate-circuit-recovery.sh 가 낸다."
test/load/evaluate-circuit-recovery.sh "$OUT"

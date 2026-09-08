#!/usr/bin/env bash
# 뒷단 스텁의 부하 반영 헬스 경로 자기검증 (TS-9).
#
# **합성 프로브가 이 경로를 친다.** 정적 200 을 주는 경로를 치면 뒷단이 느리거나
# 죽어 있어도 서킷이 닫혀 회복이 거짓이 된다 (09-routing · AIJ-0249). 그래서
# 이 경로는 발급이 쓰는 것과 **같은 자원 상태**를 지나야 한다.
#
# `/stub/health` 와 갈라 둔다 — 그쪽은 컴포즈 헬스체크와 회차 사후 확인이 쓰는
# 계기라, 고장 중에 빨개지면 하네스가 자기 계기를 잃는다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

command -v node >/dev/null || { echo "node 가 없다"; exit 2; }

failed=0
PORT=${STUB_SELFTEST_PORT:-18190}
stub=""

start() {
    env PORT="$PORT" "$@" node test/load/backend-stub/server.js >/dev/null 2>&1 &
    stub=$!
    for _ in $(seq 1 50); do
        curl -sf -o /dev/null "http://127.0.0.1:$PORT/stub/health" && return 0
        sleep 0.1
    done
    echo "  스텁이 안 떴다"
    return 1
}

stop() {
    [ -n "$stub" ] && kill "$stub" 2>/dev/null
    wait "$stub" 2>/dev/null
    stub=""
}
trap stop EXIT

# 상태 코드만 본다. 문구는 스텁의 봉투가 정하고 그쪽은 발급 명세를 따른다.
expect_status() {
    local name=$1 want=$2 path=$3 got
    got=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT$path")
    if [ "$got" = "$want" ]; then
        echo "  ✓ $name"
    else
        echo "  ✗ $name — $got (기대 $want)"
        failed=1
    fi
}

# 걸린 시간을 밀리초로 잰다. 느린 뒷단을 프로브가 느리게 겪어야 서킷이 열린다.
elapsed_ms() {
    local start end
    start=$(date +%s%3N)
    curl -s -o /dev/null "http://127.0.0.1:$PORT$1"
    end=$(date +%s%3N)
    echo $((end - start))
}

expect_slower() {
    local name=$1 floor=$2 path=$3 got
    got=$(elapsed_ms "$path")
    if [ "$got" -ge "$floor" ]; then
        echo "  ✓ $name (${got}ms)"
    else
        echo "  ✗ $name — ${got}ms (${floor}ms 이상이어야 한다)"
        failed=1
    fi
}

# 계기가 낸 수. 프로브가 발급으로 세어지면 RC4 봉우리가 오염된다.
counter() {
    curl -s "http://127.0.0.1:$PORT/stub/health" \
        | python3 -c "import json,sys; print(json.load(sys.stdin)['$1'])"
}

expect_same_counter() {
    local name=$1 key=$2 path=$3 before after
    before=$(counter "$key")
    curl -s -o /dev/null "http://127.0.0.1:$PORT$path"
    after=$(counter "$key")
    if [ "$before" = "$after" ]; then
        echo "  ✓ $name"
    else
        echo "  ✗ $name — $key 가 $before 에서 $after 로 늘었다"
        failed=1
    fi
}

echo "뒷단 스텁 자기검증"

start || exit 2
expect_status "한산할 때는 200" 200 /stub/ready
# **프로브는 발급이 아니다.** 받은 건수에 섞이면 회복 봉우리(RC4)를 재는 자가
# 프로브 회차 수만큼 부풀고, 그 봉우리로 게이트를 판정하게 된다.
expect_same_counter "받은 건수에 안 섞인다" accepted /stub/ready
expect_same_counter "처리 건수에도 안 섞인다" served /stub/ready
# **계기는 고장 중에도 살아 있어야 한다.** 컴포즈 헬스체크와 회차 사후 확인이 쓴다.
expect_status "계기 경로는 그대로 200" 200 /stub/health

# 빨리 실패하는 고장. 프로브가 이것을 못 보면 죽은 대에 서킷이 닫힌다.
curl -s -o /dev/null "http://127.0.0.1:$PORT/stub/fault?status=503"
expect_status "고장이면 그 상태를 낸다" 503 /stub/ready
expect_status "고장 중에도 계기는 산다" 200 /stub/health
curl -s -o /dev/null "http://127.0.0.1:$PORT/stub/fault?status=0"
expect_status "고장을 끄면 돌아온다" 200 /stub/ready

# 느려지는 고장. 서킷은 5xx 가 아니라 느린 호출로 열린다 — 프로브가 그것을
# 안 겪으면 열린 서킷이 프로브 하나로 도로 닫힌다.
curl -s -o /dev/null "http://127.0.0.1:$PORT/stub/latency?ms=300"
expect_slower "느리면 프로브도 느리다" 250 /stub/ready
curl -s -o /dev/null "http://127.0.0.1:$PORT/stub/latency?ms=0"
stop

# 동시 한도. 발급이 못 들어가는 순간에 프로브만 통과하면 안 된다.
start MAX_INFLIGHT=1 LATENCY_MS=800 || exit 2
curl -s -o /dev/null "http://127.0.0.1:$PORT/api/v1/coupons/c1/issue" &
busy=$!
sleep 0.3
expect_status "포화면 못 받는다고 낸다" 503 /stub/ready
wait "$busy" 2>/dev/null
stop

[ "$failed" -eq 0 ] && echo "뒷단 스텁 자기검증 통과" || echo "뒷단 스텁 자기검증 실패"
exit "$failed"

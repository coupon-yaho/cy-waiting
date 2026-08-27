// 게이트웨이 오버헤드 (G6.11). **게이트웨이만의 몫을 잰다.**
//
// 전체 응답 시간에는 뒷단 처리도, k6 자신의 비용도, 루프백 왕복도 섞여 있다.
// 그 값으로 판정하면 뒷단을 느리게 만드는 것만으로 게이트웨이가 느려 보인다.
//
// 그래서 **같은 판에서 세 갈래를 동시에 돌린다.**
//
//   gateway  게이트웨이를 지나 뒷단까지 — 뒷단이 실어 준 자기 시간을 뺀다
//   direct   스텁을 직접 — 같은 빼기를 한다. 남는 것이 하네스의 바닥값이다
//   soldout  매진 단락 — 뒷단을 아예 안 거치므로 뺄 것이 없다 (R3)
//
// 판정은 `gateway - direct` 다. 스텁이 응답을 쓰는 시간은 헤더가 이미 나간
// 뒤라 자기 시계에 안 잡히는데, 그 미측정 구간이 양쪽에 똑같이 들어 있어
// 빼면 사라진다. 따로 돌려 빼는 것과 달리 부하 조건도 어긋나지 않는다.
import http from 'k6/http';
import { check, fail } from 'k6';
import { Counter, Trend } from 'k6/metrics';

http.setResponseCallback(http.expectedStatuses(200, 202, 409));

const BASE = __ENV.BASE_URL || 'http://localhost:18080';
const STUB = __ENV.STUB_URL || 'http://localhost:18090';

const IDLE = '/api/v1/coupons/c1/issue';
const SOLD = '/api/v1/coupons/c3/issue';

// 한산 통과 상한(크레딧의 70%)의 절반쯤. 상한에 붙여 놓으면 창 경계가 한 번
// 흔들릴 때 큐가 켜지고, 그 뒤로는 사다리 8번이 판을 통째로 가져간다.
const RATE = 100;
// **갓 뜬 JVM 을 재면 게이트웨이가 아니라 예열을 잰다.** 같은 콜드 JVM 에서
// 12초를 넣었을 때 오버헤드 p99 가 9.82ms, 60초를 넣었을 때 5.44ms 였다.
// 그 위로는 더 안 내려간다 — 150초를 넣어도 5.58 이었다. 남는 것은 JIT 가
// 아니라 판 자체의 잡음이고, 그 잡음은 대조군 상한이 잡는다.
//
// 러너마다 다를 수 있어 밖에서 덮을 수 있게 둔다.
const WARMUP = __ENV.WARMUP || '60s';
const MEASURE = '90s';

// 매진 단락은 뒷단을 안 거치니 스텁에 부하를 안 얹는다. 두 갈래의 차이를
// 흔들지 않을 만큼만 넣고, 90초에 1,800 표본이면 p99 를 보기에 족하다.
const SOLDOUT_RATE = 20;

const arm = (exec, rate) => ({
  executor: 'constant-arrival-rate',
  rate, timeUnit: '1s', duration: MEASURE, startTime: WARMUP,
  preAllocatedVUs: 40, maxVUs: 200, exec,
});

export const options = {
  scenarios: {
    warmup: {
      executor: 'constant-arrival-rate',
      rate: RATE, timeUnit: '1s', duration: WARMUP,
      preAllocatedVUs: 40, maxVUs: 200, exec: 'warm',
    },
    gateway: arm('viaGateway', RATE),
    direct: arm('viaStub', RATE),
    soldout: arm('soldOut', SOLDOUT_RATE),
  },
  thresholds: {
    checks: ['rate>0.99'],
    // **여기서 판정하지 않는다.** 게이트 기준은 두 갈래의 차이라 임계로
    // 못 쓴다 (6.6.6). 이 둘은 판이 통째로 어긋났을 때 일찍 끊는 용도다.
    gateway_own_ms: ['p(99)<50'],
    soldout_ms: ['p(99)<50'],
    overhead_unmeasured: ['count==0'],
    overhead_clamped: ['count==0'],
    // **갈래가 아예 안 돈 경우를 잡는다.** exec 이름이 어긋나면 검사도 안
    // 찍히므로 통과율로는 안 걸리고, 요청 수 하한은 나머지 셋이 채운다.
    soldout_measured: ['count>0'],
    gateway_measured: ['count>0'],
    direct_measured: ['count>0'],
  },
  // **요약에 p99 를 실어야 게이트가 읽는다.** 기본은 p95 까지라, 없으면
  // 판정이 "숫자가 아니다" 로 떨어진다 — 실제로 그렇게 한 번 떨어뜨렸다.
  summaryTrendStats: ['min', 'med', 'avg', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

/** 게이트웨이를 지난 왕복에서 뒷단 몫을 뺀 값. */
const viaGw = new Trend('gateway_own_ms');

/** 스텁을 직접 친 왕복에서 같은 빼기를 한 값. 하네스의 바닥이다. */
const viaDirect = new Trend('harness_baseline_ms');

/** 매진 단락. 뒷단을 안 거치므로 전부 게이트웨이 몫이다. */
const soldOutMs = new Trend('soldout_ms');

/** 뒷단이 자기 시간을 안 실어 준 응답. 있으면 그만큼 표본이 빠진 것이다. */
const missing = new Counter('overhead_unmeasured');

/** 뺀 값이 음수였던 응답. 단위나 시계가 어긋났다는 뜻이다. */
const clamped = new Counter('overhead_clamped');

/**
 * 갈래마다 따로 센다. **없으면 표본 0 건인 판이 통과한다** — 임계가 달린 Trend 는
 * 표본이 없어도 요약에서 안 사라지고 모든 통계가 0 으로 남고, 0 은 상한을 안
 * 넘으므로 "아무것도 안 쟀다" 가 "아주 빨랐다" 로 읽힌다.
 *
 * <p>하나로 합치면 하한이 <b>산수 우연</b>에 기댄다 — 한쪽이 통째로 죽어도
 * 나머지 한쪽이 하한을 채울 수 있고, 도착률이나 판 길이를 건드리는 순간 그
 * 우연이 깨진다.
 */
const gwMeasured = new Counter('gateway_measured');

const directMeasured = new Counter('direct_measured');

/** 매진 갈래는 뺄 값이 없어 {@code record} 를 안 거친다. */
const soldOutMeasured = new Counter('soldout_measured');

// **`>>` 를 안 쓴다.** JS 의 비트 시프트는 32비트라 VU 가 269 를 넘으면 음수
// 옥텟이 나오고, 그 주소는 신뢰 목록에 못 들어가 429 가 된다. 판이 느려져 VU 가
// 늘 때 — 측정이 가장 필요한 순간에 — 터지고, 실패는 통과율로만 보여 원인을
// 안 가리킨다.
//
// 지금 도착률(게이트웨이 100 + 매진 20)은 남용 상한 200/초 아래라 한 주소로
// 몰려도 안 걸린다. 그래도 펴 두는 것은 도착률을 올릴 때 여기가 먼저 막히기
// 때문이다 — 그때 증상은 오버헤드가 아니라 통과율로 나타난다.
const spread = (n) => `10.14.${Math.floor(n / 256) % 250}.${n % 250}`;

const headers = (n) => ({
  'X-Member-Id': String(n),
  'X-Member-Grade': 'GOLD',
  'X-Forwarded-For': spread(n * 8 + (__VU % 8)),
});

/** 한 요청분의 식별자. VU 가 몇 개든 겹치지 않게 편다. */
const nth = () => __VU * 1000003 + __ITER;

/** 뒷단이 실어 준 자기 시간을 빼서 담는다. 못 빼면 안 센다. */
function record(trend, counter, r) {
  const spent = Number(r.headers['X-Stub-Service-Ms']);
  if (!Number.isFinite(spent)) {
    // **뺄 값이 없으면 안 센다.** 전체 시간을 그대로 넣으면 뒷단 몫이 우리
    // 오버헤드로 보이고, 그 판정은 틀린 값이다.
    missing.add(1);
    return;
  }
  const own = r.timings.duration - spent;
  if (own < 0) {
    // 클램프해서 0 으로 넣으면 빼기가 통째로 뒤집혀도 p99 가 0 으로 초록이다.
    clamped.add(1);
    return;
  }
  trend.add(own);
  counter.add(1);
}

function served(r, path) {
  try {
    return r.status === 200 && r.json().data.path === path;
  } catch (e) {
    return false;
  }
}

/**
 * **게이트웨이가 판에 있는지 먼저 못 박는다.**
 *
 * 스텁은 어떤 URL 에도 `success:true` 를 돌려준다. 그래서 통과 경로의 응답
 * 만으로는 게이트웨이를 지났다는 증거가 한 줄도 없다 — 라우트 프리픽스가
 * 틀려도, 판정 필터가 통째로 안 붙어도 이 판은 초록이 된다.
 *
 * 매진 봉투는 게이트웨이만 만든다. 뒷단도 Redis 도 안 거치는 종결이라 (R3)
 * 이것이 오면 판정 필터까지 붙어 있다는 뜻이다.
 */
export function setup() {
  const r = http.post(`${BASE}${SOLD}`, null, { headers: headers(1) });
  let code = null;
  try {
    code = r.json().error.code;
  } catch (e) {
    code = null;
  }
  if (r.status !== 409 || code !== 'COUPON-306') {
    fail(`게이트웨이가 판에 없다 — 매진 봉투 대신 ${r.status} ${code} 가 왔다`);
  }
  const direct = http.get(`${STUB}/stub/health`);
  if (direct.status !== 200) {
    fail(`대조군이 스텁에 못 닿는다 — ${direct.status}. 차이로 판정할 수 없다`);
  }
}

export function warm() {
  http.post(`${BASE}${IDLE}`, null, { headers: headers(nth()) });
}

export function viaGateway() {
  const r = http.post(`${BASE}${IDLE}`, null, { headers: headers(nth()) });
  const ok = served(r, IDLE);
  // 줄을 선 응답은 뒷단을 안 거친다. 그것까지 담으면 다른 경로를 재게 된다.
  if (ok) {
    record(viaGw, gwMeasured, r);
  }
  check(r, { '한산한 쿠폰이 뒷단까지 간다': () => ok });
}

export function viaStub() {
  const r = http.post(`${STUB}${IDLE}`, null, { headers: headers(nth()) });
  const ok = served(r, IDLE);
  if (ok) {
    record(viaDirect, directMeasured, r);
  }
  check(r, { '대조군이 스텁까지 간다': () => ok });
}

export function soldOut() {
  const r = http.post(`${BASE}${SOLD}`, null, { headers: headers(nth()) });
  let ok = false;
  try {
    ok = r.status === 409 && r.json().error.code === 'COUPON-306';
  } catch (e) {
    ok = false;
  }
  if (ok) {
    soldOutMs.add(r.timings.duration);
    soldOutMeasured.add(1);
  }
  check(r, { '매진은 게이트웨이가 종결한다': () => ok });
}

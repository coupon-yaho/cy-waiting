// 오픈 순간 스파이크 (6.6.4). **200ms 램프가 이 시나리오의 전부다.**
//
// 1초 균등으로 때리면 선착순 오픈의 실제 스파이크를 다섯 배 과소평가한다.
// Phase 10 의 착수 판정이 이 값에 달려 있다.
import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';

// 202 는 줄에 섰다는 뜻이고 429·503 은 그 위의 상한이다. 셋 다 판정이 돈 증거라
// 실패로 안 센다 — 그대로 두면 실패율이 진짜 실패에 아무 신호도 못 준다.
http.setResponseCallback(http.expectedStatuses(200, 202, 429, 503));

const BASE = __ENV.BASE_URL || 'http://localhost:18080';

// **200ms 안에 다 던진다.** `target` 은 초당 도착률이라, 200ms 램프 뒤에 그 비율을
// 유지하면 버스트가 아니라 고원을 재게 된다 — 처음에 그렇게 짰다.
//
// 정해진 인원이 각자 한 번씩 보내게 두고, 그 도착이 실제로 얼마나 짧은 창에
// 몰렸는지를 게이트가 잰다. 창이 벌어지면 그 회차는 스파이크를 안 만든 것이다.
//
// **한 러너로는 200ms 를 못 만든다.** 2만 VU 를 띄우는 데만 몇 초가 걸려 실측
// 도착률이 4,000/초 언저리다 — 200ms 라면 100,000/초여야 한다. 진짜 그 값은
// 생성기를 여러 대로 나눠야 하고, 그건 Phase 10 의 일이다.
const SPIKE_USERS = Number(__ENV.SPIKE_USERS || '20000');

// **쿠폰을 받는다.** 예열은 재려는 쿠폰이 아니라 다른 쿠폰으로 돌려야 한다 — 같은
// 쿠폰으로 데우면 크레딧이 올라간 채 본 회차가 시작해 전원이 통과하고, 줄에 선 것이
// 0 이 된다. JIT 는 JVM 몫이라 어느 쿠폰으로 데워도 같이 데워진다.
const COUPON = __ENV.COUPON || 'c2';

export const options = {
  // 게이트가 분위수를 읽는다. **p99 를 넣는다** — 샤딩 착수 판정이 기록으로
  // 남기는 값이고, 안 넣으면 요약에 없어 늘 "없음" 이 찍힌다.
  summaryTrendStats: ['min', 'p(10)', 'p(25)', 'med', 'p(75)', 'p(90)', 'p(99)', 'max'],
  scenarios: {
    spike: {
      executor: 'per-vu-iterations',
      vus: SPIKE_USERS,
      iterations: 1,
      maxDuration: '60s',
    },
  },
  thresholds: {
    // 판정이 안 낸 응답이 섞이면 배선이 어긋난 것이다.
    http_req_failed: ['rate<0.01'],
    // **다 던져야 한다.** 한 명이라도 못 보내면 스파이크가 그만큼 작아진다.
    http_reqs: [`count>=${SPIKE_USERS}`],
    // **줄이 서야 한다.** 안 서면 스파이크가 유휴 몫을 못 넘긴 것이고,
    // 그때는 이 시나리오가 스파이크를 안 만든 것이다.
    queued_responses: ['count>0'],
  },
};

const queuedResponses = new Counter('queued_responses');
const shedResponses = new Counter('shed_responses');
// **통과한 것도 센다.** 등록 타이머는 성공한 등록만 재므로, 쿠폰이 IDLE 인 동안 그냥
// 지나간 요청은 그 평균에 안 들어간다. 세 갈래를 다 세야 어느 모집단을 잰 회차인지
// 갈린다 — 안 세면 같은 하네스의 두 평균이 다른 것을 재고도 한 표에 들어간다.
const passedResponses = new Counter('passed_responses');
// **폭만 보면 부족하다.** 만 건 중 9,999 건이 한 값이고 하나만 멀리 있어도 폭은
// 넓다. 그 회차는 회복이 곧 두 번째 스파이크가 되는데 게이트는 초록이다.
//
// 분위수 둘로도 부족하다. 절반이 24초, 40%가 30초, 나머지가 36초면 최소·중앙·p90
// 이 서로 다르지만 **89%가 두 덩어리로 돌아온다.** 사분위를 촘촘히 놓고 이웃한
// 값끼리 붙어 있는 자리가 있는지 본다 — 덩어리는 반드시 어딘가를 붙여 놓는다.
const retryAfterSeconds = new Trend('retry_after_seconds');

const headers = (member) => ({
  'X-Member-Id': String(member),
  'X-Member-Grade': 'GOLD',
  // VU 마다 다른 주소. 한 사람처럼 보이면 남용 방지가 막고, 그 실패가 코드
  // 결함인지 하네스 모양인지 안 갈린다.
  'X-Forwarded-For': `10.10.${__VU % 250}.${(__VU % 250) + 1}`,
});

// 줄에 섰다. **봉투까지 본다** — 202 만 보면 뒷단이 낸 202 도 통과한다.
const queued = (r) => {
  if (r.status !== 202) {
    return false;
  }
  try {
    const data = r.json().data;
    return data.admitted === false && typeof data.position === 'number';
  } catch (e) {
    return false;
  }
};

export default function () {
  const member = __VU * 100000 + __ITER;
  const issue = http.post(`${BASE}/api/v1/coupons/${COUPON}/issue`, null, {
    headers: headers(member),
  });

  if (queued(issue)) {
    queuedResponses.add(1);
  } else if (issue.status === 200) {
    passedResponses.add(1);
  } else if (issue.status === 429 || issue.status === 503) {
    shedResponses.add(1);
    // **다시 올 시각이 흩어져야 한다** (F7). 한 값으로 몰리면 그 초에 같은
    // 스파이크가 다시 온다 — 회복이 곧 두 번째 사고가 된다.
    const after = Number(issue.headers['Retry-After']);
    if (Number.isFinite(after)) {
      retryAfterSeconds.add(after);
    }
  }

  check(issue, {
    '판정이 낸 응답이다': (r) => [200, 202, 429, 503].includes(r.status),
  });
}

// R1 의 대조군. **경합 쿠폰은 줄로 가야 한다.**
//
// `idle-coupon.js` 하나만으로는 아무것도 증명하지 못한다 — 대기열을 통째로
// 끄면 그쪽이 통과한다. 같은 하네스에서 줄이 실제로 서는 것까지 봐야
// "한산할 때만 안 선다" 가 성립한다 (6.7.2).
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

// 202 는 줄에 섰다는 뜻이고, 429 는 그 위에 상한이 걸린 것이다. 둘 다 판정이
// 돈 증거라 실패로 세지 않는다.
http.setResponseCallback(http.expectedStatuses(200, 202, 429, 503));

const BASE = __ENV.BASE_URL || 'http://localhost:18080';

// **몰아친다.** 유휴 몫을 확실히 넘겨야 줄이 선다. 넘기지 못하면 이 시나리오는
// idle 과 같은 것을 재고, 대조군이 아니게 된다.
export const options = {
  scenarios: {
    contended: {
      executor: 'constant-arrival-rate',
      rate: 400,
      timeUnit: '1s',
      duration: '20s',
      preAllocatedVUs: 200,
      maxVUs: 400,
    },
  },
  thresholds: {
    // **줄이 서야 한다.** 안 서면 유휴 몫을 못 넘긴 것이거나 대기열이 꺼진
    // 것이고, 어느 쪽이든 대조군 구실을 못 한다.
    queued_responses: ['count>0'],
  },
};

const queuedResponses = new Counter('queued_responses');
const passedResponses = new Counter('passed_responses');

const memberHeaders = (id) => ({
  'X-Member-Id': String(id),
  'X-Member-Grade': 'GOLD',
  'X-Forwarded-For': `10.3.${__VU % 250}.1`,
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
  // **사람마다 다른 식별자.** 같은 사람이 반복하면 이미 줄에 선 것으로 읽혀
  // 순번을 그대로 돌려받고, 줄이 자라지 않는다.
  const member = __VU * 1000000 + __ITER;
  const path = '/api/v1/coupons/c2/issue';
  const issue = http.post(`${BASE}${path}`, null, { headers: memberHeaders(member) });

  if (queued(issue)) {
    queuedResponses.add(1);
  } else if (issue.status === 200) {
    passedResponses.add(1);
  }

  check(issue, {
    // 판정이 낸 응답 중 하나여야 한다. 그 밖이면 배선이 어긋난 것이다.
    '판정이 낸 응답이다': (r) => [200, 202, 429, 503].includes(r.status),
  });
}

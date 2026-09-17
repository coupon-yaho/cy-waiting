// 앞단 LB 기준선 회차.
//
// **최대치 회차와 같은 모양으로 보낸다.** 경로·메서드·헤더를 같은 몫으로 섞어야 뺀 값이 게이트웨이 몫이 된다.
// 뒤에는 더미 응답뿐이라 여기서 나는 천장은 LB 나 생성기의 것이다.
import http from 'k6/http';
import { check } from 'k6';

const BASE = __ENV.BASE_URL || 'http://localhost:18070';
const RATE = Number(__ENV.RATE || '8000');
const DURATION = __ENV.DURATION || '30s';
const POLL_SHARE = Number(__ENV.POLL_SHARE || '0.10');
const ENTER_SHARE = Number(__ENV.ENTER_SHARE || '0.30');

export const options = {
  summaryTrendStats: ['min', 'med', 'p(90)', 'p(99)', 'max'],
  scenarios: {
    baseline: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: Number(__ENV.VUS || '2000'),
      maxVUs: Number(__ENV.MAX_VUS || '20000'),
    },
  },
  thresholds: {
    // 못 만든 부하로 기준선을 적지 않는다.
    dropped_iterations: ['count==0'],
    http_req_failed: ['rate<0.01'],
    checks: ['rate>0.99'],
  },
};

const headers = (member) => ({
  'X-Member-Id': String(member),
  'X-Member-Grade': 'GOLD',
  'X-Forwarded-For': `10.${(__VU % 200) + 20}.${__VU % 250}.${(__ITER % 250) + 1}`,
});

export default function () {
  const member = __VU * 1000000 + __ITER;
  const dice = Math.random();
  let r;
  if (dice < POLL_SHARE) {
    r = http.get(`${BASE}/api/v1/coupons/c2/queue`,
        { headers: Object.assign(headers(member), { 'Queue-Token': 'baseline' }) });
  } else if (dice < POLL_SHARE + ENTER_SHARE) {
    r = http.post(`${BASE}/api/v1/coupons/c2/issue`, null, { headers: headers(member) });
  } else {
    r = http.post(`${BASE}/api/v1/coupons/c1/issue`, null, { headers: headers(member) });
  }
  check(r, { 'LB 가 응답한다': (res) => res.status === 200 });
}

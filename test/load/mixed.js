// 혼합 트래픽 (6.6.5). **핫 쿠폰 하나가 콜드 쿠폰의 통로를 막으면 안 된다.**
//
// R1 은 "부하 없는 쿠폰은 즉시 통과" 다. 그 성질은 한산한 쿠폰만 때려서는 안
// 보인다 — 옆에서 다른 쿠폰이 몰리고 있을 때에도 지켜지는가가 본론이다.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

http.setResponseCallback(http.expectedStatuses(200, 202, 429, 503));

const BASE = __ENV.BASE_URL || 'http://localhost:18080';

// 핫 하나에 몰아치고, 콜드 하나를 저부하로 같이 때린다. 두 시나리오가 같은
// 게이트웨이를 지나므로 격리가 실제로 서는지 한 회차에서 보인다.
export const options = {
  scenarios: {
    hot: {
      executor: 'constant-arrival-rate',
      rate: 800, timeUnit: '1s', duration: '20s',
      preAllocatedVUs: 300, maxVUs: 600,
      exec: 'hot',
    },
    cold: {
      executor: 'constant-arrival-rate',
      rate: 5, timeUnit: '1s', duration: '20s',
      preAllocatedVUs: 10,
      exec: 'cold',
      startTime: '2s',
    },
  },
  thresholds: {
    // **콜드가 한 번이라도 줄을 서면 격리가 깨진 것이다.**
    cold_queued: ['count==0'],
    // 핫은 줄이 서야 한다. 안 서면 이 실행이 혼합이 아니다.
    hot_queued: ['count>0'],
  },
};

const hotQueued = new Counter('hot_queued');
const coldQueued = new Counter('cold_queued');
const coldPassed = new Counter('cold_passed');

const headers = (member, lane) => ({
  'X-Member-Id': String(member),
  'X-Member-Grade': 'GOLD',
  'X-Forwarded-For': `10.11.${lane}.${(__VU % 250) + 1}`,
});

const servedByBackend = (r, path) => {
  if (r.status !== 200) {
    return false;
  }
  try {
    return r.json().data.path === path;
  } catch (e) {
    return false;
  }
};

export function hot() {
  const path = '/api/v1/coupons/c2/issue';
  const r = http.post(`${BASE}${path}`, null,
      { headers: headers(__VU * 1000000 + __ITER, 1) });
  if (r.status === 202) {
    hotQueued.add(1);
  }
  check(r, { '핫이 판정을 지난다': (x) => [200, 202, 429, 503].includes(x.status) });
}

export function cold() {
  const path = '/api/v1/coupons/c1/issue';
  const r = http.post(`${BASE}${path}`, null,
      { headers: headers(__VU * 2000000 + __ITER, 2) });
  if (r.status === 202) {
    coldQueued.add(1);
  }
  const passed = servedByBackend(r, path);
  if (passed) {
    coldPassed.add(1);
  }
  // **옆이 몰려도 콜드는 그대로 지나가야 한다.** 이게 R1 의 본론이다.
  check(r, { '콜드가 줄 없이 지나간다': () => passed });
}

// 한 수명 안에 몰아넣는다 (G6.16). **"동시 1만 → 뒷단 1건" 은 창 하나 안의 이야기다.**
//
// `read-burst.js` 는 초당 2,000 을 10초 보내므로 수명(300ms) 창이 서른 번 넘게
// 지나간다 — 그 값은 지속 부하에서의 병합 배수이지 이 게이트가 말하는 값이 아니다.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

http.setResponseCallback(http.expectedStatuses(200));

const BASE = __ENV.BASE_URL || 'http://localhost:18080';

// **한 번에 다 던진다.** 1만 VU 가 각자 한 번씩만 보내므로, 전부 수명 하나 안에
// 도착한다. 창이 넘어가면 이 시나리오는 재려던 것을 안 재는 것이다.
export const options = {
  scenarios: {
    window: {
      executor: 'per-vu-iterations',
      vus: 10000,
      iterations: 1,
      maxDuration: '30s',
    },
  },
  thresholds: {
    checks: ['rate>0.99'],
  },
};

const servedResponses = new Counter('served_responses');

const headers = () => ({
  'X-Member-Id': String(__VU),
  'X-Member-Grade': 'GOLD',
  'X-Forwarded-For': `10.8.${__VU % 250}.1`,
  Cookie: `_ga=GA1.1.${__VU}; SESSION=${__VU}`,
});

export default function () {
  const path = '/api/v1/coupons';
  const list = http.get(`${BASE}${path}`, { headers: headers() });

  if (list.status === 200) {
    servedResponses.add(1);
  }
  check(list, {
    '조회가 온전한 응답을 받는다': (r) => {
      if (r.status !== 200) {
        return false;
      }
      try {
        return r.json().data.path === path;
      } catch (e) {
        return false;
      }
    },
  });
}

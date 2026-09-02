// 계약이 안 선 상태를 잰다 (6.10.5).
//
// 뒷단이 공유해도 된다고 말하지 않으면 게이트웨이는 안 모은다. 그때 뒷단 도달이
// **요청 수와 같아야** 한다. 이 검사가 없으면 "발급 계층이 붙이기 전까지 어떻게
// 되는가" 를 아무도 안 보고, 스텁이 늘 붙이는 다른 갈래만 초록으로 남는다.
//
// 스텁은 `SHARED_HEADER=false` 로 띄운다.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

http.setResponseCallback(http.expectedStatuses(200));

const BASE = __ENV.BASE_URL || 'http://localhost:18080';
const PATH = '/api/v1/coupons';

export const options = {
  scenarios: {
    burst: {
      executor: 'constant-arrival-rate',
      rate: 200, timeUnit: '1s', duration: '15s',
      preAllocatedVUs: 60, maxVUs: 300,
    },
  },
  thresholds: { checks: ['rate>0.99'] },
};

/** 응답을 받은 건수. 뒷단 도달과 대 보는 값이다. */
const served = new Counter('served_responses');

export default function () {
  const r = http.get(`${BASE}${PATH}`, {
    headers: {
      'X-Member-Id': String(__VU * 1000003 + __ITER),
      'X-Member-Grade': 'GOLD',
      'X-Forwarded-For': `10.21.${Math.floor(__ITER / 256) % 250}.${__ITER % 250}`,
    },
  });
  const ok = r.status === 200;
  if (ok) {
    served.add(1);
  }
  check(r, { '조회가 200 으로 온다': () => ok });
}

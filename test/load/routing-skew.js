// 게이트웨이 여러 대의 쏠림을 재는 **고정 유입 생성기** (9.4.5 · R-4).
//
// 한 대짜리 하네스로는 이 성질을 원리적으로 못 잰다 — 누적이 하나뿐이라
// 어긋날 상대가 없다. 여기서는 같은 유입을 게이트웨이 여러 대에 고르게
// 나눠 넣어, 각자 제 누적을 들고 같은 여유를 보는 상태를 만든다.
import http from 'k6/http';
import { Counter } from 'k6/metrics';

http.setResponseCallback(http.expectedStatuses(200, 202, 429, 503));

// 쉼표로 구분한 게이트웨이 주소들. 부르는 쪽이 실제로 열린 포트를 찾아 넘긴다 —
// 포트를 범위로 열면 어느 컨테이너가 어느 포트를 받는지 순서가 안 정해진다.
const BASES = (__ENV.BASE_URLS || 'http://localhost:18080').split(',');
const COUPON = __ENV.COUPON || 'c1';
const RATE = Number(__ENV.RATE || 66);
const DURATION = __ENV.DURATION || '40s';
const VUS = Number(__ENV.VUS || 160);

export const options = {
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate',
      rate: RATE, timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: VUS, maxVUs: VUS,
    },
  },
  thresholds: { dropped_iterations: ['count==0'] },
};

const passed = new Counter('issue_200');
const queued = new Counter('issue_202');
const other = new Counter('issue_other');
// 게이트웨이별 통과. **한쪽으로 기울면 쏠림이 아니라 생성기를 잰 것이다.**
const perGateway = new Counter('issue_by_gateway');

export default function () {
  // **VU 와 회차를 함께 센다.** 회차만 세면 모든 VU 의 첫 회차가 같은 값(0)이라
  // 시작 순간 전체가 한 게이트웨이로 간다 — 그 한 초가 노드 상한을 넘기고, 줄이
  // 한 번 켜지면 추월 금지 때문에 그 뒤로 전부 줄로 간다. 실제로 첫 회차가
  // 2641 건 중 2596 건을 줄로 보냈다.
  //
  // VU 로만 가르는 것도 안 된다. VU 가 게이트웨이에 고정되면 한쪽 응답이 느려질
  // 때 그쪽 유입만 줄어, 고르개가 아니라 생성기가 부하를 재분배한 것이 된다.
  const base = BASES[(__VU + __ITER) % BASES.length];
  const member = 7_000_000 + __VU * 100_000 + __ITER;
  const res = http.post(`${base}/api/v1/coupons/${COUPON}/issue`, null, {
    headers: {
      'X-Member-Id': String(member),
      'X-Member-Grade': 'GOLD',
      'X-Forwarded-For': `10.${(__VU % 200) + 20}.${(__ITER % 250) + 1}.${(__VU % 250) + 1}`,
    },
  });
  if (res.status === 200) {
    passed.add(1);
    perGateway.add(1, { gateway: String(__ITER % BASES.length) });
  } else if (res.status === 202) queued.add(1);
  else other.add(1);
}

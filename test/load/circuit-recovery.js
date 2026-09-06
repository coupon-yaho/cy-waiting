// 서킷 진입·유지·회복 구간의 **고정 유입 생성기.**
//
// 회차 내내 같은 유입을 게이트웨이 여러 대에 고르게 넣는다. 뒷단이 고장 난
// 구간에는 대부분이 끊기거나 줄로 가는데, **그게 정상이다** — 여기서 재는 것은
// 응답이 아니라 그 구간에 배분이 얼마나 나갔는가다. 판정은 표본이 낸다.
import http from 'k6/http';
import { Counter } from 'k6/metrics';

// 끊는 것도 줄 세우는 것도 판정이 낸 정상 동작이다 (O-7). 뒷단 고장 구간의
// 503 을 실패로 세면 이 시나리오가 자기 자극 때문에 빨개진다.
http.setResponseCallback(http.expectedStatuses(200, 202, 429, 503));

// 쉼표로 구분한 게이트웨이 주소들. 부르는 쪽이 실제로 열린 포트를 찾아 넘긴다 —
// 포트를 범위로 열면 어느 컨테이너가 어느 포트를 받는지 순서가 안 정해진다.
const BASES = (__ENV.BASE_URLS || 'http://localhost:18080').split(',');
const COUPON = __ENV.COUPON || 'c1';
const RATE = Number(__ENV.RATE || 200);
const DURATION = __ENV.DURATION || '75s';
const VUS = Number(__ENV.VUS || 300);

export const options = {
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate',
      rate: RATE, timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: VUS, maxVUs: VUS,
    },
  },
  // **못 만든 부하로 판정하지 않는다.** 흘린 회차가 있으면 기준선 유입이 목표와
  // 다르고, 회복 봉우리를 그 기준선에 견주는 판정이 통째로 어긋난다.
  thresholds: { dropped_iterations: ['count==0'] },
};

const passed = new Counter('circuit_200');
const queued = new Counter('circuit_202');
const shed = new Counter('circuit_shed');
// **판정 밖 응답만 어긋남이다.** 끊긴 것과 섞으면 고장 구간에서 "판정이 끊었다"
// 와 "배선이 틀렸다" 가 한 수가 된다.
const offJudgement = new Counter('circuit_off_judgement');

export default function () {
  // **VU 와 회차를 함께 센다.** 회차만 세면 모든 VU 의 첫 회차가 같은 값이라
  // 시작 순간 전체가 한 게이트웨이로 간다. VU 로만 가르면 VU 가 게이트웨이에
  // 고정돼, 한쪽이 느려질 때 그쪽 유입만 줄어든다 — 생성기가 부하를 재분배한다.
  const gw = (__VU + __ITER) % BASES.length;
  const member = 9_000_000 + __VU * 100_000 + __ITER;
  const res = http.post(`${BASES[gw]}/api/v1/coupons/${COUPON}/issue`, null, {
    headers: {
      'X-Member-Id': String(member),
      'X-Member-Grade': 'GOLD',
      // 한 주소로 몰면 주소별 한도에 걸려 그 요청이 뒷단에 안 닿는다.
      'X-Forwarded-For': `10.${(__VU % 200) + 20}.${(__ITER % 250) + 1}.${(__VU % 250) + 1}`,
    },
  });
  if (res.status === 200) {
    passed.add(1);
  } else if (res.status === 202) {
    queued.add(1);
  } else if (res.status === 429 || res.status === 503) {
    shed.add(1);
  } else {
    offJudgement.add(1);
  }
}

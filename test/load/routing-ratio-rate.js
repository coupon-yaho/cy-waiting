// 유입 비율 실측의 **고정 유입 생성기** (9.4.5 · P2C).
//
// 일꾼이 한 건씩 보내는 하네스는 유입을 초당 몇 건 폭 안에 못 고정한다 —
// 출발을 흩어도 응답이 돌아오는 시점이 흔들려 초마다 몇 건씩 넘치고, 넘친
// 순간 줄 모드가 켜져 그 회차는 분배가 아니라 줄을 잰다. P2C 가 여유를 보는
// 깊이는 그 폭 안에서만 만들어진다. 초당 도착을 고정하는 실행기가 답이다.
import http from 'k6/http';
import { Counter } from 'k6/metrics';

// 202·503 도 "예상" 으로 둔다 — k6 가 실패로 세어 요약을 오염시키지 않도록.
// 무엇이 몇 건인지는 아래 카운터가 따로 센다.
http.setResponseCallback(http.expectedStatuses(200, 202, 429, 503));

const BASE = __ENV.BASE_URL || 'http://localhost:18080';
const COUPON = __ENV.COUPON || 'c1';
const RATE = Number(__ENV.RATE || 23);
const DURATION = __ENV.DURATION || '40s';
// 물린 건수의 상한. 유입 × 지연보다 넉넉히 둔다 — 모자라면 k6 가 도착을
// 떨어뜨려 고정 유입이 아니게 된다. 기본 조건(66/s · 1.5초)이 99 를 물리므로
// 100 은 모자란다. 이 VU 는 답을 기다리며 서 있는 것이라 부하 시험의 VU 상한
// 100 이 재려던 "생성기 자체의 부하" 와는 다른 수다.
const VUS = Number(__ENV.VUS || 160);

export const options = {
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate',
      rate: RATE, timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: VUS, maxVUs: VUS,
    },
  },
  // **도착을 떨어뜨렸으면 그 회차는 고정 유입이 아니다.** 판정 불가로 끝낸다.
  thresholds: { dropped_iterations: ['count==0'] },
};

// **코드별로 카운터를 따로 둔다.** 태그로 가르면 요약 파일에는 합만 남는다 —
// 요약은 태그를 접어 내보내므로, 판정기가 200 과 202 를 가를 수 없다.
const passed = new Counter('issue_200');
const queued = new Counter('issue_202');
const other = new Counter('issue_other');

export default function () {
  // 회원과 주소를 흩는다. 한 주소로 몰면 주소별 한도에 걸려 429 가 나고, 그
  // 요청은 뒷단에 안 닿아 표본에서 빠진다.
  const member = 7_000_000 + __VU * 100_000 + __ITER;
  const res = http.post(`${BASE}/api/v1/coupons/${COUPON}/issue`, null, {
    headers: {
      'X-Member-Id': String(member),
      'X-Member-Grade': 'GOLD',
      'X-Forwarded-For': `10.${(__VU % 200) + 20}.${(__ITER % 250) + 1}.${(__VU % 250) + 1}`,
    },
  });
  if (res.status === 200) passed.add(1);
  else if (res.status === 202) queued.add(1);
  else other.add(1);
}

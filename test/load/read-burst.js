// 조회 폭주 (6.6.6). **코얼레싱은 조회를 때려야만 검증된다.**
//
// 기존 시나리오는 전부 발급 경로다. 조회를 안 때리면 6.10 이 만들어져도 효과를
// 재지 못하고, **재지 못하는 보호 장치는 없는 것과 같다.**
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

http.setResponseCallback(http.expectedStatuses(200));

const BASE = __ENV.BASE_URL || 'http://localhost:18080';

// 같은 쿠폰 목록을 동시에 두드린다. 키가 하나여야 모이는 것을 잰다.
export const options = {
  scenarios: {
    burst: {
      executor: 'constant-arrival-rate',
      rate: 2000,
      timeUnit: '1s',
      duration: '10s',
      preAllocatedVUs: 500,
      maxVUs: 1000,
    },
  },
  thresholds: {
    checks: ['rate>0.99'],
  },
};

const servedResponses = new Counter('served_responses');

// **회원 헤더를 붙인다.** 신원 필터가 모든 조회에 요구하는 값이라, 안 붙이면
// 400 으로 끊겨 뒷단에 하나도 안 간다 — 그 상태로 "뒷단 도달 0" 을 보면 모으기가
// 완벽히 도는 것처럼 보인다. 실제로 그렇게 잰 적이 있다.
//
// VU 마다 다른 회원이다. 같은 값을 쓰면 남용 방지의 회원 상한에 걸린다.
const headers = () => ({
  'X-Member-Id': String(__VU * 1000 + __ITER),
  'X-Member-Grade': 'GOLD',
  'X-Forwarded-For': `10.4.${__VU % 250}.1`,
  // **브라우저처럼 쿠키를 보낸다.** 안 보내면 하네스가 재는 것이 실제 조회가
  // 아니다 — 쿠키가 있다고 안 모으는 규칙이 들어와도 이 시험이 안 빨개진다.
  Cookie: `_ga=GA1.1.${__VU}; SESSION=${__VU}`,
});

export default function () {
  const path = '/api/v1/coupons';
  const list = http.get(`${BASE}${path}`, { headers: headers() });

  const ok = list.status === 200;
  if (ok) {
    servedResponses.add(1);
  }
  check(list, {
    // 모으든 안 모으든 사용자는 온전한 응답을 받아야 한다. 뒷단 도달 수를
    // 줄이면서 응답이 깨지면 그건 보호가 아니라 사고다.
    '조회가 온전한 응답을 받는다': (r) => {
      if (!ok) {
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

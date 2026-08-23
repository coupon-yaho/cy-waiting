// 스모크. **부하를 재는 것이 아니라 하네스가 도는지를 본다.**
//
// 여기서 무거운 것을 돌리면 PR 마다 몇 분씩 걸려 아무도 PR 을 안 연다.
// 규모가 필요한 것은 main·nightly 의 다른 시나리오가 맡는다.
import http from 'k6/http';
import { check } from 'k6';

// **의도한 400 을 실패로 세지 않는다.** 그대로 두면 실패율이 33% 로 나와서,
// 진짜 실패가 났을 때 그 숫자가 아무 신호도 못 준다.
http.setResponseCallback(http.expectedStatuses(200, 400));

const BASE = __ENV.BASE_URL || 'http://localhost:18080';

// 최대 20. 이 시험의 목적은 배선 확인이고, 규모는 별도로 잰다.
export const options = {
  scenarios: {
    smoke: {
      executor: 'ramping-vus',
      startVUs: 1,
      stages: [
        { duration: '10s', target: 20 },
        { duration: '20s', target: 20 },
        { duration: '5s', target: 0 },
      ],
    },
  },
  // 판정은 여기가 아니라 evaluate-gate.sh 가 한다. 이건 조기 중단용이다.
  thresholds: {
    checks: ['rate>0.99'],
  },
};

const 회원_헤더 = (id) => ({
  'X-Member-Id': String(id),
  'X-Member-Grade': 'GOLD',
});

export default function () {
  const 회원 = __VU * 1000 + __ITER;

  // 조회는 그대로 프록시된다.
  const 조회 = http.get(`${BASE}/api/v1/coupons/c1`, { headers: 회원_헤더(회원) });
  check(조회, { '조회가 뒷단까지 간다': (r) => r.status === 200 });

  // 발급은 판정 필터를 지난다. 지금은 통과만 시킨다 — 판정 내용은 CY-400.
  const 발급 = http.post(`${BASE}/api/v1/coupons/c1/issue`, null, {
    headers: 회원_헤더(회원),
  });
  check(발급, { '발급이 뒷단까지 간다': (r) => r.status === 200 });

  // 형식이 깨진 요청은 게이트웨이가 끊는다. 뒷단까지 가면 안 된다.
  const 깨진_것 = http.get(`${BASE}/api/v1/coupons/c1`, {
    headers: { 'X-Member-Grade': 'GOLD' },
  });
  check(깨진_것, { '헤더가 없으면 게이트웨이가 끊는다': (r) => r.status === 400 });
}

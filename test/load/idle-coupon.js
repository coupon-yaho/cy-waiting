// R1 실측. **한산한 쿠폰은 줄 없이 지나가야 한다.**
//
// 단위 시험(G2.1)은 도메인 안에서만 이걸 증명한다. 여기서 다시 재는 이유는
// 레거시가 뒤집혔던 자리라서다 — 필터 순서, 스냅샷 갱신, 리미터가 다 붙은
// 상태에서 한 번 더 본다.
//
// **뒤집힘은 조용하다.** 한산한 쿠폰이 줄을 서도 응답은 202 라 정상으로 보이고,
// 사용자는 기다릴 뿐 오류를 안 본다. 그래서 "줄이 하나도 안 섰는가" 를 값으로
// 못 박는다.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

http.setResponseCallback(http.expectedStatuses(200, 202));

const BASE = __ENV.BASE_URL || 'http://localhost:18080';

// **저부하다.** R1 은 "부하가 없을 때" 의 성질이라, 몰아치면 줄이 서는 것이
// 정상이고 그때는 아무것도 못 잰다. 유휴 몫 안에 확실히 들어가는 양만 보낸다.
export const options = {
  scenarios: {
    idle: {
      executor: 'constant-arrival-rate',
      // 유휴 몫은 노드 예산 × 0.7 이다. 스텁이 보고하는 300 크레딧 기준으로
      // 초당 5건은 그 안에 넉넉히 들어간다.
      rate: 5,
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 10,
    },
  },
  thresholds: {
    checks: ['rate>0.99'],
    // **여기가 이 시나리오의 전부다.** 하나라도 줄을 섰으면 R1 이 깨진 것이고,
    // 그 뒤의 어떤 초록도 뜻이 없다.
    queued_responses: ['count==0'],
  },
};

const queuedResponses = new Counter('queued_responses');
const admittedResponses = new Counter('admitted_responses');

// **막힌 건수를 센다.** 실패율만 보면 "무언가 막았다" 까지만 알 수 있는데,
// 그게 다른 쿠폰의 부하 때문인지 이 쿠폰의 상한 때문인지를 가르는 것이 R1
// 판정의 전부다. 이유까지는 게이트웨이 지표(`waiting_abuse_total` 의 `key`
// 태그)가 답한다 — 여기서 맵으로 모으면 VU 마다 따로 도는 값이라 안 합쳐진다.
const rejectedResponses = new Counter('rejected_responses');

const memberHeaders = (id) => ({
  'X-Member-Id': String(id),
  'X-Member-Grade': 'GOLD',
  // VU 마다 다른 주소를 준다. 한 사람처럼 보이면 남용 방지에 걸리고, 그
  // 실패가 코드 결함인지 하네스 모양인지 안 갈린다.
  'X-Forwarded-For': `10.2.${__VU % 250}.1`,
});

// **뒷단이 답한 것인지 본다.** 상태만 보면 게이트웨이가 단락시켜도 통과한다.
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

export default function () {
  const member = __VU * 100000 + __ITER;
  const path = '/api/v1/coupons/c1/issue';
  const issue = http.post(`${BASE}${path}`, null, { headers: memberHeaders(member) });

  if (issue.status === 202) {
    queuedResponses.add(1);
  } else if (issue.status !== 200) {
    rejectedResponses.add(1);
  }
  const passed = servedByBackend(issue, path);
  if (passed) {
    admittedResponses.add(1);
  }

  check(issue, {
    // 한산한 쿠폰의 저부하는 전부 뒷단까지 가야 한다. 202 가 하나라도 있으면
    // 이 검사가 떨어지고, 임계가 그것을 실패로 만든다.
    '한산한 쿠폰이 줄 없이 지나간다': () => passed,
  });

  sleep(1);
}

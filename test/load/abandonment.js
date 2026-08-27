// 중도 이탈 (6.6.5). **줄을 서 놓고 안 오는 사람이 크레딧을 먹는다.**
//
// 배분은 줄 앞쪽에 임계를 올려 주는데, 그 사람이 다시 안 오면 그 몫은 허공에
// 쓰인 것이다. Phase 7 의 G7.5(크레딧 낭비 <5%)를 잴 수 있어야 한다.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

http.setResponseCallback(http.expectedStatuses(200, 202, 400, 403, 429, 503));

const BASE = __ENV.BASE_URL || 'http://localhost:18080';

// 이탈 비율. 계획서가 정한 30% 다 — 값을 바꿔 가며 낭비 곡선을 볼 수 있게 둔다.
const ABANDON_RATE = Number(__ENV.ABANDON_RATE || '0.3');

export const options = {
  scenarios: {
    join: {
      executor: 'constant-arrival-rate',
      rate: 300, timeUnit: '1s', duration: '20s',
      preAllocatedVUs: 200, maxVUs: 400,
    },
  },
  thresholds: {
    // 줄이 서야 이탈을 잴 수 있다.
    joined: ['count>0'],
    // **이탈자가 있어야 한다.** 없으면 이 시나리오가 이탈을 안 만든 것이다.
    abandoned: ['count>0'],
    // **안 이탈한 사람은 다시 와야 한다.** 안 오면 이탈률이 100% 로 보이고,
    // 그때 이 시나리오는 이탈을 재는 것이 아니라 배선 오류를 재는 것이다.
    polled: ['count>0'],
  },
};

const joined = new Counter('joined');
const abandoned = new Counter('abandoned');
const polled = new Counter('polled');

const headers = (member) => ({
  'X-Member-Id': String(member),
  'X-Member-Grade': 'GOLD',
  'X-Forwarded-For': `10.12.${__VU % 250}.${(__VU % 250) + 1}`,
});

export default function () {
  const member = __VU * 1000000 + __ITER;
  const issue = http.post(`${BASE}/api/v1/coupons/c2/issue`, null,
      { headers: headers(member) });

  let token = null;
  if (issue.status === 202) {
    joined.add(1);
    try {
      token = issue.json().data.queueToken;
    } catch (e) {
      token = null;
    }
  }
  check(issue, { '판정이 낸 응답이다': (r) => [200, 202, 429, 503].includes(r.status) });

  if (token === null) {
    return;
  }
  // **여기서 갈린다.** 이탈자는 다시 안 온다 — 그 사람 몫으로 올라간 임계가
  // 허공에 쓰이고, 그 낭비가 이 시나리오가 재려는 값이다.
  if (Math.random() < ABANDON_RATE) {
    abandoned.add(1);
    return;
  }
  // **토큰은 헤더로 간다.** 쿼리로 보내면 400 이고, 그 400 은 "안 왔다" 와
  // 구별이 안 돼서 이탈률이 100% 로 보인다 — 실제로 그렇게 잰 적이 있다.
  const status = http.get(`${BASE}/api/v1/coupons/c2/queue`,
      { headers: Object.assign(headers(member), { 'Queue-Token': token }) });
  if (status.status === 200) {
    polled.add(1);
  }
}

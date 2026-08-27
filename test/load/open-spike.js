// 오픈 순간 스파이크 (6.6.4). **200ms 램프가 이 시나리오의 전부다.**
//
// 1초 균등으로 때리면 선착순 오픈의 실제 스파이크를 다섯 배 과소평가한다.
// Phase 10 의 착수 판정이 이 값에 달려 있다.
import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';

// 202 는 줄에 섰다는 뜻이고 429·503 은 그 위의 상한이다. 셋 다 판정이 돈 증거라
// 실패로 안 센다 — 그대로 두면 실패율이 진짜 실패에 아무 신호도 못 준다.
http.setResponseCallback(http.expectedStatuses(200, 202, 429, 503));

const BASE = __ENV.BASE_URL || 'http://localhost:18080';

// **200ms 안에 다 던진다.** k6 가 VU 를 그만큼 빨리 못 띄우면 도착이 늘어지고,
// 그러면 이 시나리오는 재려던 것을 안 재는 것이다 — 실측 램프를 같이 남긴다.
export const options = {
  scenarios: {
    spike: {
      executor: 'ramping-arrival-rate',
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: 2000,
      maxVUs: 20000,
      stages: [
        { duration: '200ms', target: 20000 },
        { duration: '3s', target: 20000 },
        { duration: '2s', target: 0 },
      ],
    },
  },
  thresholds: {
    // 판정이 안 낸 응답이 섞이면 배선이 어긋난 것이다.
    http_req_failed: ['rate<0.01'],
    // **줄이 서야 한다.** 안 서면 스파이크가 유휴 몫을 못 넘긴 것이고,
    // 그때는 이 시나리오가 스파이크를 안 만든 것이다.
    queued_responses: ['count>0'],
  },
};

const queuedResponses = new Counter('queued_responses');
const shedResponses = new Counter('shed_responses');
const retryAfterSeconds = new Trend('retry_after_seconds');

const headers = (member) => ({
  'X-Member-Id': String(member),
  'X-Member-Grade': 'GOLD',
  // VU 마다 다른 주소. 한 사람처럼 보이면 남용 방지가 막고, 그 실패가 코드
  // 결함인지 하네스 모양인지 안 갈린다.
  'X-Forwarded-For': `10.10.${__VU % 250}.${(__VU % 250) + 1}`,
});

// 줄에 섰다. **봉투까지 본다** — 202 만 보면 뒷단이 낸 202 도 통과한다.
const queued = (r) => {
  if (r.status !== 202) {
    return false;
  }
  try {
    const data = r.json().data;
    return data.admitted === false && typeof data.position === 'number';
  } catch (e) {
    return false;
  }
};

export default function () {
  const member = __VU * 100000 + __ITER;
  const issue = http.post(`${BASE}/api/v1/coupons/c2/issue`, null, {
    headers: headers(member),
  });

  if (queued(issue)) {
    queuedResponses.add(1);
  } else if (issue.status === 429 || issue.status === 503) {
    shedResponses.add(1);
    // **다시 올 시각이 흩어져야 한다** (F7). 한 값으로 몰리면 그 초에 같은
    // 스파이크가 다시 온다 — 회복이 곧 두 번째 사고가 된다.
    const after = Number(issue.headers['Retry-After']);
    if (Number.isFinite(after)) {
      retryAfterSeconds.add(after);
    }
  }

  check(issue, {
    '판정이 낸 응답이다': (r) => [200, 202, 429, 503].includes(r.status),
  });
}

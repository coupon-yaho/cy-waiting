// 스모크. **부하를 재는 것이 아니라 하네스가 도는지를 본다.**
//
// 여기서 무거운 것을 돌리면 PR 마다 몇 분씩 걸려 아무도 PR 을 안 연다.
// 규모가 필요한 것은 main·nightly 의 다른 시나리오가 맡는다.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

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
    // **줄이 한 번은 서야 한다.** 뒷단 도달만으로 통과하면 판정을 통째로 우회해도
    // 초록이다 — 이 하네스가 실제로 그랬다. 대기 응답이 나온다는 것이 판정
    // 필터가 돌았다는 유일한 증거다.
    queued_responses: ['count>0'],
  },
};

// **VU 마다 다른 주소를 준다.** 운영에는 앞단 프록시가 실제 클라이언트 주소를
// 붙이므로 한 하네스가 한 사람인 것처럼 보이면 안 된다 — 그러면 남용 방지에
// 걸리고, 그 실패가 코드 결함인지 하네스 모양인지 안 갈린다.
const memberHeaders = (id) => ({
  'X-Member-Id': String(id),
  'X-Member-Grade': 'GOLD',
  'X-Forwarded-For': `10.1.${__VU}.1`,
});

// **뒷단이 답한 것인지 본다.** 상태만 보면 게이트웨이가 200 으로 단락시켜도
// 통과한다 — 스텁이 경로를 되돌려 주는 이유가 그 구별이다.
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

// 헤더와 본문이 같은 키를 가리키는지 본다. 어긋나면 로그와 응답을 잇는
// 키가 남을 가리킨다. 요청마다 다른지는 단위 시험이 본다.
const tracedConsistently = (r) => {
  const header = r.headers['X-Request-Id'];
  if (!header) {
    return false;
  }
  try {
    return r.json().error.requestId === header;
  } catch {
    return false;
  }
};

const queuedResponses = new Counter('queued_responses');

// 줄에 섰다. **봉투까지 본다** — 202 만 보면 뒷단이 낸 202 도 통과한다.
const queued = (r) => {
  if (r.status !== 202) {
    return false;
  }
  try {
    const data = r.json().data;
    return data.admitted === false && typeof data.queueToken === 'string'
        && typeof data.position === 'number';
  } catch {
    return false;
  }
};

export default function () {
  const member = __VU * 1000 + __ITER;

  // 조회는 그대로 프록시된다.
  const listPath = '/api/v1/coupons/c1';
  const list = http.get(`${BASE}${listPath}`, { headers: memberHeaders(member) });
  check(list, { '조회가 뒷단까지 간다': (r) => servedByBackend(r, listPath) });

  // 발급은 판정 필터를 지난다. **둘 다 정상이다** — 유휴 몫 안이면 뒷단으로 가고,
  // 넘치면 줄을 선다. 뒷단 도달만 재면 판정이 도는 순간 그 체크가 거짓이 되고,
  // 실제로 판정을 우회하던 동안에만 초록이었다.
  const issuePath = '/api/v1/coupons/c1/issue';
  const issue = http.post(`${BASE}${issuePath}`, null, {
    headers: memberHeaders(member),
  });
  const wasQueued = queued(issue);
  if (wasQueued) {
    queuedResponses.add(1);
  }
  check(issue, {
    '발급이 판정을 지난다': (r) => servedByBackend(r, issuePath) || wasQueued,
  });

  // 회원 식별자가 없으면 게이트웨이가 끊는다. 뒷단까지 가면 안 된다.
  const noId = http.get(`${BASE}${listPath}`, {
    headers: { 'X-Member-Grade': 'GOLD' },
  });
  // 순번 조회는 서명한 토큰으로만 답한다. 헤더만 들고 오면 끊는다.
  const noToken = http.get(`${BASE}/api/v1/coupons/c1/queue`, {
    headers: memberHeaders(member),
  });
  check(noToken, {
    '토큰 없는 순번 조회는 끊는다': (r) => r.status === 400,
    // 뒷단이 답했으면 스텁이 경로를 되돌려 준다. data 유무로 보면 뒷단이
    // data 없이 400 을 내도 통과한다.
    '순번 조회는 뒷단까지 안 간다': (r) => !servedByBackend(r, '/api/v1/coupons/c1/queue'),
  });

  check(noId, {
    '회원 식별자가 없으면 게이트웨이가 끊는다': (r) => r.status === 400,
    // 뒷단은 캐시 헤더를 안 단다. 우리만 달면 그 헤더로 게이트웨이가 드러난다.
    '뒷단도 내는 거절에 없는 헤더를 안 단다': (r) => !r.headers['Cache-Control'],
    // 본문과 헤더가 어긋나면 로그와 응답을 잇는 키가 남을 가리킨다.
    '응답의 추적 키가 헤더와 본문에서 같다': (r) => tracedConsistently(r),
  });

  // **지시한 간격을 지킨다.** 게이트웨이가 1초로 물으라고 하는데 하네스가
  // 쉬지 않고 두드리면 남용 방지가 막는 것이 맞고, 그건 잴 값이 아니다.
  sleep(1);
}

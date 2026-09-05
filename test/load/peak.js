// 피크 100K RPS 지속 60초 (10.7.1 · R4).
//
// **세 갈래를 실제 비율로 섞는다.** 계획서가 그 비율을 든다 — 2 만 명이 2 초마다
// 폴링하면 10K 라 100K 의 10 분의 1 이고, 나머지는 통과 트래픽과 신규 진입이다.
// 한 갈래만 때리면 R4 의 한 축만 증명한 것이 된다.
//
// **한 러너로는 100K 를 못 만든다.** 실측 도착률이 4,000/초 언저리다 — 이
// 시나리오는 그 사실을 숨기지 않는다. 아래 임계가 목표 유입에 못 미치면
// 빨개지고, 판정기는 그 회차를 근거로 쓰지 않는다. 진짜 값은 생성기를 여러
// 대로 나눠야 나오고, 그건 이 시나리오가 아니라 배선의 일이다.
import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';

// 끊는 것도 줄 세우는 것도 정상 동작이다. 판정이 낸 것은 실패로 안 센다 (O-7).
http.setResponseCallback(http.expectedStatuses(200, 202, 429, 503));

const BASE = __ENV.BASE_URL || 'http://localhost:18080';
const RATE = Number(__ENV.RATE || '100000');
const DURATION = __ENV.DURATION || '60s';

// 갈래별 몫. 계획서 1절의 근거를 그대로 옮긴다 — 바꾸려면 거기를 먼저 고친다.
const POLL_SHARE = Number(__ENV.POLL_SHARE || '0.10');
const ENTER_SHARE = Number(__ENV.ENTER_SHARE || '0.30');

// **갈래는 보낸 수로 센다.** 성공한 것만 세면 섞은 비율을 확인할 수 없다 —
// 몫이 어긋난 것인지 뒷단이 끊은 것인지 같은 수가 두 뜻을 갖는다.
const polled = new Counter('peak_poll');
const entered = new Counter('peak_enter');
const passed = new Counter('peak_pass');
// **표 없이 시작한 폴링 몫을 비율로 잡는다.** 그 갈래는 폴링 대신 진입을 보내므로
// 그만큼 폴링이 10% 아래로, 진입이 30% 위로 간다. 세기만 하면 몫이 어긋난 회차가
// 그대로 지나간다 — 아래 임계가 그것을 회차의 실패로 만든다.
const bootstrap = new Rate('peak_poll_bootstrap');

// 결과는 갈래와 따로 센다. 끊긴 것도 줄 세운 것도 판정이 낸 정상 동작이다 (O-7).
const admitted = new Counter('peak_admitted');
const queued = new Counter('peak_queued');
const closed = new Counter('peak_closed');
const shed = new Counter('peak_shed');
// **판정 밖 응답을 끊긴 것과 섞지 않는다.** 400·404 와, k6 가 연결 실패·타임아웃에
// 쓰는 상태 0 이 여기 온다. 끊은 수에 합치면 목표 유입 근처에서 "판정이 끊었다" 와
// "하네스가 못 붙었다" 가 한 수가 된다.
const offJudgement = new Counter('peak_off_judgement');

// 발급 요청의 결과. 200 은 뒷단까지 갔다는 뜻이다.
function tallyIssue(r) {
  if (r.status === 200) {
    admitted.add(1);
  } else if (r.status === 202) {
    queued.add(1);
  } else if (r.status === 429 || r.status === 503) {
    shed.add(1);
  } else {
    offJudgement.add(1);
    return false;
  }
  return true;
}

// **폴링의 200 은 통과가 아니다.** 줄에서 기다리는 사람도, 매진으로 끝난 사람도
// 200 을 받는다 — 상태만 보면 그 전부가 통과로 세어진다. 봉투를 읽어 가른다.
function tallyPoll(r) {
  if (r.status === 429 || r.status === 503) {
    shed.add(1);
    return true;
  }
  if (r.status !== 200) {
    offJudgement.add(1);
    return false;
  }
  let state = null;
  try {
    state = r.json().data.status;
  } catch (e) {
    // 봉투가 다르면 아래가 판정 밖으로 센다.
  }
  if (state === 'WAITING') {
    queued.add(1);
  } else if (state === 'ADMITTED') {
    admitted.add(1);
  } else if (state === 'CLOSED' || state === 'SOLD_OUT') {
    closed.add(1);
  } else {
    offJudgement.add(1);
    return false;
  }
  return true;
}

export const options = {
  summaryTrendStats: ['min', 'med', 'p(90)', 'p(99)', 'max'],
  scenarios: {
    peak: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      // 도착률을 못 맞추면 흘린다. 아래 임계가 그것을 잡는다.
      preAllocatedVUs: Number(__ENV.VUS || '2000'),
      maxVUs: Number(__ENV.MAX_VUS || '20000'),
    },
  },
  thresholds: {
    // **못 만든 부하로 판정하지 않는다.** 흘린 회차가 있으면 그 회차의 수는
    // 목표 유입의 것이 아니다 — 근거로 쓰면 100K 를 안 낸 채로 100K 를
    // 증명했다고 적게 된다.
    dropped_iterations: ['count==0'],
    // 판정이 안 낸 응답이 섞이면 배선이 어긋난 것이다.
    http_req_failed: ['rate<0.01'],
    // 봉투까지 본 뒤의 어긋남. 상태만 보는 위 임계가 못 잡는 자리다.
    peak_off_judgement: ['count==0'],
    // **검사에 임계를 건다.** 안 걸면 검사가 떨어져도 회차가 안 빨개져,
    // 검사가 있다는 사실만 남고 값이 없다.
    checks: ['rate>0.99'],
    // 표를 못 얻어 진입으로 돌린 몫. 모든 VU 가 표 없이 시작하므로 0 은 될 수
    // 없다. 10% 를 넘으면 섞은 비율이 계획서의 60·10·30 이 아니다.
    peak_poll_bootstrap: ['rate<0.1'],
  },
};

const headers = (member) => ({
  'X-Member-Id': String(member),
  'X-Member-Grade': 'GOLD',
  // 한 주소로 몰면 주소별 한도에 걸려 그 요청이 뒷단에 안 닿는다.
  'X-Forwarded-For': `10.${(__VU % 200) + 20}.${__VU % 250}.${(__ITER % 250) + 1}`,
});

// 줄에 선 사람이 들고 다니는 표. VU 마다 하나라, 진입이 새로 받을 때마다 덮인다 —
// 한 사람이 2 초마다 폴링하는 모양은 아니다. 여기서 재려는 것은 폴링 몫이 실제
// 부하에 실렸을 때 게이트웨이가 견디는가지 개인의 폴링 주기가 아니다.
//
// **폴링의 `X-Member-Id` 는 표의 주인이 아니다.** 판정은 토큰에서 주인을 뽑으므로
// 동작하지만, 헤더를 근거로 읽으면 다른 사람의 자리를 본다고 오해한다.
let token = null;

export default function () {
  const member = __VU * 1000000 + __ITER;
  const dice = Math.random();

  if (dice < POLL_SHARE) {
    // **표가 없으면 폴링이 성립 안 한다.** 그때는 진입으로 돌린다 — 안 그러면
    // 400 이 쌓여 판정 밖 응답이 늘고, 그 회차가 통째로 거절된다.
    bootstrap.add(token === null);
    if (token === null) {
      enter(member);
      return;
    }
    polled.add(1);
    const r = http.get(`${BASE}/api/v1/coupons/c2/queue`,
        { headers: Object.assign(headers(member), { 'Queue-Token': token }) });
    check(r, { '폴링이 판정을 지난다': tallyPoll });
    return;
  }

  if (dice < POLL_SHARE + ENTER_SHARE) {
    enter(member);
    return;
  }

  // **통과 트래픽.** 피크에서는 이 쿠폰도 한산하지 않다 — 몫이 60% 라 초당
  // 수만이 몰린다. 그래서 여기서 202 가 나오는 것은 결함이 아니라 판정이다.
  // 줄 없이 지나가는 것을 확인하는 자리는 `idle-coupon.js` 다.
  passed.add(1);
  const r = http.post(`${BASE}/api/v1/coupons/c1/issue`, null, { headers: headers(member) });
  check(r, { '통과가 판정을 지난다': tallyIssue });
}

function enter(member) {
  entered.add(1);
  const r = http.post(`${BASE}/api/v1/coupons/c2/issue`, null, { headers: headers(member) });
  if (r.status === 202) {
    try {
      token = r.json().data.queueToken || token;
    } catch (e) {
      // 봉투가 다르면 표를 못 얻는다. 다음 회차가 다시 시도한다.
    }
  }
  check(r, { '진입이 판정을 지난다': tallyIssue });
}

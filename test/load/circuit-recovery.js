// 서킷 진입·유지·회복 구간의 **고정 유입 생성기.**
//
// 회차 내내 같은 유입을 게이트웨이 여러 대에 고르게 넣는다. 뒷단이 느려진
// 구간에는 대부분이 끊기거나 줄로 가는데, **그게 정상이다** — 여기서 재는 것은
// 응답이 아니라 그 구간에 배분이 얼마나 나갔는가다. 판정은 표본이 낸다.
//
// **줄에 선 사람을 버리지 않는다.** 넣고 답만 세는 생성기로 재 봤더니, 조인
// 구간에 줄이 한 번 서고 나서 아무도 뒷단에 안 닿았다 — 서킷은 반쯤 열린 채
// 표본을 못 채워 영영 안 닫혔고, 회복 구간이 통째로 안 생겼다. 배분이 반쯤
// 열렸을 때 0 대신 최소 한 건을 내보내는 이유가 그 프로브인데, 그 한 건을
// 실제로 보내 주는 쪽이 없으면 설계가 성립하지 않는다.
//
// 그래서 이 생성기는 **한 사람의 생애를 끝까지 돈다** — 등록하고, 순번을 묻고,
// 차례가 오면 표를 들고 다시 발급을 부른다.
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';

// 끊는 것도 줄 세우는 것도 판정이 낸 정상 동작이다 (O-7). 느린 구간의 503 을
// 실패로 세면 이 시나리오가 자기 자극 때문에 빨개진다.
http.setResponseCallback(http.expectedStatuses(200, 202, 429, 503));

// 쉼표로 구분한 게이트웨이 주소들. 부르는 쪽이 실제로 열린 포트를 찾아 넘긴다 —
// 포트를 범위로 열면 어느 컨테이너가 어느 포트를 받는지 순서가 안 정해진다.
const BASES = (__ENV.BASE_URLS || 'http://localhost:18080').split(',');
const COUPON = __ENV.COUPON || 'c1';
const RATE = Number(__ENV.RATE || 60);
const DURATION = __ENV.DURATION || '75s';
const VUS = Number(__ENV.VUS || 200);
// **표를 곧바로 쓰는 사람들.** 고정 유입만으로 돌리면 한 VU 의 다음 회차가 평균
// 5초 뒤라, 표시된 사람이 그때까지 안 온다 — 줄의 머리부터 표시하므로 그 사람이
// 올 때까지 뒤가 못 지나가고, 서킷의 프로브 공급이 그 간격에 묶인다. 실측에서
// 배분은 초당 2건을 표시하는데 뒷단에 닿는 것이 0.77건이었다.
const HOLDERS = Number(__ENV.HOLDERS || 40);
// 제품이 간격을 안 실었을 때 쓸 값(초). 폴링 정책의 기본과 같은 자릿수다.
const DEFAULT_WAIT_SEC = Number(__ENV.DEFAULT_WAIT_SEC || 1);

export const options = {
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate',
      rate: RATE, timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: VUS, maxVUs: VUS,
    },
    holders: {
      executor: 'constant-vus',
      vus: HOLDERS, duration: DURATION,
      exec: 'holder',
    },
  },
  // **못 만든 부하로 판정하지 않는다.** 흘린 회차가 있으면 기준선 유입이 목표와
  // 다르고, 회복 봉우리를 그 기준선에 견주는 판정이 통째로 어긋난다.
  thresholds: {
    dropped_iterations: ['count==0'],
    // 판정 밖 응답이 섞이면 배선이 어긋난 것이다. 안 걸면 전량이 그것이어도 초록이다.
    circuit_off_judgement: ['count==0'],
    // **아무것도 안 잰 회차를 초록으로 끝내지 않는다.** 표를 들고 뒷단에 닿은 것이
    // 하나도 없으면 회복 봉우리를 잴 재료가 없다.
    circuit_redeemed: ['count>0'],
    // **매달린 대는 죽은 대가 아니지만 정상도 아니다.** 시한 초과를 어느 계수에도
    // 안 담으면, 게이트웨이가 붙은 채 안 답하는 회차가 조용히 초록으로 끝난다.
    circuit_timeout_rate: ['rate<0.05'],
  },
};

const passed = new Counter('circuit_passed');
const queued = new Counter('circuit_queued');
const polled = new Counter('circuit_polled');
const redeemed = new Counter('circuit_redeemed');
const shed = new Counter('circuit_shed');
// **판정 밖 응답만 어긋남이다.** 끊긴 것과 섞으면 느린 구간에서 "판정이 끊었다"
// 와 "배선이 틀렸다" 가 한 수가 된다.
const offJudgement = new Counter('circuit_off_judgement');

// 이 VU 가 지금 들고 있는 것. 줄에 선 사람 하나를 끝까지 따라간다.
let queueToken = null;
let entryToken = null;
let member = 0;

/**
 * 한 요청이 매달릴 수 있는 시간. **리더를 죽이는 순간 그 대로 나간 요청이 문제다** — 기본 시한(60초)까지
 * 매달리면 그 VU 가 회차 내내 묶여, 풀을 다 써도 회차를 흘린다. 흘린 회차는 통째로 판정 불가다 (CY-907).
 */
const REQ_TIMEOUT = __ENV.REQ_TIMEOUT || '5s';

/** k6 가 시한 초과에 붙이는 코드. 연결 실패(1211 계열)와 갈라야 죽은 대를 잘못 안 짚는다. */
const TIMEOUT_CODE = 1050;

/** 요청 인자. 시한을 한 곳에서 준다 — 자리마다 쓰면 하나를 빠뜨려도 안 보인다. */
function params(extra) {
  return { headers: headers(extra), timeout: REQ_TIMEOUT };
}

function headers(extra) {
  return Object.assign({
    'X-Member-Id': String(member),
    'X-Member-Grade': 'GOLD',
    // 한 주소로 몰면 주소별 한도에 걸려 그 요청이 뒷단에 안 닿는다.
    'X-Forwarded-For': `10.${(__VU % 200) + 20}.${(__ITER % 250) + 1}.${(__VU % 250) + 1}`,
  }, extra || {});
}

const gatewayDown = new Counter('circuit_gateway_down');
const timedOut = new Rate('circuit_timeout_rate');

function tally(r) {
  // **연결 자체가 안 된 것은 판정이 아니다.** 죽인 리더로 간 요청이라, 판정 밖
  // 응답으로 세면 우리가 만든 자극이 회차를 무효로 만든다.
  if (r.status === 0) {
    gatewayDown.add(1);
    timedOut.add(r.error_code === TIMEOUT_CODE);
    // **시한 초과는 죽은 것이 아니다.** 자극이 지연이라 살아 있는 대도 늦을 수 있는데, 그것으로 명단에서
    // 빼면 그 VU 가 멀쩡한 대에 영영 안 쏜다 — 회복 구간의 유입이 VU 마다 달라져 판정이 흔들린다.
    // 붙지도 못한 것만 죽은 것으로 본다.
    if (r.error_code !== TIMEOUT_CODE && r.request && r.request.url) {
      const hit = BASES.find((b) => r.request.url.indexOf(b) === 0);
      if (hit) {
        down[hit] = true;
      }
    }
    return;
  }
  timedOut.add(false);
  if (r.status === 200) {
    passed.add(1);
  } else if (r.status === 202) {
    queued.add(1);
  } else if (r.status === 429 || r.status === 503) {
    shed.add(1);
  } else {
    offJudgement.add(1);
  }
}

// 연결이 아예 안 되는 주소. **죽인 리더가 여기 들어온다.**
//
// 실제 앞단은 안 붙는 대를 빼고 산 대에 전량을 몰아준다. 생성기가 죽은 주소로
// 계속 쏘면 그 몫이 통째로 사라져, 승계 뒤 유효 유입이 절반이 된다 — 램프도
// 없는 새 리더가 두 배 부하를 받는 더 가혹하고 현실적인 조합을 한 번도 안
// 지나게 된다. 그리고 그 실패가 판정 밖 응답으로 세어져 회차가 통째로 무효다.
const down = {};

// **주소를 회차마다 바꾼다.** VU 로만 가르면 VU 가 게이트웨이에 고정돼, 한쪽이
// 느려질 때 그쪽 유입만 줄어든다 — 고르개가 아니라 생성기가 부하를 재분배한다.
function base() {
  const alive = BASES.filter((b) => !down[b]);
  const pool = alive.length > 0 ? alive : BASES;
  return pool[(__VU + __ITER) % pool.length];
}

function reset() {
  queueToken = null;
  entryToken = null;
  member = 9_000_000 + __VU * 100_000 + __ITER;
}

// **제품이 말한 간격을 지킨다.** 게이트웨이는 429·503·202 에 `Retry-After` 를
// 실어 보내고 실제 사용자는 그 값으로 돌아온다. 무시하고 촘촘히 집으면 줄이
// 빠지는 순간이 하네스 때문에 몰려, 재려던 회복 봉우리를 스스로 만든다.
export function holder() {
  const wait = step();
  sleep(wait);
}

/**
 * 유입 시나리오는 **새로 오는 사람만** 만든다. 표를 들고 다시 오는 사람은 홀더가 맡는다.
 *
 * <p>여기서 제품이 준 간격만큼 자면 그 VU 가 묶인다. 도착 간격은 유입률이 정하지 이 잠이 정하지 않으므로,
 * 자는 동안 풀이 마르고 회차를 흘린다 — 흘린 회차는 통째로 판정 불가다. 실측 855회, 풀 495 전부 소진
 * (CY-907). 표를 안 들고 끝내므로 촘촘히 되묻는 일도 없다.
 */
export default function () {
  reset();
  step();
  reset();
}

// 제품이 실은 재시도 간격(초). 안 실렸으면 기본 폴링 주기를 쓴다.
function waitOf(r) {
  const raw = r && r.headers ? r.headers['Retry-After'] : null;
  const sec = Number(raw);
  return Number.isFinite(sec) && sec > 0 ? sec : DEFAULT_WAIT_SEC;
}

function step() {
  if (!member) {
    reset();
  }

  // 3. 차례가 왔다. 표를 들고 다시 부른다 — **이 요청이 뒷단에 닿는다.**
  if (entryToken !== null) {
    const r = http.post(`${base()}/api/v1/coupons/${COUPON}/issue`, null,
        params({ 'Entry-Token': entryToken }));
    tally(r);
    if (r.status === 200) {
      redeemed.add(1);
      reset();
      return DEFAULT_WAIT_SEC;
    }
    // **끊겼다고 표를 버리지 않는다.** 이 자리의 429 는 "잠시 뒤에 그 표로 다시
    // 오라" 는 뜻이고(RETRY_TOKEN), 제품은 차례가 온 사람을 줄 뒤로 안 돌린다.
    // 버리면 크레딧은 썼는데 뒷단 호출은 안 만든 허가가 되어, 하네스가 재려던
    // 간극을 스스로 만든다 — 그 수를 제품 탓으로 돌리게 된다.
    //
    // 조인 구간에는 이 429 가 반드시 난다. 게이트가 크레딧을 0 으로 만들면
    // 노드 예산도 0 이라, 유효한 표를 든 사람까지 전원 여기로 온다.
    if (r.status === 0 || r.status === 429 || r.status === 503) {
      return waitOf(r);
    }
    // 그 밖의 거절은 표가 죽은 것이다. 다음 회차에 새로 선다.
    reset();
    return waitOf(r);
  }

  // 2. 줄에 서 있다. 순번을 묻는다.
  if (queueToken !== null) {
    const r = http.get(`${base()}/api/v1/coupons/${COUPON}/queue`,
        params({ 'Queue-Token': queueToken }));
    if (r.status !== 200) {
      tally(r);
      // **줄에 선 사람을 폴링 한 번 실패로 버리지 않는다.** 버려도 레디스의 줄
      // 항목은 남아, 임계가 그 유령 위를 지나가고 아무도 뒷단에 안 닿는다.
      if (r.status === 0 || r.status === 429 || r.status === 503) {
        return waitOf(r);
      }
      reset();
      return waitOf(r);
    }
    polled.add(1);
    let state = null;
    let token = null;
    try {
      const data = r.json().data;
      state = data.status;
      token = data.entryToken || null;
    } catch (e) {
      // 봉투가 다르면 아래에서 새로 선다.
    }
    if (state === 'ADMITTED' && token) {
      entryToken = token;
      queueToken = null;
      // 차례가 왔다. 곧바로 표를 쓰러 간다.
      return 0;
    }
    if (state !== 'WAITING') {
      // 매진이든 이탈이든 이 사람은 끝났다.
      reset();
    }
    return waitOf(r);
  }

  // 1. 아직 안 섰다. 발급을 부른다.
  const r = http.post(`${base()}/api/v1/coupons/${COUPON}/issue`, null, params());
  tally(r);
  if (r.status === 0) {
    return DEFAULT_WAIT_SEC;
  }
  if (r.status === 202) {
    try {
      queueToken = r.json().data.queueToken || null;
    } catch (e) {
      // 표를 못 받았으면 다음 회차에 다시 선다.
    }
    if (queueToken === null) {
      reset();
    }
  } else {
    // 줄 없이 지나갔거나 끊겼다. 어느 쪽이든 다음은 새 사람이다.
    reset();
  }
  return waitOf(r);
}

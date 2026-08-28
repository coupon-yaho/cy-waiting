// 크레딧 낭비 실측 (G7.5). **이탈자가 차례를 받아 가면 그 몫은 허공에 쓰인다.**
//
// 스위퍼가 그것을 막는다 — 이탈자의 생존 신호가 만료되면 줄 앞부분에서 걷어
// 내므로, 임계가 그 사람에게 닿기 전에 빠진다. 그래서 이 시나리오가 재는 것은
// "생존 신호 수명 안에 차례가 오는 사람이 얼마나 되는가" 다.
//
// **판 전체로 재면 안 된다.** 시작 직후에는 아무도 아직 안 걷혔으므로, 처음
// 생존 수명(250초) 동안 차례가 온 이탈자는 전부 낭비다. 그 구간을 포함해 재면
// 낭비율이 `이탈률 × 수명 / 판_길이` 로 나오고, 이것은 기구의 성능이 아니라
// 판을 얼마나 길게 잡았는지를 재는 값이다. 정상 구간에서 잰다.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

http.setResponseCallback(http.expectedStatuses(200, 202, 400, 403, 429, 503));

const BASE = __ENV.BASE_URL || 'http://localhost:18080';
const COUPON = __ENV.COUPON || 'c2';
const ABANDON_RATE = Number(__ENV.ABANDON_RATE || '0.3');

/** 줄에 세울 인원. 임계가 이만큼을 지나가는 데 생존 수명보다 오래 걸려야 한다. */
const POPULATION = Number(__ENV.POPULATION || '2400');

/**
 * 줄을 세우는 시간(초). 이 뒤로는 아무도 안 들어오고 줄이 빠지기만 한다.
 *
 * <b>짧게 몰아 넣는다.</b> 길게 펴면 이탈 시각이 판 전체에 흩어져, 창을 어디에
 * 두든 "아직 수명이 안 지난 이탈자" 가 섞인다 — 그러면 재는 것이 기구가 아니라
 * 창의 위치가 된다. 한 번에 넣으면 수명이 다 같은 시각에 지나간다.
 */
const JOIN_SEC = Number(__ENV.JOIN_SEC || '60');

/** 판 전체 길이(초). 정상 구간이 남을 만큼 길어야 한다. */
const RUN_SEC = Number(__ENV.RUN_SEC || '900');

export const options = {
  scenarios: {
    // **한 사람이 한 반복이다.** 줄에 서고, 이탈하거나 차례가 올 때까지 묻는다.
    // 도착률로 나누면 폴링하는 사람을 붙잡아 둘 수 없어 줄 앞이 비고, 그러면
    // 차례가 와도 아무도 안 받아 가서 낭비율이 100% 로 보인다.
    queue: {
      executor: 'per-vu-iterations',
      vus: POPULATION,
      iterations: 1,
      maxDuration: `${RUN_SEC}s`,
    },
  },
  thresholds: {
    joined: ['count>0'],
    abandoned: ['count>0'],
    admittedHere: ['count>0'],
  },
};

const joined = new Counter('joined');
const abandoned = new Counter('abandoned');
const admittedHere = new Counter('admittedHere');

const headers = (member) => ({
  'X-Member-Id': String(member),
  'X-Member-Grade': 'GOLD',
  'X-Forwarded-For': `10.12.${__VU % 250}.${(__VU % 250) + 1}`,
});

export default function () {
  // **줄 세우는 시간에 고르게 편다.** 한꺼번에 넣으면 앞부분이 통째로 같은
  // 초에 들어와, 임계가 지나가는 시각과 이탈 시각의 관계가 뭉개진다.
  sleep((JOIN_SEC * (__VU - 1)) / POPULATION);

  // **숫자여야 한다.** 신원 필터가 형식을 검사해서, 문자열을 섞으면 전건
  // 400 이고 그 400 은 "줄에 못 섰다" 와 구별이 안 된다 — 실제로 그렇게 쟀다.
  const member = 900000000 + __VU;
  const issue = http.post(`${BASE}/api/v1/coupons/${COUPON}/issue`, null,
      { headers: headers(member) });
  check(issue, { '판정이 낸 응답이다': (r) => [200, 202, 429, 503].includes(r.status) });

  let token = null;
  if (issue.status === 202) {
    joined.add(1);
    try {
      token = issue.json().data.queueToken;
    } catch (e) {
      token = null;
    }
  }
  if (token === null) {
    return;
  }

  // **이탈자는 여기서 사라진다.** 생존 신호를 갱신하지 않으므로 수명이 지나면
  // 스위퍼가 걷는다. 걷히기 전에 임계가 닿으면 그 몫이 낭비다.
  if (Math.random() < ABANDON_RATE) {
    abandoned.add(1);
    return;
  }

  // 차례가 올 때까지 서버가 시킨 간격으로 묻는다. 판이 끝나면 그만둔다.
  const deadline = Date.now() + RUN_SEC * 1000;
  while (Date.now() < deadline) {
    const status = http.get(`${BASE}/api/v1/coupons/${COUPON}/queue`,
        { headers: Object.assign(headers(member), { 'Queue-Token': token }) });
    if (status.status === 200) {
      let data = null;
      try {
        data = status.json().data;
      } catch (e) {
        data = null;
      }
      // 차례가 왔으면 받아 간다 — 이것이 안 일어나면 준 몫이 낭비로 잡힌다.
      if (data && data.status === 'ADMITTED') {
        admittedHere.add(1);
        http.post(`${BASE}/api/v1/coupons/${COUPON}/issue`, null,
            { headers: Object.assign(headers(member), { 'Entry-Token': data.entryToken || '' }) });
        return;
      }
    }
    // **서버가 시킨 대로 기다린다.** 우리 마음대로 짧게 물으면 생존 신호가
    // 계속 갱신돼 이탈자와 성실한 사람의 차이가 사라진다.
    const after = Number(status.headers['Retry-After'] || '3');
    sleep(Number.isFinite(after) && after > 0 ? after : 3);
  }
}

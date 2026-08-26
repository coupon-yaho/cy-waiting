// 뒷단 쿠폰 서비스 대역.
//
// **용량을 흉내 내는 것이 존재 이유다.** 지연과 동시 한도를 환경변수로 받아
// 인스턴스마다 다르게 띄울 수 있어야, 가용량 기반 라우팅을 검증할 수 있다.
//
// 응답 봉투는 발급 계층 명세를 따른다. 게이트웨이가 직접 내는 것과 구별되면
// 그 차이로 게이트웨이의 존재와 상태를 알아낼 수 있다.
import { createServer } from 'node:http';

// **설정 오타를 조용히 넘기지 않는다.** 숫자가 아니면 NaN 이 되고, NaN 비교는
// 전부 거짓이라 한도가 무제한이 된다 — 용량을 흉내 내려고 만든 스텁이 아무것도
// 안 재는 상태로 돈다.
function num(name, fallback, { integer = false, min = 0, max = Infinity } = {}) {
  const raw = process.env[name];
  // **안 넣은 것과 공백을 넣은 것은 다르다.** 뒤엣것은 실수이므로 기본값으로
  // 덮으면 조용히 넘어간다. 그리고 Number 는 공백을 0 으로 읽어서, 그게 포트면
  // 임시 포트에 붙어 헬스체크도 프록시 주소도 어긋난다.
  if (raw === undefined) {
    return fallback;
  }
  const v = raw.trim() === '' ? NaN : Number(raw);
  const ok = Number.isFinite(v) && v >= min && v <= max
      && (!integer || Number.isInteger(v));
  if (!ok) {
    process.stderr.write(
        `${name} 가 [${min}, ${max}] 안의${integer ? ' 정수' : ' 숫자'}가 아니다: ${raw}\n`);
    process.exit(1);
  }
  return v;
}

const PORT = num('PORT', 8090, { integer: true, min: 1, max: 65535 });
const LATENCY_MS = num('LATENCY_MS', 0);
// 0 은 무제한. 한도를 넘으면 503 을 내 — 뒷단이 못 받는 상태를 흉내 낸다.
// 동시 한도는 세는 값이라 정수다. 1.5 를 받으면 둘째 요청까지 들어온다.
const MAX_INFLIGHT = num('MAX_INFLIGHT', 0, { integer: true });

let inflight = 0;
let served = 0;
let rejected = 0;

function json(res, status, body) {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    'Content-Type': 'application/json',
    'Content-Length': Buffer.byteLength(payload),
  });
  res.end(payload);
}

function error(res, status, code, message) {
  json(res, status, { success: false, data: null, error: { status, code, message } });
}

const server = createServer((req, res) => {
  // 스텁 자신의 상태. compose 의 healthcheck 와 시나리오의 사후 확인이 쓴다.
  if (req.url === '/stub/health') {
    return json(res, 200, { status: 'UP', inflight, served, rejected });
  }

  if (MAX_INFLIGHT > 0 && inflight >= MAX_INFLIGHT) {
    rejected += 1;
    return error(res, 503, 'TEMPORARILY_UNAVAILABLE', '뒷단이 지금 못 받는다.');
  }

  inflight += 1;
  setTimeout(() => {
    inflight -= 1;
    served += 1;
    // 발급이든 조회든 형태만 맞으면 된다. 내용은 게이트웨이가 안 본다.
    json(res, 200, {
      success: true,
      data: { path: req.url, method: req.method },
      error: null,
    });
  }, LATENCY_MS);
});

server.listen(PORT, () => {
  process.stdout.write(`stub up :${PORT} latency=${LATENCY_MS}ms maxInflight=${MAX_INFLIGHT}\n`);
});

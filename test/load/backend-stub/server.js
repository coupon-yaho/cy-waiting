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

/** 공유 선언을 붙일 것인가. 기본은 붙임 — 기존 게이트가 그 상태를 잰다. */
const SHARED_HEADER = process.env.SHARED_HEADER !== 'false';
/**
 * 본문을 이 간격으로 끝없이 흘린다 (0 이면 안 한다).
 *
 * **헤더는 빠르고 본문이 안 끝나는 뒷단**을 만든다. 응답 상한은 헤더가 오기까지만
 * 재므로 그 뒤로는 아무것도 안 걸린다. 그 상태를 재려면 그런 뒷단이 하나 있어야
 * 한다.
 *
 * 더 나쁜 모양 — **헤더 뒤 완전 침묵** — 은 이 노브로 못 만든다. 그쪽이 더 흔하다.
 */
const SLOW_BODY_MS = num('SLOW_BODY_MS', 0);

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
  const startedAt = process.hrtime.bigint();
  setTimeout(() => {
    // **슬로우 본문은 여기서 안 끝난다.** 미리 깎으면 /stub/health 가 0 을
    // 보고하는데 실제로는 소켓이 수천 개 열려 있고, MAX_INFLIGHT 도 그 모드에서
    // 죽는다 — 하네스가 재려던 것을 스스로 못 재게 된다.
    if (SLOW_BODY_MS <= 0) {
      inflight -= 1;
      served += 1;
    }
    // **자기가 쓴 시간을 실어 보낸다.** 게이트웨이 오버헤드를 재려면 뒷단 몫을
    // 빼야 하는데, 설정값(LATENCY_MS)을 빼면 스케줄링 흔들림이 우리 몫으로
    // 넘어온다 — 그 오차가 그대로 판정에 들어간다.
    const spentMs = Number(process.hrtime.bigint() - startedAt) / 1e6;
    res.setHeader('X-Stub-Service-Ms', spentMs.toFixed(3));
    // **공유해도 된다고 말한다.** 게이트웨이는 응답이 개인화됐는지 모르므로
    // 말 안 한 응답은 안 모은다. 안 붙이면 조회 폭주 시나리오의 병합 배수가
    // 조용히 1 이 되고, 그 판은 모으기를 재는 게 아니라 끈 것을 잰다.
    //
    // 끌 수 있게 둔 것은 **계약이 안 선 상태를 재기 위해서**다. 늘 붙이면
    // 하네스가 그 상태를 한 번도 안 지나고, 그러면 "붙기 전까지 어떻게 되는가"
    // 를 아무도 못 본다.
    if (SHARED_HEADER) {
      res.setHeader('Cache-Control', 'public');
    }
    if (SLOW_BODY_MS > 0) {
      // 헤더는 곧바로, 본문은 끝없이. 끊는 것은 게이트웨이 몫이다.
      res.writeHead(200, { 'Content-Type': 'application/json' });
      const tick = setInterval(() => res.write(' '), SLOW_BODY_MS);
      res.on('close', () => {
        clearInterval(tick);
        inflight -= 1;
        served += 1;
      });
      return;
    }
    // 발급이든 조회든 형태만 맞으면 된다. 내용은 게이트웨이가 안 본다.
    json(res, 200, {
      success: true,
      data: { path: req.url, method: req.method },
      error: null,
    });
  }, LATENCY_MS);
});

server.listen(PORT, () => {
  process.stdout.write(`stub up :${PORT} latency=${LATENCY_MS}ms `
    + `maxInflight=${MAX_INFLIGHT} shared=${SHARED_HEADER} slowBody=${SLOW_BODY_MS}ms\n`);
});

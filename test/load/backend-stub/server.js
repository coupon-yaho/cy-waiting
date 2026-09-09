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
let latencyMs = num('LATENCY_MS', 0);
// 0 은 무제한. 한도를 넘으면 503 을 내 — 뒷단이 못 받는 상태를 흉내 낸다.
// 동시 한도는 세는 값이라 정수다. 1.5 를 받으면 둘째 요청까지 들어온다.
const MAX_INFLIGHT = num('MAX_INFLIGHT', 0, { integer: true });

let inflight = 0;
let served = 0;
let accepted = 0;
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

/**
 * 지금 즉시 내야 할 오류 코드. 0 이면 정상이다.
 *
 * **느려지는 고장과 빨리 실패하는 고장은 반대로 움직인다.** 앞엣것은 물린
 * 건수가 쌓여 저절로 걸러지지만, 뒤엣것은 물린 건수가 안 쌓여 그 대가 가장
 * 한가해 보이고 오히려 트래픽을 끌어당긴다. 이 노브가 뒤엣것을 만든다.
 */
let faultStatus = num('FAULT_STATUS', 0, { integer: true, max: 599 });

let faulted = 0;

// 프로브가 친 횟수. **발급 셈과 갈라 둔다** — 합치면 회복 봉우리(RC4)를 재는
// 자가 프로브 회차만큼 부풀고, 그 봉우리로 게이트를 판정하게 된다.
let probed = 0;

const server = createServer((req, res) => {
  // 스텁 자신의 상태. compose 의 healthcheck 와 시나리오의 사후 확인이 쓴다.
  //
  // **고장 중에도 산다.** 여기가 같이 빨개지면 컨테이너가 unhealthy 로 내려가고
  // 하네스가 자기 계기를 잃는다 — 무엇이 고장인지 볼 수단이 사라진다.
  if (req.url === '/stub/health') {
    return json(res, 200, { status: 'UP', inflight, accepted, served, rejected, faulted, probed, faultStatus, latencyMs });
  }

  // **합성 프로브가 치는 경로.** 회복 판정에 쓰는 호출이라 발급과 **같은 자원
  // 상태**를 지나야 한다 — 정적 200 을 주면 뒷단이 느리거나 죽어 있어도 서킷이
  // 닫혀 회복이 거짓이 된다 (09-routing · AIJ-0249).
  //
  // **자리를 안 먹는다.** 물린 건수를 읽기만 하고 안 올린다. 프로브가 용량을
  // 쓰면 회복을 재려던 장치가 회복을 늦춘다.
  //
  // **발급 셈에 안 섞인다.** 실제 뒷단(CY-890)도 이 둘을 지켜야 한다.
  if (req.url === '/stub/ready') {
    probed += 1;
    if (faultStatus > 0) {
      return error(res, faultStatus, 'INSTANCE_FAULT', '이 인스턴스가 지금 못 받는다.');
    }
    if (MAX_INFLIGHT > 0 && inflight >= MAX_INFLIGHT) {
      return error(res, 503, 'TEMPORARILY_UNAVAILABLE', '뒷단이 지금 못 받는다.');
    }
    // 느린 뒷단을 프로브도 느리게 겪는다. 서킷은 5xx 가 아니라 느린 호출로 열린다.
    return setTimeout(() => {
      // **본문이 안 끝나는 모드도 같이 겪는다.** 헤더만 빠른 뒷단에서 프로브가
      // 즉시 200 을 받으면, 발급이 한 건도 못 끝나는데 서킷이 닫힌다 — 이 경로가
      // 없애려던 거짓 회복이 그 모드에서 그대로 난다.
      if (SLOW_BODY_MS > 0) {
        if (res.writableEnded || res.destroyed) {
          return;
        }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        const tick = setInterval(() => res.write(' '), SLOW_BODY_MS);
        const stop = () => clearInterval(tick);
        res.on('close', stop);
        res.on('error', stop);
        return;
      }
      json(res, 200, { status: 'READY', inflight });
    }, latencyMs);
  }

  // **도중에 켜고 끈다.** 기동 환경변수로만 두면 고장을 만들려고 컨테이너를
  // 다시 띄워야 하고, 그러면 재기동과 고장이 같은 자극이 되어 무엇을 잰
  // 것인지 갈리지 않는다.
  // **지연도 도중에 바꾼다.** 서킷은 5xx 응답을 실패로 안 센다 — 게이트웨이가
  // 상태 코드를 서킷에 안 물렸기 때문이고, 그건 뒷단이 요청을 받은 뒤에 낸 답이라
  // 재시도가 곧 초과 발급이 되기 때문이다. 그래서 서킷을 열려면 느린 호출을
  // 만들어야 하고, 그 자극이 여기 있다.
  //
  // **컨테이너를 다시 안 띄운다.** 기동 환경변수로만 두면 자극을 주려고 프로세스를
  // 새로 띄워야 하고, 그러면 누적 셈이 0 으로 돌아가 회차의 도착을 못 잰다.
  if (req.url.startsWith('/stub/latency')) {
    const asked = Number(new URL(req.url, 'http://stub').searchParams.get('ms'));
    if (!Number.isInteger(asked) || asked < 0 || asked > 60000) {
      return error(res, 400, 'BAD_REQUEST', 'ms 는 0~60000 의 정수여야 한다.');
    }
    latencyMs = asked;
    return json(res, 200, { latencyMs });
  }

  if (req.url.startsWith('/stub/fault')) {
    const asked = Number(new URL(req.url, 'http://stub').searchParams.get('status'));
    if (!Number.isInteger(asked) || asked < 0 || asked > 599) {
      return error(res, 400, 'BAD_REQUEST', 'status 는 0~599 의 정수여야 한다.');
    }
    faultStatus = asked;
    return json(res, 200, { faultStatus });
  }

  // **아는 계기 경로가 아니면 404 다.** 발급 핸들러로 흘려보내면 프로브 경로
  // 오타가 200 을 받고 자리를 먹고 받은 건수를 올린다 — 계약 셋 중 둘이 조용히
  // 깨지고, 그 사실을 아무것도 안 알린다.
  if (req.url.startsWith('/stub/')) {
    return error(res, 404, 'NOT_FOUND', '없는 계기 경로다.');
  }

  // **지연을 안 태운다.** 즉시 실패가 이 모드의 요점이다 — 늦게 실패하면
  // 물린 건수가 쌓여 고르개가 알아서 피하고, 재려던 것이 사라진다.
  if (faultStatus > 0) {
    faulted += 1;
    return error(res, faultStatus, 'INSTANCE_FAULT', '이 인스턴스가 지금 못 받는다.');
  }

  if (MAX_INFLIGHT > 0 && inflight >= MAX_INFLIGHT) {
    rejected += 1;
    return error(res, 503, 'TEMPORARILY_UNAVAILABLE', '뒷단이 지금 못 받는다.');
  }

  // **받은 수를 따로 센다.** 회복 버스트는 뒷단이 **받은** 건수로 재는데
  // (RC4), 처리 완료 수로 재면 느린 구간에 밀린 것이 회복 순간에 한꺼번에
  // 끝나면서 봉우리처럼 보인다 — 재려던 유입이 아니라 밀린 일을 잰다.
  accepted += 1;
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
    // 조용히 1 이 되고, 그 회차는 모으기를 재는 게 아니라 끈 것을 잰다.
    //
    // 끌 수 있게 둔 것은 **계약이 안 선 상태를 재기 위해서**다. 늘 붙이면
    // 하네스가 그 상태를 한 번도 안 지나고, 그러면 "붙기 전까지 어떻게 되는가"
    // 를 아무도 못 본다.
    if (SHARED_HEADER) {
      res.setHeader('Cache-Control', 'public');
    }
    if (SLOW_BODY_MS > 0) {
      // **이미 끊겼으면 시작도 안 한다.** 아래 핸들러는 이 시점에 다는데,
      // 그 전에 닫혔으면 close 를 못 받아 인터벌이 영영 안 멎고 inflight 가
      // 안 돌아온다 — MAX_INFLIGHT 가 차서 하네스가 통째로 죽는다.
      if (res.writableEnded || res.destroyed) {
        inflight -= 1;
        return;
      }
      // 헤더는 곧바로, 본문은 끝없이. 끊는 것은 게이트웨이 몫이다.
      res.writeHead(200, { 'Content-Type': 'application/json' });
      const tick = setInterval(() => res.write(' '), SLOW_BODY_MS);
      let done = false;
      const finish = () => {
        if (done) {
          return;
        }
        done = true;
        clearInterval(tick);
        inflight -= 1;
        served += 1;
      };
      res.on('close', finish);
      res.on('error', finish);
      return;
    }
    // 발급이든 조회든 형태만 맞으면 된다. 내용은 게이트웨이가 안 본다.
    json(res, 200, {
      success: true,
      data: { path: req.url, method: req.method },
      error: null,
    });
  }, latencyMs);
});

server.listen(PORT, () => {
  process.stdout.write(`stub up :${PORT} latency=${latencyMs}ms `
    + `maxInflight=${MAX_INFLIGHT} shared=${SHARED_HEADER} slowBody=${SLOW_BODY_MS}ms\n`);
});

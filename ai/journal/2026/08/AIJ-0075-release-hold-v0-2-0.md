---
id: AIJ-0075
date: 2026-08-25
kind: decide
phase: 4
plan: [04-control-plane, 05-data-plane]
jira: CY-267
commits: []
agent: claude-opus-5
confidence: high
promoted-to:
---

# v0.2.0 을 세운 이유

## 무엇을

릴리스 직전 검토에서 **제어 평면의 입력이 통째로 배선돼 있지 않다**는 것이 나왔다.
태그를 세우고 Phase 4 를 실제로 완결하는 쪽으로 돌렸다.

`CapacityCollector.collect(...)` 는 `src/main` 에 호출부가 없다. `lastKnown` 이
생성자에서 받은 `floor` 로 초기화된 뒤 영영 그대로다. `GatewayHeartbeatLoop` 은
`@Component` 도 `@Bean` 도 없어 프로덕션에서 안 돈다 — `GatewayRegistry.observed`
호출부도 0건이다. `coupon:capacity:v1` 을 읽거나 `gw:instances` 에 쓰는 어댑터
메서드 자체가 없다. `ControlPlanePropertiesConfig` 는 `defaults()` 를 그대로
돌려주므로 운영에서 값을 바꿀 수도 없다.

기본값이 `floor = 1`, `expectedNodes = 1` 이라 나오는 값이 이렇다.

| 값 | 계산 | 실제 |
|---|---|---|
| `globalCredit` | `floor` | 1 |
| `gatewayCount` | `expectedNodes` | 1 |
| `globalCap` | `1 / 1` | 1 |
| `idleCap` | `(long)(1 × 0.2)` | 0 |
| fail-open 예산 | `(long)(1 × 0.5)` | 0 |

**R1 이 0% 다.** 한산한 쿠폰의 첫 요청부터 `idleCap = 0` 이라 사다리 9번이
`COUPON_EXHAUSTED` 를 내고 전원 줄을 선다. `PASS_UNDER_CAP` 은 실배선에서 도달
불가 분기다. 레디스가 끊기면 fail-open 예산이 0 이라 전원 503 이다.

## 왜 (근거)

`main` 은 항상 배포 가능해야 한다. 지금 배포하면 대기열이 사실상 항상 켜지고
장애 구간에서 전면 차단이 된다. 이건 릴리스 중 수정으로 덮을 크기가 아니라
Phase 4 가 약속한 것을 실제로 세우는 작업이다.

## 왜 안 드러났는가

**게이트가 우회 경로에서 초록이었다.** 두 층 다 그렇다.

Phase 4 의 G4.7·G4.8·G4.10 은 인메모리 대역이 `registry.observed(...)` 를 손으로
먹여서 통과했다. 계획서도 G4.3·G4.7 은 인메모리로 잰다고 적어 뒀고 실물 재측정을
CY-395·CY-396 으로 떼어 뒀는데, "실물로 다시 잰다" 와 "프로덕션에 배선이 없다" 는
다른 말이다. 후자는 아무 데도 안 적혀 있었다.

Phase 5 의 부하 게이트는 더 나쁘다. `test/load/compose.yml` 이 `coupons:active` 를
시드하지 않아 스냅샷이 영영 비고, 그러면 모든 요청이 "미지 쿠폰 + 낡음" 경로로
빠져 **판정 필터를 통째로 우회한다.** compose 의 healthcheck 주석이 이미 그 사실을
적어 뒀다 — "받는 것으로 보면 재료가 아직 없어 영영 안 뜬다". readiness 가 영영
`OUT_OF_SERVICE` 인 노드에 부하를 넣고 그 결과를 게이트로 썼다.

그 우회 경로에는 상한도 없다. 리미터도 래치도 안 탄다. 사다리 3번이 같은 무지에서
`globalCap` 을 거는 것과 대비된다.

## 고려했으나 택하지 않은 것

- **배선만 고치고 낸다** — 배선을 고치면 `gatewayCount` 가 살아나면서 안전한 방향
  (전면 억제)에서 위험한 방향(N배 초과)으로 뒤집힌다. 부하 하네스가 판정을 우회하는
  한 그 뒤집힘을 잴 수단이 없다. 셋을 같이 닫아야 한다.
- **알려진 제약으로 적고 낸다** — R1 이 0% 인 것은 제약이 아니라 제품이 성립하지
  않는 상태다.
- **게이트 문장을 고쳐 통과시킨다** — 판정 시점에 문장을 고치면 게이트가 결과에
  맞춰 움직인다.

## 확신이 낮은 부분 / 남은 위험

**Phase 4·5 의 게이트 판정을 다시 해야 한다.** 우회 경로에서 잰 것이 어디까지인지
아직 다 세지 않았다. 최소한 G4.7·G4.8·G4.10 과 Phase 5 의 부하 항목이다.

**검토에서 같이 나온 것들.** 릴리스 검토(에이전트 여덟)에서 나온 나머지다. 배선과
독립이라 따로 뗀다.

- 미지 쿠폰의 낡음 구간 통과에 상한이 없다 (`AdmissionGatewayFilter`)
- `SnapshotRefreshLifecycle.PHASE` 가 웹 서버 드레이닝보다 커서 갱신 루프가 먼저
  죽는다. 배포마다 낡음 창이 열린다. 그 파일 주석이 CY-422 로 조건을 걸어 뒀고
  "판정이 요청 경로에 붙기 전에 다시 정한다" 는 조건이 이번에 발동했다
- 큐 상한이 `ZCARD` 를 세는데 v0.2.0 에는 큐를 줄이는 것이 없다. 청소기가 Phase 7
  이라 유령이 쌓여 신규가 영구 거절된다. 임계 위 인원(`ZCOUNT`)을 세야 한다
- `sweep.lua` 의 `K`·`budget` 에 천장이 없다. 8000 에서 `unpack` 이 터지고 그 자리가
  매 틱 같으면 큐가 영구 정지한다
- `grace` 에 두 writer 가 다른 의미의 값을 쓴다. `queue_status` 는 `'admitted'`,
  `sweep` 은 숫자다. sweep 이 붙는 순간 입장자가 지워진다
- 전역 예외 핸들러가 없어 프록시 경로 실패가 스프링 기본 봉투로 나간다. 실측으로
  `ApiError` 봉투와 모양도 `requestId` 형식도 다르다
- `GatewayRegistry` 의 감소 확정 조건이 시험에 안 잠겨 있다. `>=` 를 `>` 로 바꿔도
  전 시험이 통과하고, 그 변형에서는 정상 관측이 감소를 앞당겨 분모가 급락한다
- `QueueStatusFilter.credit()` 을 무조건 "모름" 으로 바꿔도 전 시험이 통과한다.
  응답 본문의 `etaSeconds` 를 단언하는 시험이 0건이다

**모름이 0 초로 나가던 것은 이 브랜치에서 고쳤다.** `EtaPolicy.UNKNOWN` 은 NaN 인데
`(long) Math.max(0, NaN)` 이 0 이라, 배분 속도를 모르는 구간의 대기자 전원이
`etaSeconds: 0` 을 받았다. 순번 5000 인 사람에게 곧 입장이라고 말하는 셈이다.
와이어에 실을 초를 도메인이 정하게 하고(`EtaPolicy.reportSec`) 모르면 가장 넓은
구간의 하한을 준다.

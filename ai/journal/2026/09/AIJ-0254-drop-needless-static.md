---
id: AIJ-0254
date: 2026-09-08
kind: refactor
plan: []
phase: 9
jira: CY-884
commits: [HEAD]
agent: claude-opus-5
confidence: high
promoted-to:
---

# 이유 없는 static 을 걷었다

## 무엇을

메서드에 붙은 `static` 을 패키지마다 훑어 **협력자 없이 붙어 있던 것들만** 뺐다.
동작은 안 바뀐다 — 호출부를 인스턴스 참조로 바꾼 것이 전부다.

| 클래스 | 뺀 것 |
|---|---|
| `AdmissionDecider` | `queueCapacity`, `globalCap` |
| `GatewayPresenceConfig` | `voteFreshSec`, `passed`, `beatStep` |
| `ControlPlaneConfig` | `onLeadershipGained`, `startingCredit` |
| `GatewayRedisPort` | `passArg`, `presence` |
| `BackendFallback` | `notCalled` |

`notCalled` 와 `passArg` 는 `private` 도 같이 되돌렸다. **원래 `private static`
이었던 것은 이 둘뿐이다** — 나머지는 처음부터 패키지 전용으로 태어나 시험이
부르던 것들이라 되돌릴 `private` 이 없다.

## 왜 지금

`notCalled` 와 `passArg` 는 원래 `private` 이었고, 훅이 규칙 위반을 잡았을 때
**`static` 을 빼는 대신 `private` 을 뗐다.** 훅을 통과시킨 것뿐이고 규칙이 막으려던
것은 그대로 남았다 — 상태를 안 쓰는 헬퍼가 상태를 든 클래스 안에 섞여 있는 상태다.

같은 자리가 둘이면 나머지도 같을 것이라 보고 패키지 단위로 다시 훑었다.

## 남긴 것과 이유

`static` 이 근거가 있는 자리는 안 건드렸다.

- **`static final` 초기화에서 부른다** — `PollIntervalPolicy.aliveTtl`, `maxInterval`.
  인스턴스로 바꾸면 초기화 순서가 뒤집힌다.
- **생성 전에 부른다** — `RoutingCandidate.seed`.
- **다른 `static` 이 부른다** — `TrustedProxies.literal`.
- **클래스 상수를 읽는다** — `ErrorBackoff.quiet`, `step`. 인스턴스로 옮기면
  그 상수가 필드가 되고 값이 달라질 여지가 생긴다.
- **유틸리티 클래스** — `RedisKeys`, `Durations`, `CapacityCollector.idleMinimum`,
  `Leadership.newOwnerId`, `ClusterCircuit.eased`, `SnapshotRefresher.dedicatedScheduler`.

## 미룬 것

`AdmissionGatewayFilter.retryAfterSec`(시험 29줄)·`codeOf`(9줄),
`BulkheadMetrics.bind`(5줄), `GatewayRoutes.connectRetryConfig`(4줄) 은 호출부
수정이 시험 쪽에 몰려 이 변경이 리팩터인지 시험 수정인지 안 보인다. `*Metrics.bind`
셋이 같은 모양이라 통째로 판단할 것도 같이 CY-885 로 뺐다.

`GatewayRedisPort.presence` 는 인스턴스로만 바꿨다. 칸 수 검증은 옛 스크립트가
돌려주는 모양을 픽스처로 만들어야 하는데 살아 있는 레디스로는 그 모양이 안 나온다.

## 어떻게 검증했나

- `./gradlew build` 와 `integrationTest --tests '*GatewayRedisPortTest*'` 12건
- 호출부 치환이 같은 객체를 보는지 — `AdmissionDecider` 빈 정의가 하나뿐이고
  시험이 덮는 곳이 없다. 옮긴 다섯은 `@Bean` 이 아니라 CGLIB 가로채기도 없다

## 남은 위험

**지금 등가인 이유가 "상태를 안 쓴다" 뿐이다.** 이 리팩터가 준비한 미래(필드가
생기는 것)가 오면 주어와 기댓값이 다른 인스턴스이던 시험들이 조용히 갈라진다.
그래서 필터 아홉과 판정기 하나를 같은 객체로 묶었다.

`passArg` 를 `private` 으로 되돌리며 직접 부르던 시험을 `beat` 로 옮겼다.
스크립트가 거절하는 값으로 재므로 판별력은 올랐지만, **단위로 잡던 것을 통합으로
옮긴 것이라** 컨테이너가 흔들리면 파싱과 무관한 이유로 빨개진다.

## 다음 사람이 알아야 할 것

**훅이 막으면 규칙이 무엇을 막으려는지 먼저 본다.** 접근 제어자를 넓혀 통과시키는
것은 규칙을 끄는 것과 같고, 이 저장소에서 두 번 그렇게 들어갔다.

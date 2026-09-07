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
| `BackendFallback` | `notCalled` — 여기서만 `private` 도 같이 붙였다 |

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

`AdmissionGatewayFilter.codeOf`, `retryAfterSec`, `GatewayRoutes.connectRetryConfig`,
`BulkheadMetrics.bind` 는 **시험이 클래스 이름으로 부르는 자리가 스물이 넘는다.**
같이 고치면 이 변경이 리팩터인지 시험 수정인지 안 보인다. CY-885 로 뺐다.

`GatewayRedisPort.passArg`, `presence` 는 인스턴스로 바꾸되 `private` 은 안 붙였다 —
상한 묶기와 칸 수 검증을 시험이 직접 부르고, 그 둘은 롤백 구간의 계약이라
지울 시험이 아니다.

## 다음 사람이 알아야 할 것

**훅이 막으면 규칙이 무엇을 막으려는지 먼저 본다.** 접근 제어자를 넓혀 통과시키는
것은 규칙을 끄는 것과 같고, 이 저장소에서 두 번 그렇게 들어갔다.

---
id: AIJ-0096
date: 2026-08-26
kind: refactor
phase: 4
plan: [04-control-plane]
jira: CY-548
commits: []
agent: claude-opus-5
confidence: high
promoted-to:
---

# 갱신 루프 시험을 가상 시간으로

## 무엇을

`SnapshotRefreshLifecycle` 이 스케줄러를 안에서 만들던 것을 생성자로 올렸다.
운영 배선은 그대로 `SnapshotRefresher::dedicatedScheduler` 를 넘긴다.

시험이 `VirtualTimeScheduler` 를 넣어 판을 손으로 돌린다. Awaitility 와 관용치가
전부 빠졌다.

## 왜 (근거)

**이 클래스만 실제로 기다리고 있었다.** 제어 평면의 다른 시험은 이미 가상 시계를
쓰는데(`AllocationSchedulerTest`, `CapacityRefreshTest`), 여기만 `start()` 안에서
`Schedulers.newSingle` 을 만들어 시험이 그 시계를 못 잡았다.

그래서 단언이 전부 관용치였다. "두 줄기면 같은 시간에 두 배로 는다. 한 줄기면 그
절반 언저리다" 같은 문장은 **주기를 늘려도 통과한다.** 실제로 재는 것이 무엇인지가
흐려진다.

가상 시계에서는 "언저리" 가 없다. 세 판을 돌리면 받아온 횟수가 정확히 4다
(구독 즉시 한 번 + 세 판). 두 번 시작해도 4라는 것이 한 줄기라는 증거다.

**멈췄다 켤 때 스케줄러를 새로 받는다.** 시험 픽스처가 팩토리를 부를 때마다 새
가상 시계를 만들도록 했는데, 이건 운영 동작과 같다 — `stop()` 이 스케줄러를
버리므로 다시 켜면 새것이어야 한다. 버린 것을 다시 쓰면 루프가 조용히 죽는다.

## 고려했으나 택하지 않은 것

- **`VirtualTimeScheduler.getOrSet()` 로 전역 교체** — `Schedulers.newSingle` 을
  안 가로챈다. 그리고 전역 상태라 병렬 실행에서 새어 나간다.
- **주기를 더 줄여 기다림을 짧게** — 흔들림이 줄 뿐 없어지지 않는다. 느린 CI 에서
  간헐 실패가 남는다.
- **팩토리 대신 스케줄러 자체를 주입** — 다시 켤 때 버린 것을 받게 된다.

## 확신이 낮은 부분 / 남은 위험

- 가상 시계는 `subscribeOn`·`delayElements` 가 그 시계를 타는 것에 기댄다.
  루프 구현이 다른 스케줄러를 섞어 쓰기 시작하면 시험이 조용히 실시간으로
  돌아간다. 그때는 실행 시간이 다시 늘어나는 것으로 드러난다.
- 종료 순서 시험(`웹_서버가_드레이닝을_끝낸_뒤에_멎는다`)은 단계 비교라 이 변경과
  무관하다. 행위로 재는 것은 CY-547 이다.

## 어떻게 검증했는가

클래스 전체 실행 시간이 0.34초다. 전에는 판이 도는 것을 기다리느라 초 단위였다.

단언이 값으로 바뀌었다 — `hasValueGreaterThan(1)` 이 `hasValue(4)` 가 됐다.
앞엣것은 루프가 한 번만 돌아도 통과한다.

`./gradlew build contextTest` 통과.

## 다음 사람이 알아야 할 것

`SnapshotRefreshLifecycle.of` 는 넷을 받는 쪽이 시험용이다. 운영에서 쓰지 않는다 —
전용 스레드를 안 쓰면 갱신이 요청 처리 뒤에 줄을 선다.

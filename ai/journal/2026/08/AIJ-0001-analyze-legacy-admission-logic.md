---
id: AIJ-0001
date: 2026-08-13
kind: analyze
phase: 0
plan: []
jira: TBD
commits: []
agent: claude-opus-5
confidence: high
promoted-to: B-1
---

# 기존 구현의 어댑티브 판정이 역방향으로 동작함을 확인

## 무엇을

기존 구현(현 `waiting-legacy/`) 2,524줄 전체와 Lua 5개, 설정을 읽고
요구사항 R1("부하 없는 쿠폰은 대기열 없이 통과")의 실제 동작을 추적했다.
**R1이 정확히 반대로 동작**하고 있었다.

## 왜 (근거)

`AllocationScheduler.toState()`에 다음 불변식이 있다.

```
waiting == 0  →  runtime = IDLE
waiting == 0  →  want() = clamp(0, 0, stock) = 0  →  isActive() = false
              →  FairShareAllocator 가 배분 대상에서 제외  →  credit = 0
```

즉 **`IDLE`이면 `credit`은 반드시 0**이다. 두 값이 독립이 아니라 같은
원인(`waiting == 0`)에서 나온다. 그런데 판정은 `cap = credit / N`을 쓰므로:

```
bypassQueue() false → mustQueue() false → localAdmissionCap(credit=0) = 0
→ tryAcquire(cap=0) = false → ENQUEUE_RATE
```

**한산한 쿠폰의 모든 요청이 큐로 간다.** 한 명이 들어가면 `waiting=1` →
다음 틱 `QUEUEING` → 이후 전원 `ENQUEUE_BACKLOG`. 트래픽이 조금이라도 있는
쿠폰은 대기열이 영구히 켜진다.

`FairShareAllocatorTest:74-79`(`대기자가_없는_쿠폰은_몫을_가져가지_않는다`)가
이 불변식을 직접 증명하고 있어, 두 테스트를 나란히 두면 모순이 드러난다.

## 고려했으나 택하지 않은 것

- **`localAdmissionCap`에 하한을 두는 수정** — `max(1, credit/N)` 형태.
  버렸다. `credit=10`·노드 20대에서 총 20이 되어 2배 초과 발행이 된다.
  한 버그를 다른 버그로 바꾸는 것에 불과하다.
- **`IDLE`일 때 판정을 건너뛰는 수정** — 통과 상한이 사라져 백엔드를 보호할
  수단이 없어진다. R1은 "무제한 통과"가 아니라 "큐 없이 통과"다.
- **`credit`의 의미를 바꾸는 수정** — 배분의 공정 몫이라는 정의는 옳다.
  문제는 그것을 통과 허가량으로도 쓴 것이다. 두 개념을 분리하는 쪽이 맞다.

## 확신이 낮은 부분 / 남은 위험

- **없음.** 코드 경로가 결정적이고 기존 테스트 두 개가 서로 모순됨을
  직접 확인했다. 이 항목은 `confidence: high`다.
- 다만 **왜 3개월간 발견되지 않았는가**에 대한 답은 추정이다 —
  `AdmissionDeciderTest.java:24`의 자유형 픽스처가 `(IDLE, credit=1000)`이라는
  프로덕션에 존재할 수 없는 상태를 만들어 검증하고 있었다. 이것이 유일한
  원인인지는 확신할 수 없으나, 최소한 필요조건이었다.

## 어떻게 검증했는가

- `AllocationScheduler.java:209-222`(`toState`), `CouponState.java:73-78`
  (`localAdmissionCap`), `SecondWindowLimiter.java:23-32`, `FairShareAllocator.java:28-33`
  의 코드 경로를 직접 추적
- `FairShareAllocatorTest:74-79`와 `AdmissionDeciderTest:72-78`의 픽스처가
  양립할 수 없음을 확인
- **실행 검증은 하지 않았다** — 빌드·기동을 하지 않고 정적 분석만 했다.
  결론이 결정적 코드 경로에 근거하므로 충분하다고 판단했다

## 다음 사람이 알아야 할 것

이 발견이 **제로베이스 재작성 판단의 근거 중 하나**가 되었다 (AIJ-0002).
수정 자체는 작지만, 이 버그를 살린 구조(자유형 픽스처, `credit`의 이중 의미)가
상태 모델 수준의 문제라 부분 수정으로는 재발을 막을 수 없다고 보았다.

대응 설계는 `plan/02-domain-core.md` 4.1절 (2계층 리미터), 규칙은 B-1·B-2.

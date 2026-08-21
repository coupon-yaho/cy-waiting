# 테스트 규칙 (TS)

---

## TS-1 · MUST · 테스트를 먼저 커밋한다

RED → GREEN → REFACTOR를 **각각 별도 커밋**으로 남긴다. 이력이 곧 TDD 준수의 증거다.

```
test(admission): idle coupon passes without queueing     ← 실패해야 정상
feat(admission): derive idle cap from global credit
refactor(admission): extract limiter tier resolution     ← 선택
```

- RED 커밋 바디 첫 줄에 `[red]`를 남긴다. CI가 이걸 보고 실패를 허용한다
- **GREEN에서 최소 구현을 지킨다.** 다음 테스트를 미리 만족시키면 다음 RED가
  처음부터 통과해 사이클이 무의미해진다

---

## TS-2 · MUST · 테스트 이름은 한글 문장

무엇을 보장하는지가 이름에서 읽혀야 한다.

```java
@Test
void 대기자가_없는_쿠폰은_큐를_거치지_않고_통과한다() { }

@Test
void 스케줄러가_멎어도_줄_선_사람을_추월시키지_않는다() { }
```

`test1`, `shouldWork`, `testDecide` 같은 이름은 쓰지 않는다.

---

## TS-3 · MUST · 픽스처는 도달 가능한 상태만 만든다

**이 프로젝트에서 가장 중요한 테스트 규칙이다.**

이전 구현에서 핵심 기능이 반대로 동작했는데도 테스트가 전부 통과했다.
원인은 픽스처가 `(IDLE, credit=1000)`이라는 **프로덕션에 존재할 수 없는 상태**를
만들 수 있었던 것이다.

```java
// 금지 — 불변식을 어긴 조합을 만들 수 있다
private static CouponState state(QueueMode mode, RuntimeState runtime,
                                 int credit, long stock, long waiting, double scale)

// 올바름 — 각 팩토리가 도달 가능한 상황 하나씩만 만든다
//          (런타임을 유도하는 팩토리는 상태가 둘일 수 있다. 그때는 경계를
//           팩토리에 대고 잰다 — 생성자 단언은 동어반복이다)
CouponStates.idle(long stock)
CouponStates.queueing(int credit, long stock, long waiting)
CouponStates.draining(int credit, long stock, long waiting)
CouponStates.closed(long waiting)
CouponStates.off(long stock)
CouponStates.unknown()
```

**픽스처가 불변식을 어길 수 있으면 테스트는 버그를 증명하지 못한다.**

새 상태 조합이 필요하면 팩토리를 추가하고, **그 상태가 실제로 어떻게 생기는지**를
Javadoc 한 줄로 남긴다. 설명할 수 없으면 그 상태는 존재하지 않는 것이다.

**픽스처는 `src/testFixtures` 에 둔다.** `src/test` 에 두면 다른 소스셋에서 못 쓰고,
프로덕션에 두면 **도달 불가 상태를 만드는 팩토리가 운영 코드에 노출된다** — 이 규칙이
막으려는 것이 정확히 그것이다. Gradle `java-test-fixtures` 플러그인을 쓴다 (T1.2.4).

---

## TS-4 · MUST · 시계를 고정한다

`Clock.fixed` 또는 조작 가능한 구현을 주입한다. 실제 시간에 의존하는 테스트는
느리거나 불안정하고, 초 경계 같은 중요한 분기를 검증하지 못한다.

```java
private final MutableClock clock = MutableClock.at("2026-06-10T00:00:00Z");

@Test
void 초가_넘어가면_카운터가_리셋된다() {
    // ... 상한 소진
    clock.advance(Duration.ofSeconds(1));
    // ... 다시 통과
}
```

---

## TS-5 · SHOULD · 계층별로 다른 도구를 쓴다

| 계층 | 범위 | 도구 | 기준 |
|---|---|---|---|
| **단위** | 순수 도메인 | JUnit 5 | 브랜치 100%, 뮤테이션 생존 ≤ 10% |
| **통합** | 실제 Redis·Lua | Testcontainers | 브랜치 80% |
| **컨텍스트** | 라우트·필터·배선 | `@SpringBootTest` | 판정 분기 100% |
| **카오스** | 장애·회복 | Toxiproxy | 시나리오 전수 |
| **부하** | 처리량·지연 | k6 | 게이트 기준 |

**단위 테스트로 잡을 수 있는 것을 통합 테스트로 잡지 않는다.**
느리고, 실패 원인이 흐려지고, 결국 아무도 안 돌린다.

---

## TS-6 · SHOULD · 속성 테스트를 안전 불변식에 쓴다

예제 기반 테스트로는 "모든 입력에서" 를 증명할 수 없다.
아래 넷은 속성 테스트로 검증한다.

| 속성 | 검증 |
|---|---|
| 배분 총합이 `globalCredit`을 넘지 않는다 | 무작위 입력 10만 회 |
| 표시 순위가 단조 비증가 | 무작위 시퀀스 10만 회 |
| 리미터가 초당 상한을 넘기지 않는다 | 무작위 호출 패턴 |
| 샤드 분포가 균등하다 | 10만 memberId |

---

## TS-7 · MUST · 불안정 테스트는 격리하지 말고 고친다

`@Disabled`나 재시도로 덮지 않는다. 원인을 고치거나 삭제한다.

**재시도로 덮은 테스트는 장애 회복 검증에서 가장 먼저 거짓 신호를 준다** —
회복이 느려서 실패한 것인지 원래 불안정한 것인지 구분할 수 없게 된다.

---

## TS-8 · MUST · 카오스도 TDD로 쓴다

시나리오를 먼저 작성해 **실패하는 것을 확인한 뒤** 대응 로직을 구현한다.
"구현하고 나서 장애를 넣어봤더니 되더라"는 검증이 아니다.

각 시나리오는 **진입·유지·회복** 3단계로 판정한다.
회복 판정이 없으면 "장애는 견뎠는데 복구가 안 되는" 상태를 놓친다.
→ [plan/08-resilience.md](../../plan/08-resilience.md)

---

## TS-9 · MUST · 하네스를 자기검증한다

**장애를 주입하지 못하는 하네스는 모든 카오스 테스트를 통과시킨다.**
하네스 자체가 거짓 신호의 최대 원천이다.

- Toxiproxy가 실제로 연결을 끊는지 확인하는 테스트
- 주입한 지연이 실측 RTT에 나타나는지 확인하는 테스트
- 백엔드 스텁의 용량 설정이 실제 처리량에 반영되는지 확인하는 테스트

---

## TS-10 · SHOULD · 무엇을 막는 테스트인지 남긴다

실패 모드가 조용한 테스트에는 `@DisplayName` 또는 클래스 Javadoc으로
**어떤 사고를 막는지** 적는다.

```java
/**
 * 라우트 설정이 실제로 살아 있는지 검증한다.
 *
 * <p>이 테스트가 막는 실패 모드는 조용하다 — 프리픽스를 틀리거나 필터 이름을
 * 오타 내면 <b>기동은 성공하는데 판정만 사라진다.</b> 전 요청이 그냥 통과하고
 * 부하 시험 전까지 아무도 모른다.
 */
```

---

## TS-11 · SHOULD · 단언은 구체적으로

```java
// 약함 — 무엇이 잘못됐는지 모른다
assertThat(grants).isNotEmpty();

// 강함
assertThat(grants).containsExactly(new Grant(1, 956), new Grant(2, 40));
```

`as()`로 실패 시 맥락을 남긴다.

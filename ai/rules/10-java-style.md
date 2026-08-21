# Java 스타일 규칙 (JS)

---

## JS-1 · MUST · FQDN 금지

타입은 **import 해서 짧은 이름으로** 쓴다.

```java
// 금지
private java.util.List<com.waiting.domain.Grant> grants;

// 올바름
import java.util.List;
import com.waiting.domain.Grant;

private List<Grant> grants;
```

**예외**: 같은 단순명의 타입이 한 파일에 둘 이상 필요할 때만 한쪽을 FQDN으로 쓴다.
이때는 `RULE-EXCEPTION(JS-1)` 주석을 단다. 대개는 그 자체가 설계 냄새이므로
**타입 이름을 바꾸는 쪽**을 먼저 검토한다.

---

## JS-2 · MUST · 와일드카드 import 금지

`import java.util.*` 는 쓰지 않는다. 무엇에 의존하는지가 파일에서 사라진다.

---

## JS-3 · SHOULD · 불변 데이터는 `record`

| 대상 | 선택 |
|---|---|
| 값 객체, DTO, 응답, 설정 | **`record`** |
| 상태를 바꿔야 하는 것 | `class` |
| 프레임워크가 기본 생성자 + setter를 요구할 때 | `class` |

```java
// 좋음 — 불변 값
public record Grant(long couponId, int admit) { }

// 좋음 — 생성자에서 불변식 강제
public record CouponState(QueueMode mode, RuntimeState runtime, int credit, ...) {
    public CouponState {
        if (runtime == RuntimeState.IDLE && credit != 0) {
            throw new IllegalArgumentException("IDLE 상태에서 credit은 0이어야 한다");
        }
    }
}
```

**컴팩트 생성자를 적극 쓴다.** 불변식을 타입 안에 가두는 가장 싼 방법이다.

`@ConfigurationProperties`도 record로 쓴다 — `@DefaultValue`와 함께 쓰면
생성자 바인딩이 동작한다.

---

## JS-4 · SHOULD · Lombok을 쓸 곳과 쓰지 말 곳

**적극 쓴다:**

| 애노테이션 | 용도 |
|---|---|
| `@RequiredArgsConstructor` | 생성자 주입. 필드 추가 시 생성자를 손대지 않아도 된다 |
| `@Slf4j` | 로거 선언. `LoggerFactory.getLogger(X.class)` 반복 제거 |
| `@Builder` | 인자 4개 이상인 생성. record와도 함께 쓸 수 있다 |
| `@Getter` | 프레임워크가 getter를 요구하는 가변 클래스 |
| `@Value` | 불변 클래스 (record를 못 쓰는 경우) |

**쓰지 않는다:**

| 애노테이션 | 이유 |
|---|---|
| `@Data` (JS-9, MUST) | `equals`/`hashCode`/`setter`를 한꺼번에 만든다. 무엇이 생겼는지 아무도 모른다 |
| `@AllArgsConstructor` | 필드 순서가 바뀌면 **컴파일은 되는데 인자가 뒤바뀐다** |
| `@SneakyThrows` | 검사 예외를 숨긴다. 리액티브 체인에서 특히 위험 |
| record 위의 `@Getter` 등 | record가 이미 제공한다. 중복 |

> `@RequiredArgsConstructor`와 생성자 하나뿐인 클래스에서는 `@Autowired`가 필요 없다.
> **생성자가 둘 이상이면 반드시 `@Autowired`로 어느 쪽인지 지정한다** — 없으면
> Spring이 기본 생성자를 찾다 실패한다.

---

## JS-5 · MUST · 필드는 `final`

주입받는 협력자와 설정값은 전부 `final`. 가변이 필요하면 그 이유를 주석으로 남긴다.

메트릭 게이지가 읽어 가는 값처럼 **다른 스레드가 읽는 가변 필드는 `volatile`**로 표시한다.

---

## JS-6 · MUST · Javadoc은 최대 5줄

메서드·클래스 주석의 본문(태그 제외)은 **5줄을 넘지 않는다.**

```java
/**
 * 이번 초에 통과시켜도 되는 인원.
 *
 * <p>몫을 넘긴 사람은 버려지는 게 아니라 큐로 간다 — 오차는 손실이 아니라
 * 지연이며 다음 틱 배분에 반영돼 스스로 교정된다.
 */
public int contendedCap(int gatewayCount) { ... }
```

### 무엇을 쓰는가

| 쓴다 | 쓰지 않는다 |
|---|---|
| **왜** 이렇게 했는가 | 코드를 읽으면 아는 것 |
| 무엇을 **하지 않기로** 했는가 | `@param name 이름` 같은 동어반복 |
| 어기면 무슨 일이 나는가 | 구현 절차 설명 |
| 근거가 바뀌면 재검토해야 할 조건 | |

**5줄을 넘겨야 할 것 같으면 대개 설명이 아니라 설계가 문제다.**
그래도 필요하면 `plan/` 또는 `ai/journal/`에 쓰고 링크한다.

`@param`은 이름만 반복하는 경우 생략한다. **의미가 이름에서 안 드러날 때만** 쓴다.

---

## JS-7 · SHOULD · 매직넘버를 코드에 박지 않는다

튜닝 값은 `@ConfigurationProperties`로, 상수는 이름 있는 `static final`로.
숫자가 왜 그 값인지 한 줄 근거를 남긴다.

```java
/** 틱 주기의 3배. 짧으면 GC 한 번에 리더가 바뀐다 */
@DefaultValue("3s") Duration leaseTtl
```

---

## JS-8 · SHOULD · 이른 반환으로 중첩을 줄인다

가드 절을 먼저 두고 본론을 마지막에. 판정 로직은 특히 **위에서 아래로 읽히는
순서**가 곧 우선순위여야 한다.

---

## JS-10 · SHOULD · 예외 메시지에 값을 담는다

```java
// 나쁨
throw new IllegalStateException("잘못된 상태");

// 좋음
throw new IllegalStateException("Lua 응답을 해석할 수 없습니다: " + raw);
```

단, **사용자에게 나가는 응답에는 내부 값을 담지 않는다.**
실패 사유를 나눠서 응답하면 공격자에게 오라클을 준다.

---

## JS-11 · MUST · 한글 주석, 영문 식별자

| 무엇 | 언어 | 근거 |
|---|---|---|
| 주석·Javadoc | **한글** | 설계 근거를 정확히 적으려면 모국어가 낫다 |
| 로그 메시지 | **한글** | 운영자가 읽는다 (LG-9) |
| 테스트 이름·헬퍼 | **한글** | 시험은 문장이다 (TS-2) |
| **클래스·메서드·변수·레코드·파라미터** | **영문** | 아래 |

식별자는 언어 문법과 도구가 함께 읽는다. IDE 자동완성, 스택트레이스, 프로파일러
출력, 리플렉션 문자열, 로그 필드명이 전부 식별자를 그대로 노출한다. 한글이 섞이면
그 경로마다 인코딩과 정렬이 갈리고, 무엇보다 **한 저장소 안에서 패키지마다 규칙이
달라진다** — 그게 실제로 일어났다.

`control` 패키지 하나가 이걸 어겨 식별자 16개가 한글이었다. `domain` 과 `adapter`
는 멀쩡했다. 규칙이 `PREFER` 였고 훅이 안 보고 있어서 세 파일이 그대로 `develop`
까지 갔다. **`MUST` 로 올리고 훅에 넣었다** — 주석과 문자열을 지운 뒤 한글이
남으면 위반이다.

**운영 코드에 유니코드 이스케이프(`\uXXXX`)를 쓰지 않는다.** 문자열 안도 마찬가지다.

자바는 렉싱 **전에** 이걸 푼다. 그래서 어디에 있든 토큰 경계를 바꾼다.

| 쓰면 | 풀리면 |
|---|---|
| `int \uD55C\uAE00 = 0;` | 한글 식별자가 된다 |
| `/* \u002a/ void 한글() {}` | 주석이 거기서 **닫히고** 뒤가 코드다 |
| `// \u000A int 한글;` | 주석이 거기서 **끊기고** 뒤가 코드다 |

경계를 바꾸는 것만 골라내려 들면 목록이 끝없이 늘어난다. 그래서 통째로 막는다.
한글이 필요하면 문자열에 그대로 적는다 (LG-9). 역슬래시가 짝수면 이스케이프가
아니므로(`"\\uD55C"`) 걸리지 않는다.

```java
// 금지 — 스택트레이스와 프로파일러에 그대로 나온다
public boolean 시계가_앞섰나() { ... }
private record 상태(GatewaySnapshot snapshot, Instant fetchedAt) { }
int \uD55C\uAE00 = 0;                       // 풀리면 한글 식별자다

// 올바름 — 이름은 영문, 왜인지는 한글 주석이 말한다
/** 발행 시각이 이 노드의 현재보다 미래인가 — 시계가 갈렸다는 신호다. */
public boolean isClockAhead() { ... }
```

---

## JS-12 · MUST · 생성자를 직접 노출하지 않는다 — 정적 팩토리를 쓴다

값 객체·상태 객체는 **public 생성자 대신 이름 있는 정적 팩토리**로 만든다.

```java
// 금지 — 인자만 보고는 어떤 상태인지 알 수 없고, 불가능한 조합을 만들 수 있다
new CouponState(ADAPTIVE, IDLE, 1000, 500, 0, 1.0)

// 올바름 — 이름이 어떤 상태인지 말해 준다
CouponState.idle(500)
CouponState.queueing(1000, 500, 20_000)
CouponState.closed(20_000)
```

얻는 것:

| 이점 | 설명 |
|---|---|
| **이름이 의도를 말한다** | 인자 목록이 같아도 다른 팩토리를 만들 수 있다 |
| **불가능한 조합을 원천 차단** | 도달 가능한 상태마다 팩토리 하나 → DS-2 |
| **캐싱·재사용이 가능** | `unknown()` 같은 상수 인스턴스 |
| **구현 교체가 자유롭다** | 호출부가 구체 타입에 묶이지 않는다 |

### 규칙

- **`class`** — 생성자를 `private`으로 두고 정적 팩토리만 노출한다
- **`record`** — 정규 생성자는 언어 제약상 record보다 접근성을 낮출 수 없다.
  따라서 **컴팩트 생성자에서 불변식을 강제**하고, 정적 팩토리를 **권장 진입점**으로
  둔다. 생성자를 막을 수는 없어도 **불가능한 값을 만들 수는 없게** 한다

  ```java
  public record CouponState(QueueMode mode, RuntimeState runtime, int credit, ...) {

      /** 정규 생성자는 숨길 수 없으므로 여기서 불변식을 막는다 */
      public CouponState {
          if (runtime == RuntimeState.IDLE && credit != 0) {
              throw new IllegalArgumentException("IDLE 상태의 credit은 0이어야 한다");
          }
      }

      public static CouponState idle(long stock) { ... }
      public static CouponState queueing(int credit, long stock, long waiting) { ... }
  }
  ```

  불변식이 **여러 필드에 걸쳐 복잡해지면** record를 포기하고 `class` + private
  생성자로 간다. 검증을 통과시키려고 불변식을 느슨하게 만들지 않는다

- 팩토리 이름은 `of` / `from` / 상태명(`idle`, `closed`) 중 하나로 일관되게
- **Spring이 생성자 주입을 하는 컴포넌트는 예외다.** 협력자 주입은
  `@RequiredArgsConstructor`가 만드는 생성자를 그대로 쓴다 (JS-4)

### 예외

`Grant`, `CouponDemand`처럼 **필드가 2~3개이고 조합에 불변식이 없는** 단순 값은
record의 기본 생성자를 그대로 써도 된다. 불변식이 생기는 순간 팩토리로 전환한다.

---

## JS-13 · MUST · private 메서드를 `static`으로 만들지 않는다

```java
// 금지
private static long etaSec(long ahead, int credit) { ... }

// 올바름
private long etaSec(long ahead, int credit) { ... }
```

이유:

- `private static`은 **"이 로직은 이 클래스에 속하지 않는다"는 신호**다.
  그런 로직이 생겼다면 숨길 것이 아니라 **밖으로 꺼내야 한다** — 별도의
  도메인 서비스나 값 객체의 메서드가 될 자리다
- 인스턴스 메서드로 두면 나중에 필드(설정값, 협력자)를 쓰게 될 때
  **시그니처 변경 없이** 확장된다. `static`이면 그때 전 호출부를 고친다
- 클래스 안에서 `static`과 인스턴스가 섞이면 **어느 것이 상태에 의존하는지**를
  매번 확인해야 한다

### 예외 (`static`이 맞는 곳)

| 위치 | 예 |
|---|---|
| `public static` 정적 팩토리 | `CouponState.idle(...)` — JS-12 |
| 유틸리티 클래스의 `public static` | `RedisKeys.queue(...)` — JS-14 |
| 상수 | `private static final Duration NEAR = ...` |
| 중첩 타입 | JS-14 |
| 테스트 픽스처 팩토리 | `CouponStates.idle(...)` |

정적 팩토리(`public static`)와 정적 헬퍼(`private static`)는 다르다.
**전자는 권장, 후자는 금지**다.

---

## JS-14 · MUST · 유틸리티 클래스와 중첩 클래스는 `static`으로

### 유틸리티 클래스

인스턴스가 의미 없는 클래스는 **`final` + `private` 생성자**로 생성을 막는다.

```java
/** Redis 키를 한 곳에 모은다. 문자열이 흩어지면 조용한 사고가 난다 */
public final class RedisKeys {

    private RedisKeys() {
    }

    public static String queue(long couponId) {
        return "queue:{" + couponId + "}";
    }
}
```

단, **유틸리티 클래스를 남발하지 않는다.** 상태에 의존하지 않는 계산이 모였다는
이유만으로 `XxxUtils`를 만들면 도메인 로직이 이름 없는 곳으로 새어 나간다.
계산에 도메인 의미가 있으면 **값 객체나 도메인 서비스**가 맞다.

| 유틸리티가 맞음 | 도메인 서비스가 맞음 |
|---|---|
| `RedisKeys` — 키 문자열 조립 | `FairShareAllocator` — 배분 규칙 |
| 순수 문자열·수치 변환 | `AdmissionDecider` — 판정 규칙 |

### 중첩 클래스

**중첩 클래스는 항상 `static`**으로 선언한다.

```java
// 금지 — 바깥 인스턴스 참조를 암묵적으로 붙든다
private class Accumulator { }

// 올바름
private static final class Accumulator { }
```

**예외는 JUnit 5 의 `@Nested` 하나다.** 그쪽은 static 이면 아예 실행되지
않는다 — 규칙과 프레임워크가 충돌하는 자리라 규칙이 진다. 훅도 면제하므로
`RULE-EXCEPTION` 주석을 달 필요가 없다.

non-static 내부 클래스는 바깥 인스턴스를 잡고 있어, 그 참조가 배경 루프나
컬렉션에 실려 나가면 **바깥 객체 전체가 GC되지 않는다.** 리액티브 체인처럼
객체가 스레드를 넘나드는 곳에서 특히 위험하다.

바깥 필드를 정말 써야 하면 **생성자 인자로 명시적으로 받는다** — 무엇에
의존하는지가 코드에 드러난다.

# 예외·에러 처리 규칙 (EX)

---

## EX-1 · MUST · 정상 실패는 예외가 아니다

이 시스템에서 **매진·큐 상한·새치기 방지는 예외가 아니라 판정 결과**다.
비즈니스 규칙에 따른 거절이지 장애가 아니다.

```java
// 금지 — 제어 흐름에 예외를 쓴다. 100K RPS에서 스택트레이스 생성 비용이 그대로 부하가 된다
if (state.remainingStock() <= 0) {
    throw new SoldOutException(couponId);
}

// 올바름 — 판정은 값으로 반환한다
return AdmissionDecision.REJECT_SOLD_OUT;
```

**예외는 "이 지점에서 의미 있는 값을 반환할 수 없을 때"만 쓴다.**

| 값으로 표현 | 예외로 표현 |
|---|---|
| 매진, 큐 상한, 유입 초과, 새치기 방지 | Redis 접근 실패 |
| 토큰 무효, 인증 실패 | Lua 응답 파싱 불가 |
| 스냅샷 미등재 쿠폰 | 설정 오류 (시크릿 미주입) |

PRD의 `409` / `429` 응답 코드들은 전부 **판정 결과**이지 예외가 아니다.

---

## 2. 예외 계층

```
WaitingException                 (RuntimeException, abstract)
├── BusinessException            도메인 규칙 위반 → 4xx
│   └── ErrorCode 를 반드시 가진다
└── SystemException              인프라·설정 실패 → 5xx
    ├── RedisAccessException
    ├── ScriptExecutionException
    └── ConfigurationException
```

### EX-2 · MUST · 모든 예외는 `WaitingException`을 상속한다

프레임워크 예외를 그대로 흘려보내지 않는다. 경계에서 **한 번 번역**한다.
번역하지 않으면 전역 핸들러가 무엇을 어떻게 응답해야 할지 알 수 없다.

### EX-3 · MUST · `BusinessException`은 스택트레이스를 채우지 않는다

```java
public abstract class BusinessException extends WaitingException {

    protected BusinessException(ErrorCode code, String detail) {
        super(code, detail, null, false, false);   // writableStackTrace = false
    }
}
```

비즈니스 예외는 **어디서 났는지가 아니라 무엇이 위반됐는지**가 정보다.
스택트레이스 생성은 100K RPS에서 무시할 수 없는 비용이고, 어차피 읽지 않는다.

`SystemException`은 **반대다** — 스택트레이스가 유일한 단서이므로 반드시 채운다.

### EX-4 · MUST · 검사 예외를 만들지 않는다

리액티브 체인에서 검사 예외는 람다마다 try-catch를 강요하고, 결국
`@SneakyThrows`나 빈 catch로 이어진다. 전부 `RuntimeException` 계열로 둔다.

---

## 3. ErrorCode

### EX-5 · MUST · 에러 코드는 enum 한 곳에서 관리한다

```java
public enum ErrorCode {

    STOCK_EXHAUSTED(HttpStatus.CONFLICT,     "재고가 모두 소진되었습니다"),
    QUEUE_FULL     (HttpStatus.TOO_MANY_REQUESTS, "대기열이 가득 찼습니다"),
    OVERLOAD       (HttpStatus.SERVICE_UNAVAILABLE, "일시적으로 처리할 수 없습니다"),
    UNKNOWN_COUPON (HttpStatus.NOT_FOUND,    "존재하지 않는 쿠폰입니다"),
    UNAUTHORIZED   (HttpStatus.UNAUTHORIZED, "인증이 필요합니다"),
    ...
}
```

각 코드가 갖는 것:

| 필드 | 용도 |
|---|---|
| `httpStatus` | 응답 상태 |
| `clientMessage` | **사용자에게 나가는** 메시지. 내부 정보 없음 |
| `logLevel` | 이 코드가 발생했을 때의 로그 레벨 |

### EX-6 · MUST · 응답 규격을 백엔드와 일치시킨다

게이트웨이가 종결하는 `409 STOCK_EXHAUSTED`는 **쿠폰 서비스가 주는 것과
클라이언트 입장에서 구별되지 않아야 한다.** 규격이 갈리면 클라이언트가
두 경로를 분기하게 되고, 그때부터 게이트웨이 종결이 관찰 가능한 변경이 된다.

응답 스키마는 `plan/` 의 API 계약을 따른다. 바꿀 때는 양쪽을 같이 바꾼다.

### EX-7 · MUST · 내부 정보를 응답에 담지 않는다

| 응답에 담는다 | 담지 않는다 |
|---|---|
| `code` (열거값) | 예외 메시지 원문 |
| `clientMessage` | 스택트레이스 |
| `retryAfter` 등 행동 지침 | 내부 식별자, 키 이름 |
| `traceId` (지원 문의용) | 실패 사유의 세부 구분 |

**실패 사유를 세분해서 응답하면 공격자에게 오라클을 준다.**
토큰 검증 실패는 위조·만료·타 쿠폰을 구분하지 않고 하나로 수렴시킨다.

내부 상세는 **로그에만** 남기고 `traceId`로 연결한다.

---

## 4. 리액티브에서의 예외

### EX-8 · MUST · 체인 안에서 `throw` 하지 않는다

```java
// 위험 — 어디서 잡히는지 예측이 어렵고, 어떤 연산자는 삼켜 버린다
.map(raw -> {
    if (invalid(raw)) throw new ParseException(raw);
    return parse(raw);
})

// 올바름 — 에러를 신호로 만든다
.flatMap(raw -> invalid(raw)
        ? Mono.error(new ScriptExecutionException(raw))
        : Mono.just(parse(raw)))
```

동기 코드(컴팩트 생성자 검증, 설정 로딩)에서는 `throw`가 맞다.

### EX-9 · MUST · 경계에서 번역한다

```java
// adapter/redis — 인프라 예외를 도메인 경계 밖으로 흘리지 않는다
.onErrorMap(RedisException.class, e -> new RedisAccessException(key, e))
```

### EX-10 · MUST · 예외를 삼키지 않는다

```java
// 금지 — 무슨 일이 있었는지 영영 알 수 없다
.onErrorResume(e -> Mono.empty())

// 최소한 로그를 남긴다
.onErrorResume(e -> {
    log.debug("가용량 정리 실패. 다음 틱에 재시도합니다", e);
    return Mono.empty();
})
```

**의도적으로 무시하는 경우 그 이유를 주석으로 남긴다.**
"청소 실패는 배분을 막을 이유가 못 된다" 같은 근거가 있어야 한다.

### EX-11 · MUST · 응답이 커밋된 뒤에는 되돌리지 않는다

```java
.onErrorResume(error -> {
    if (exchange.getResponse().isCommitted()) {
        return Mono.error(error);   // 상태코드를 바꿀 수 없다
    }
    return writeError(exchange, error);
})
```

---

## 5. 전역 처리

### EX-12 · MUST · `@RestControllerAdvice`로는 부족하다

**게이트웨이에서 가장 흔한 함정이다.**

| 예외 발생 지점 | `@RestControllerAdvice`가 잡는가 |
|---|---|
| `@RestController` | ✅ |
| `RouterFunction` 핸들러 (순번 조회) | ❌ |
| `GatewayFilter` (판정 필터) | ❌ |
| 라우팅·프록시 계층 | ❌ |

이 프로젝트의 예외는 **대부분 뒤 세 곳에서** 난다.
따라서 `ErrorWebExceptionHandler`를 구현해 전역 처리한다.

```java
@Component
@Order(-2)   // DefaultErrorWebExceptionHandler(-1)보다 앞
public class WaitingErrorWebExceptionHandler implements ErrorWebExceptionHandler { ... }
```

`RouterFunction` 쪽은 필요하면 `.onErrorResume`으로 지역 처리를 겸한다.

### EX-13 · MUST · 알 수 없는 예외는 500으로 수렴시키되 로그를 남긴다

`WaitingException`이 아닌 예외가 전역 핸들러에 도달했다면
**번역 지점을 빠뜨렸다는 뜻**이다. ERROR로 로깅하고, 그 자체를 결함으로 다룬다.

---

## 6. 로깅과의 관계

| 예외 종류 | 로그 레벨 | 스택트레이스 |
|---|---|---|
| `BusinessException` | 남기지 않음 (메트릭으로 센다) | 없음 |
| `SystemException` (자동 복구됨) | `WARN` + 억제 (LG-3) | 있음 |
| `SystemException` (복구 불가) | `ERROR` | 있음 |
| 미번역 예외 | `ERROR` | 있음 |

**`BusinessException`을 로깅하지 않는 이유**: 매진·큐 상한은 정상 동작이고,
100K RPS에서 그것을 로깅하면 LG-1을 정면으로 어긴다. 세는 것은 메트릭의 일이다.

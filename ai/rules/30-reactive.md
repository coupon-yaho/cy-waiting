# 리액티브 규칙 (RX)

> 이 게이트웨이는 100K RPS를 상대한다. 이벤트 루프를 한 번 막으면
> 그 노드 전체가 멈춘다. 규칙이 엄격한 이유다.

---

## RX-1 · MUST · 블로킹 호출 금지

이벤트 루프에서 아래를 호출하지 않는다.

```
.block()   .blockFirst()   .blockLast()   .toFuture().get()
Thread.sleep()   CountDownLatch.await()   synchronized 블록 안의 I/O
JDBC, 동기 HTTP 클라이언트, 파일 I/O
```

**허용되는 유일한 곳**: `@PreDestroy` 종료 경로. 이때도 반드시 타임아웃을 건다.

```java
// 종료 시 즉시 등록 해제. Redis가 죽어 있어도 종료가 지연되면 안 된다
redis.opsForHash().remove(KEY, instanceId)
        .timeout(Duration.ofSeconds(1))
        .onErrorResume(error -> Mono.empty())
        .block();
```

---

## RX-2 · MUST · 주기 루프는 `repeatWhen`, `Flux.interval` 아님

```java
// 금지 — Redis가 느려지면 틱이 큐에 쌓였다가 회복 순간 한꺼번에 터진다
Flux.interval(tick).concatMap(i -> doWork())

// 올바름 — 한 번이 끝나야 다음 지연이 시작된다
Mono.defer(this::doWork)
        .onErrorResume(e -> { log.error("...", e); return Mono.just(false); })
        .repeatWhen(done -> done.delayElements(tick, scheduler))
        .subscribe();
```

**배분 루프에서 이걸 어기면 크레딧이 몰아서 발행된다.**
회복 순간에 정확히 일어나므로 평시 테스트로는 안 잡힌다.

---

## RX-3 · MUST · 배경 루프에 스케줄러를 명시한다

`delayElements`, `subscribeOn`은 인자 없이 쓰면 공용 `parallel()` 풀을 쓴다.
트래픽이 몰릴 때 배경 루프가 요청 처리 뒤에 줄을 선다.

```java
.subscribeOn(dedicatedScheduler)
.repeatWhen(done -> done.delayElements(interval, dedicatedScheduler))
```

- **배분 스케줄러**: 전용 단일 스레드
- **스냅샷 갱신**: 전용 스케줄러
- **하트비트**: 전용 스케줄러

갱신 루프가 밀리면 헬스체크가 503을 내고 그 노드가 로테이션에서 빠진다.
부하가 높을 때 노드가 빠지는 것은 최악의 되먹임이다.

---

## RX-4 · MUST · `subscribe()` 결과를 버리지 않는다

`Disposable`을 보관하고 `@PreDestroy`에서 `dispose()`한다.
안 하면 컨텍스트가 내려가도 루프가 계속 돌아 테스트가 서로 오염된다.

---

## RX-5 · MUST · 에러가 루프를 죽이지 않게 한다

`onErrorResume`을 **`repeatWhen` 앞에** 둔다. 뒤에 두면 첫 에러에 루프가 끝난다.

```java
doWork()
    .onErrorResume(this::logAndContinue)     // ← 먼저
    .repeatWhen(done -> done.delayElements(interval, scheduler))
```

---

## RX-6 · MUST · 실패해도 마지막 좋은 상태를 지우지 않는다

Redis가 흔들리는 동안 판정 재료를 비우면 전면 장애가 된다.
갱신 실패 시 **기존 스냅샷을 유지**하고, 낡음은 별도로 판정한다.

**판정 재료에 적용한다 — 권한에는 아니다.** 낡은 재료로 판정하는 것은 유계지만
리더가 둘인 것은 유계가 아니다. 리더십은 리스가 그 유계라, 리스가 지나면
확인 없이도 내려온다 (`control/Leadership`).

---

## RX-7 · MUST · 요청 바디를 읽지 않는다

파싱하려면 전체를 버퍼링해야 해서 메모리와 지연이 같이 터진다.
필요한 식별자는 **경로변수·헤더**로 받는다.

**응답 버퍼링은 다른 문제다.** 조회 코얼레싱은 응답을 모아 나눠주려면 버퍼링이
필요하다 — 그건 **크기 상한을 두고 넘으면 포기하는** 조건으로 허용한다
(`plan/06-protection.md` 6.10, B-16). 요청 바디는 그런 예외가 없다.

---

## RX-8 · SHOULD · 타임아웃을 항상 건다

외부 호출에는 타임아웃이 있어야 한다. 없으면 느린 의존이 커넥션을 잠식한다.
타임아웃 값은 설정으로 빼고 근거를 한 줄 남긴다.

---

## RX-9 · SHOULD · 응답이 커밋된 뒤에는 되돌리지 않는다

```java
.onErrorResume(error -> {
    if (exchange.getResponse().isCommitted()) {
        return Mono.error(error);   // 이미 쓰기 시작했으면 상태코드를 못 바꾼다
    }
    return writeFallback(exchange);
})
```

---

## RX-10 · SHOULD · 컨텍스트를 스레드 로컬로 나르지 않는다

MDC·`ThreadLocal`은 리액티브 체인에서 스레드를 넘나들며 깨진다.
Reactor Context 또는 명시적 인자로 전달한다.

---

## RX-11 · MUST · 공유 가변 상태에는 메모리 가시성을 명시한다

- 배경 루프가 쓰고 메트릭 스레드가 읽는 필드 → `volatile`
- 참조 통째 교체로 발행하는 스냅샷 → `volatile` 참조 + 불변 객체
- 카운터 → `AtomicLong` / `LongAdder`

**락을 판정 경로에 두지 않는다.** 스냅샷을 통째로 교체하는 설계가
락 없이 일관된 읽기를 보장하는 이유다.

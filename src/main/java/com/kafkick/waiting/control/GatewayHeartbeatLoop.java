package com.kafkick.waiting.control;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * 하트비트를 주기적으로 남기고, 관측한 노드 수를 넘긴다.
 *
 * <p><b>수명은 스프링에 맡긴다.</b> 직접 만들면 웹 서버가 내려간 뒤 도는지 전에
 * 도는지를 알 수 없고, 그 차이가 드레이닝 중 판정을 가른다 — {@link #getPhase()}.
 */
public final class GatewayHeartbeatLoop implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(GatewayHeartbeatLoop.class);

    private final Supplier<Mono<Integer>> beat;
    private final Supplier<Mono<Void>> leave;
    private final IntConsumer observed;
    private final Duration interval;
    private final Duration leaveTimeout;

    private final AtomicBoolean running = new AtomicBoolean();

    /**
     * 하트비트를 못 쓰기 시작한 시각. 성공하면 지운다.
     *
     * <p><b>매 판 찍으면 로그가 폭발한다.</b> 틱 2초·5분 단절·30대면 4,500줄이다.
     * 진입에 한 번, 해제에 지속 시간과 함께 한 번만 남긴다.
     */
    private final AtomicReference<Instant> failingSince = new AtomicReference<>();
    /** 구독과 스케줄러는 {@code start} 에서 정해져 {@code stop} 이 지운다. */
    private volatile Disposable subscription;
    private volatile Scheduler owned;

    private GatewayHeartbeatLoop(Supplier<Mono<Integer>> beat, Supplier<Mono<Void>> leave,
            IntConsumer observed, Duration interval, Duration leaveTimeout) {
        this.beat = beat;
        this.leave = leave;
        this.observed = observed;
        this.interval = interval;
        this.leaveTimeout = leaveTimeout;
    }

    public static GatewayHeartbeatLoop of(Supplier<Mono<Integer>> beat, Supplier<Mono<Void>> leave,
            IntConsumer observed, Duration interval, Duration leaveTimeout) {
        return new GatewayHeartbeatLoop(beat, leave, observed, interval, leaveTimeout);
    }

    @Override
    public void start() {
        // **CAS 를 먼저 한다.** 스케줄러를 먼저 만들면 두 번째 호출이 새 스레드를
        // 만들고 owned 를 덮어쓴 뒤 반환해, 원래 스레드가 영영 산다.
        if (!running.compareAndSet(false, true)) {
            return;
        }
        owned = Schedulers.newSingle("gw-heartbeat", true);
        subscription = loop(owned).subscribe();
    }

    /** 스케줄러를 밖에서 준다 — 시험이 가상 시간으로 돌리려면 필요하다. */
    public void start(Scheduler scheduler) {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        subscription = loop(scheduler).subscribe();
    }

    private Flux<Void> loop(Scheduler scheduler) {
        return Mono.defer(beat)
                // **타임아웃이 없으면 루프가 조용히 멎는다.** 한 판이 안 끝나면
                // 다음 지연이 시작되지 않는데, 오류가 아니라 무응답이라 아래
                // 로그도 안 나온다. 그동안 이 노드는 하트비트를 못 쓰면서
                // 요청은 계속 받아, 리더가 분모에서 뺀 뒤에도 통과를 만든다.
                .timeout(interval, scheduler)
                .doOnNext(count -> {
                    exitFailing();
                    observed.accept(count);
                })
                // **한 판이 터져도 루프는 돈다.** 여기서 멎으면 그 노드가 영영
                // 분모에서 빠지고, 남은 노드가 큰 몫을 쓴다.
                .doOnError(this::enterFailing)
                .onErrorResume(e -> Mono.empty())
                .then()
                .repeatWhen(done -> done.delayElements(interval, scheduler))
                .subscribeOn(scheduler);
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        Disposable current = subscription;
        if (current != null) {
            current.dispose();
        }
        // **등록 해제가 종료를 붙들면 안 된다.** 못 지워도 임계가 지나면 알아서
        // 빠진다. 반대로 붙들면 오케스트레이터가 강제 종료하고, 그때는 진행
        // 중인 요청까지 함께 끊긴다.
        try {
            Mono.defer(leave)
                    .timeout(leaveTimeout)
                    .doOnError(e -> log.warn("등록 해제 실패 — 임계가 지나면 빠진다: {}", e.toString()))
                    .onErrorResume(e -> Mono.empty())
                    // 안쪽 timeout 이 먼저 끊는다. 이 값은 그것도 안 들을 때의
                    // 마지막 방어다.
                    // RULE-EXCEPTION(RX-1): 종료 경로이고 stop 이 동기 규약이라,
                    // 안 기다리면 컨테이너가 다음 단계로 넘어가 해제가 유실된다.
                    .block(leaveTimeout.multipliedBy(2));
        } catch (RuntimeException e) {
            log.warn("등록 해제를 기다리지 않는다: {}", e.toString());
        }
        Scheduler mine = owned;
        if (mine != null) {
            mine.dispose();
            owned = null;
        }
    }

    private void enterFailing(Throwable cause) {
        if (failingSince.compareAndSet(null, Instant.now())) {
            log.warn("하트비트 실패 — 직전 분모를 유지한다: {}", cause.toString());
        }
    }

    private void exitFailing() {
        Instant since = failingSince.getAndSet(null);
        if (since != null) {
            log.info("하트비트 복구 — {}초 동안 못 썼다", Duration.between(since, Instant.now()).toSeconds());
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    /**
     * 웹 서버가 드레이닝을 <b>마친 뒤</b>, 레디스 커넥션이 <b>닫히기 전</b>에 멈춘다.
     *
     * <p>스프링은 phase 내림차순으로 멈춘다 — 웹 서버가 먼저, 커넥션 팩토리(0)가
     * 나중이다. 그 사이가 등록을 뺄 수 있는 유일한 창이다.
     */
    @Override
    public int getPhase() {
        // 0 이하면 커넥션이 닫힌 뒤에 해제해 매번 실패하고, 유령 항목이 임계
        // 동안 분모를 부풀린다. 드레이닝보다 먼저 빼도 안 된다 — 아직 요청을
        // 처리하는 노드를 분모에서 빼면 남은 노드가 크레딧을 다 쓰고 그 위에
        // 이 노드의 통과분이 더해진다.
        return 1024;
    }
}

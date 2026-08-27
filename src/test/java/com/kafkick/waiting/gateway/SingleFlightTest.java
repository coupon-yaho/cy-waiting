package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

/**
 * 같은 키로 동시에 온 요청을 <b>뒷단 한 번</b>으로 모읍니다.
 *
 * <p>오픈 순간 사용자는 큐에 서기 전에 쿠폰 페이지를 먼저 엽니다. 발급은 판정이
 * 막아 주는데 조회는 그대로 통과해서, 그 순간 뒷단에 100K 가 꽂힙니다.
 */
class SingleFlightTest {

    private final SingleFlight<String> flight = SingleFlight.create();

    /**
     * <b>이 기능의 전부입니다.</b> 뒷단 호출이 요청 수만큼 나가면 모은 것이 아닙니다.
     */
    @Test
    @DisplayName("같은_키의_동시_요청은_뒷단을_한_번만_부른다")
    void 같은_키의_동시_요청은_뒷단을_한_번만_부른다() {
        AtomicInteger 뒷단_호출 = new AtomicInteger();
        Sinks.One<String> 아직_안_끝남 = Sinks.one();

        List<Mono<String>> 요청들 = IntStream.range(0, 1_000)
                .mapToObj(i -> flight.join("k", () -> {
                    뒷단_호출.incrementAndGet();
                    return 아직_안_끝남.asMono();
                }))
                .toList();
        요청들.forEach(Mono::subscribe);
        아직_안_끝남.tryEmitValue("응답");

        assertThat(뒷단_호출).hasValue(1);
        assertThat(요청들).allSatisfy(m ->
                StepVerifier.create(m).expectNext("응답").verifyComplete());
    }

    /** 키가 다르면 따로 나가야 한다. 하나로 모으면 남의 응답을 받는다. */
    @Test
    @DisplayName("키가_다르면_따로_부른다")
    void 키가_다르면_따로_부른다() {
        AtomicInteger 호출 = new AtomicInteger();

        StepVerifier.create(Mono.zip(
                        flight.join("a", () -> Mono.fromSupplier(() -> "a" + 호출.incrementAndGet())),
                        flight.join("b", () -> Mono.fromSupplier(() -> "b" + 호출.incrementAndGet()))))
                .assertNext(t -> assertThat(t.getT1()).isNotEqualTo(t.getT2()))
                .verifyComplete();
        assertThat(호출).hasValue(2);
    }

    /**
     * <b>끝나면 자리를 비웁니다.</b> 안 비우면 다음 요청이 지난 응답을 받고,
     * 그때부터 그 키는 영영 갱신 안 됩니다.
     */
    @Test
    @DisplayName("끝나면_다음_요청은_다시_부른다")
    void 끝나면_다음_요청은_다시_부른다() {
        SingleFlight<Integer> 센다 = SingleFlight.create();
        AtomicInteger 호출 = new AtomicInteger();

        StepVerifier.create(센다.join("k", () -> Mono.fromSupplier(호출::incrementAndGet)))
                .expectNext(1).verifyComplete();
        StepVerifier.create(센다.join("k", () -> Mono.fromSupplier(호출::incrementAndGet)))
                .expectNext(2).verifyComplete();

        assertThat(센다.inFlight()).isZero();
    }

    /**
     * <b>터져도 자리를 비웁니다.</b> 뒷단이 실패하는 구간이 곧 이 장치가 가장
     * 필요한 구간인데, 거기서 자리가 새면 그 키가 영영 막힙니다.
     */
    @Test
    @DisplayName("터져도_자리를_비운다")
    void 터져도_자리를_비운다() {
        StepVerifier.create(flight.join("k", () -> Mono.error(new IllegalStateException("뒷단"))))
                .verifyError(IllegalStateException.class);

        assertThat(flight.inFlight()).isZero();
        StepVerifier.create(flight.join("k", () -> Mono.just("다음")))
                .expectNext("다음").verifyComplete();
    }

    /**
     * <b>앞선 사람이 끊어도 뒤엣사람은 답을 받아야 합니다.</b> 모은다는 것이
     * "먼저 온 사람의 사정에 나머지가 묶인다" 는 뜻이면 안 됩니다.
     */
    @Test
    @DisplayName("먼저_온_요청이_끊겨도_나머지는_답을_받는다")
    void 먼저_온_요청이_끊겨도_나머지는_답을_받는다() {
        Sinks.One<String> 뒷단 = Sinks.one();
        Mono<String> 첫째 = flight.join("k", 뒷단::asMono);
        Mono<String> 둘째 = flight.join("k", 뒷단::asMono);

        // **기존 구독으로 재야 한다.** 나중에 새로 구독하면, 첫째의 취소로 둘째의
        // 원래 구독이 같이 죽었어도 이 시험이 통과한다.
        AtomicReference<String> 둘째가_받은_것 = new AtomicReference<>();
        Disposable 첫_구독 = 첫째.subscribe();
        둘째.subscribe(둘째가_받은_것::set);
        첫_구독.dispose();
        뒷단.tryEmitValue("응답");

        assertThat(둘째가_받은_것).hasValue("응답");
    }

    /**
     * <b>키 가짓수에 상한이 없습니다.</b> 경로와 쿼리로 만드는 값이라, 안 막으면
     * 맵 하나가 메모리를 밀어냅니다.
     */
    @Test
    @DisplayName("상한을_넘으면_모으지_않고_그냥_부른다")
    void 상한을_넘으면_모으지_않고_그냥_부른다() {
        SingleFlight<Integer> 좁은_것 = SingleFlight.withMaxKeys(2);
        AtomicInteger 호출 = new AtomicInteger();
        Sinks.One<Integer> 안_끝남 = Sinks.one();
        좁은_것.join("a", 안_끝남::asMono).subscribe();
        좁은_것.join("b", 안_끝남::asMono).subscribe();

        // **막지 않고 그냥 부릅니다.** 여기서 거절하면 보호 장치가 조회를 끊는
        // 것이 되고, 그건 없느니만 못합니다.
        StepVerifier.create(좁은_것.join("c", () -> Mono.fromSupplier(호출::incrementAndGet)))
                .expectNext(1).verifyComplete();
        StepVerifier.create(좁은_것.join("c", () -> Mono.fromSupplier(호출::incrementAndGet)))
                .expectNext(2).verifyComplete();

        assertThat(좁은_것.inFlight()).isEqualTo(2);
        안_끝남.tryEmitValue(0);
    }

    @Test
    @DisplayName("상한이_1_미만이면_만들지_못한다")
    void 상한이_1_미만이면_만들지_못한다() {
        assertThatThrownBy(() -> SingleFlight.withMaxKeys(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 시간이 아니라 완료로 비운다. 이 시험은 그 약속이 유지되는지만 본다. */
    @Test
    @DisplayName("안_끝난_것은_자리를_쥐고_있다")
    void 안_끝난_것은_자리를_쥐고_있다() {
        Sinks.One<String> 안_끝남 = Sinks.one();
        flight.join("k", 안_끝남::asMono).subscribe();

        assertThat(flight.inFlight()).isEqualTo(1);

        안_끝남.tryEmitValue("끝");
        assertThat(flight.inFlight()).isZero();
    }

    /** 수명이 있는 값이 아니므로 오래 걸리는 것도 그대로 기다린다. */
    @Test
    @DisplayName("느린_뒷단도_모아서_한_번만_부른다")
    void 느린_뒷단도_모아서_한_번만_부른다() {
        AtomicInteger 호출 = new AtomicInteger();

        StepVerifier.withVirtualTime(() -> {
            Mono<String> 하나 = flight.join("k", () -> Mono.delay(Duration.ofSeconds(2))
                    .map(t -> "응답" + 호출.incrementAndGet()));
            Mono<String> 둘 = flight.join("k", () -> Mono.just("안 불린다"));
            return Mono.zip(하나, 둘);
        })
                .thenAwait(Duration.ofSeconds(3))
                .assertNext(t -> assertThat(t.getT1()).isEqualTo(t.getT2()))
                .verifyComplete();

        assertThat(호출).hasValue(1);
    }
}

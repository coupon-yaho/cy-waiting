package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.coupon.Tunables;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

/**
 * 운영자가 적은 값을 <b>배분 판 밖에서</b> 읽습니다 (P-1 · 6.8.2).
 *
 * <p>판 안에서 읽으면 발행이 그 왕복에 매달립니다 — 레디스가 조금 느려지는 것만
 * 으로 틱 예산을 넘겨 스냅샷이 아예 안 나가고, 전 노드가 동시에 낡음으로 빠집니다.
 */
class TunablesRefreshTest {

    private final AtomicReference<Mono<String>> 응답 = new AtomicReference<>(Mono.empty());

    /** 재료에 실려 있던 값. 승계 첫 판이 이걸 이어 싣는다. */
    private final AtomicReference<Optional<Tunables>> 재료 =
            new AtomicReference<>(Optional.empty());

    private final AtomicLong 시계 = new AtomicLong();

    private final TunablesRefresh refresh = TunablesRefresh.of(
            응답::get, 재료::get, Duration.ofMillis(200), Schedulers.parallel(), 시계::get);

    /**
     * <b>키가 없으면 안 실린 것입니다.</b> 기본값으로 채우면 그 값이 각 노드의
     * 기동 설정을 덮어써서, 운영자가 키를 만든 적도 없는데 값이 바뀝니다.
     */
    @Test
    @DisplayName("키가_없으면_안_실린_것으로_둔다")
    void 키가_없으면_안_실린_것으로_둔다() {
        refresh.refresh().block();

        assertThat(refresh.current()).isEmpty();
    }

    @Test
    @DisplayName("실려_있으면_그_값을_들고_있는다")
    void 실려_있으면_그_값을_들고_있는다() {
        응답.set(Mono.just("{\"idleCreditRatio\":0.4,\"inFlightSeconds\":7}"));

        refresh.refresh().block();

        assertThat(refresh.current()).contains(new Tunables(0.4, 7));
    }

    /**
     * <b>못 읽으면 마지막 값을 그대로 둡니다.</b> 기본값으로 되돌리면 장애가
     * 시작될 때마다 운영자가 걸어 둔 완화 조치가 사라집니다 — 그것이 필요한
     * 순간에 정확히 풀립니다.
     */
    @Test
    @DisplayName("못_읽으면_마지막_값을_지킨다")
    void 못_읽으면_마지막_값을_지킨다() {
        응답.set(Mono.just("{\"idleCreditRatio\":0.3,\"inFlightSeconds\":5}"));
        refresh.refresh().block();

        응답.set(Mono.error(new IllegalStateException("레디스가 죽었다")));
        refresh.refresh().block();

        assertThat(refresh.current()).contains(new Tunables(0.3, 5));
    }

    /**
     * <b>멎은 읽기가 배분을 멈추면 안 됩니다.</b> 오류는 잡아도 멈춤은 못 잡는
     * 구조였고, 그때 판 전체가 틱 상한에 잘려 스냅샷이 아예 안 나갔습니다.
     */
    @Test
    @DisplayName("안_끝나는_읽기도_제_시간에_끝난다")
    void 안_끝나는_읽기도_제_시간에_끝난다() {
        응답.set(Mono.just("{\"idleCreditRatio\":0.3,\"inFlightSeconds\":5}"));
        refresh.refresh().block();
        응답.set(Mono.never());

        // 예산이 실제로 200ms 인지를 가상 시계로 잰다. 실시계로 재면 CI 부하에
        // 흔들리고, 넉넉한 상한은 그 예산을 안 재는 것과 같다 (TS-4).
        StepVerifier.withVirtualTime(refresh::refresh)
                .thenAwait(Duration.ofMillis(200))
                .verifyComplete();

        assertThat(refresh.current()).contains(new Tunables(0.3, 5));
    }

    /**
     * <b>승계 첫 판이 위험합니다.</b> 새 리더의 캐시는 비어 있는데, 그 상태로
     * 발행하면 앞 리더가 싣던 값이 지워집니다.
     */
    @Test
    @DisplayName("한_번도_못_읽었으면_재료에_있던_것을_이어_싣는다")
    void 한_번도_못_읽었으면_재료에_있던_것을_이어_싣는다() {
        재료.set(Optional.of(new Tunables(0.25, 6)));
        응답.set(Mono.error(new IllegalStateException("승계 직후 레디스가 흔들린다")));

        refresh.refresh().block();

        assertThat(refresh.current()).contains(new Tunables(0.25, 6));
    }

    /** 한 번 읽고 나면 재료가 아니라 읽은 값을 쓴다. 안 그러면 갱신이 안 보인다. */
    @Test
    @DisplayName("한_번_읽고_나면_재료를_안_본다")
    void 한_번_읽고_나면_재료를_안_본다() {
        재료.set(Optional.of(new Tunables(0.25, 6)));
        응답.set(Mono.empty());

        refresh.refresh().block();

        assertThat(refresh.current()).isEmpty();
    }

    /**
     * <b>게이지는 마지막 값을 계속 냅니다.</b> 못 읽고 있다는 사실은 이 값으로만
     * 드러나고, 없으면 "5분째 못 받음" 을 걸 곳이 없습니다.
     */
    @Test
    @DisplayName("못_읽는_동안_나이가_자란다")
    void 못_읽는_동안_나이가_자란다() {
        응답.set(Mono.just("{\"idleCreditRatio\":0.3,\"inFlightSeconds\":5}"));
        refresh.refresh().block();
        assertThat(refresh.staleSeconds()).isZero();

        시계.addAndGet(Duration.ofSeconds(90).toNanos());
        응답.set(Mono.error(new IllegalStateException("레디스가 죽었다")));
        refresh.refresh().block();

        assertThat(refresh.staleSeconds()).isEqualTo(90);
    }

    /**
     * <b>키를 지운 것은 되돌린다는 뜻입니다.</b> 못 읽은 것과 달리 마지막 값을
     * 지웁니다 — 안 그러면 운영자가 값을 못 걷어냅니다.
     */
    @Test
    @DisplayName("키를_지우면_안_실린_것으로_돌아간다")
    void 키를_지우면_안_실린_것으로_돌아간다() {
        응답.set(Mono.just("{\"idleCreditRatio\":0.3,\"inFlightSeconds\":5}"));
        refresh.refresh().block();

        응답.set(Mono.empty());
        refresh.refresh().block();

        assertThat(refresh.current()).isEmpty();
    }

    /** 깨진 값은 값별로 기본값이 된다. 오타 하나가 배분을 멈추면 안 된다. */
    @Test
    @DisplayName("깨진_값이_와도_안_멈춘다")
    void 깨진_값이_와도_안_멈춘다() {
        응답.set(Mono.just("{이건 JSON 이 아니다"));

        refresh.refresh().block();

        assertThat(refresh.current()).contains(Tunables.defaults());
    }
}

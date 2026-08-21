package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

/**
 * 배분은 리더 한 대만 돈다.
 *
 * <p><b>레디스가 안 되면 리더가 아니라고 답한다.</b> 모르는 것을 "맞다" 로 답하면
 * 모든 노드가 동시에 리더가 되어 배분이 노드 수만큼 돈다 — 크레딧이 그만큼
 * 부풀고, 그건 못 물린다.
 */
class LeadershipTest {

    private static final Duration LEASE = Duration.ofSeconds(2);

    private static Leadership leadership(Supplier<Mono<Boolean>> acquire) {
        return Leadership.of("node-1", LEASE, acquire, () -> Mono.empty());
    }

    @Test
    @DisplayName("획득하면_리더다")
    void 획득하면_리더다() {
        Leadership leadership = leadership(() -> Mono.just(true));

        leadership.renew().block(LEASE);

        assertThat(leadership.isLeader()).isTrue();
    }

    @Test
    @DisplayName("남이_잡고_있으면_리더가_아니다")
    void 남이_잡고_있으면_리더가_아니다() {
        Leadership leadership = leadership(() -> Mono.just(false));

        leadership.renew().block(LEASE);

        assertThat(leadership.isLeader()).isFalse();
    }

    @Test
    @DisplayName("레디스가_안_되면_리더가_아니라고_답한다")
    void 레디스가_안_되면_리더가_아니라고_답한다() {
        // **모르는 것을 맞다고 답하면 전 노드가 동시에 리더가 된다.** 배분이
        // 노드 수만큼 돌아 크레딧이 그만큼 부풀고, 이미 나간 통과는 못 물린다.
        Leadership leadership = leadership(() -> Mono.error(new IllegalStateException("끊겼다")));

        leadership.renew().block(LEASE);

        assertThat(leadership.isLeader()).isFalse();
    }

    @Test
    @DisplayName("리더였다가_실패하면_리더에서_내려온다")
    void 리더였다가_실패하면_리더에서_내려온다() {
        // 한 번 잡았다고 계속 리더로 두면, 레디스가 끊긴 사이 lease 가 만료돼
        // 다른 노드가 잡았는데도 둘 다 리더라고 믿는다.
        AtomicInteger 호출 = new AtomicInteger();
        Leadership leadership = leadership(() -> 호출.incrementAndGet() == 1
                ? Mono.just(true)
                : Mono.error(new IllegalStateException("끊겼다")));
        leadership.renew().block(LEASE);
        assertThat(leadership.isLeader()).isTrue();

        leadership.renew().block(LEASE);

        assertThat(leadership.isLeader()).isFalse();
    }

    @Test
    @DisplayName("리더였다가_빈_응답을_받으면_내려온다")
    void 리더였다가_빈_응답을_받으면_내려온다() {
        // 스크립트가 아무것도 안 돌려주는 것은 오류가 아니라 **조용한 실패**다.
        // 안 다루면 직전 상태가 그대로 남아, 락을 잃고도 리더라고 믿는다.
        //
        // **리더가 아닌 상태에서 시작하면 아무것도 못 잰다** — 처리를 지워도
        // 여전히 거짓이라 통과한다.
        AtomicInteger 호출 = new AtomicInteger();
        Leadership leadership = leadership(() -> 호출.incrementAndGet() == 1
                ? Mono.just(true)
                : Mono.empty());
        leadership.renew().block(LEASE);
        assertThat(leadership.isLeader()).isTrue();

        leadership.renew().block(LEASE);

        assertThat(leadership.isLeader()).isFalse();
    }

    @Test
    @DisplayName("해제가_취소돼도_리더에서_내려온다")
    void 해제가_취소돼도_리더에서_내려온다() {
        // 종료 중에는 구독이 뜯길 수 있다. 완료 신호에만 기대면 그때 리더로
        // 남고, 다음 리더와 겹치는 구간이 리스가 아니라 영영이 된다.
        Leadership leadership = Leadership.of("node-1", LEASE,
                () -> Mono.just(true), Mono::never);
        leadership.renew().block(LEASE);

        Disposable 구독 = leadership.release().subscribe();
        구독.dispose();

        assertThat(leadership.isLeader()).isFalse();
    }

    @Test
    @DisplayName("소유자_ID_는_기동마다_다르다")
    void 소유자_ID_는_기동마다_다르다() {
        // **재기동한 자신을 이전 소유자로 오인하면 안 된다.** 같은 ID 를 쓰면
        // 죽기 전에 잡아 둔 락을 새 프로세스가 자기 것으로 알고 연장한다.
        assertThat(Leadership.newOwnerId()).isNotEqualTo(Leadership.newOwnerId());
    }

    @Test
    @DisplayName("리더가_아니면_해제를_안_부른다")
    void 리더가_아니면_해제를_안_부른다() {
        // 남의 락을 지울 위험은 스크립트가 막지만, 애초에 안 부르는 것이 맞다.
        AtomicInteger 해제 = new AtomicInteger();
        Leadership leadership = Leadership.of("node-1", LEASE,
                () -> Mono.just(false), () -> Mono.fromRunnable(해제::incrementAndGet));
        leadership.renew().block(LEASE);

        leadership.release().block(LEASE);

        assertThat(해제.get()).isZero();
    }

    @Test
    @DisplayName("해제하면_리더에서_내려온다")
    void 해제하면_리더에서_내려온다() {
        AtomicInteger 해제 = new AtomicInteger();
        Leadership leadership = Leadership.of("node-1", LEASE,
                () -> Mono.just(true), () -> Mono.fromRunnable(해제::incrementAndGet));
        leadership.renew().block(LEASE);

        leadership.release().block(LEASE);

        assertThat(해제.get()).isEqualTo(1);
        assertThat(leadership.isLeader()).isFalse();
    }

    @Test
    @DisplayName("해제가_터져도_리더에서_내려온다")
    void 해제가_터져도_리더에서_내려온다() {
        // 못 지웠으면 lease 만료로 풀린다. 여기서 리더로 남으면 다음 리더와
        // 겹치는 구간이 lease 가 아니라 영영이 된다.
        Leadership leadership = Leadership.of("node-1", LEASE,
                () -> Mono.just(true), () -> Mono.error(new IllegalStateException("끊겼다")));
        leadership.renew().block(LEASE);

        leadership.release().block(LEASE);

        assertThat(leadership.isLeader()).isFalse();
    }

    @Test
    @DisplayName("설정이_잘못되면_기동에_실패한다")
    void 설정이_잘못되면_기동에_실패한다() {
        assertThatThrownBy(() -> Leadership.of("", LEASE, () -> Mono.just(true), Mono::empty))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ownerId");
        assertThatThrownBy(() -> Leadership.of("n", Duration.ZERO, () -> Mono.just(true), Mono::empty))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lease");
    }
}

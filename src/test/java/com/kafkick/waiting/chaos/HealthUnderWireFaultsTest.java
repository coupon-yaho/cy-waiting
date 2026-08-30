package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.control.JudgingHealth;
import com.kafkick.waiting.control.LoopAliveHealth;
import com.kafkick.waiting.control.ShutdownState;
import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.control.SnapshotRefresher;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Status;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/**
 * 회선이 흔들릴 때의 판정.
 *
 * <p>운영에서 훨씬 흔한 것은 레디스가 사라지는 것이 아니라 <b>붙어는 있는데
 * 느려지거나 중간에 끊기는</b> 쪽이다. 낡음 판정과 명령 상한이 실제로 걸리는
 * 것도 그때고, 전 노드가 동시에 겪는 것도 그때다.
 *
 * <p>여기서 받는 것을 빼면 공유 장애가 곧 전면 장애가 된다.
 */
@Tag("chaos")
class HealthUnderWireFaultsTest {

    private static final Duration WAIT = Duration.ofSeconds(15);

    /** 갱신 루프가 실제로 홀더에 넣는 모양 — 쿠폰이 있고 발행 시각이 지금이다. */
    private static final String 쿠폰 = "c1";

    private static RedisWireFaults 회선;
    private static LettuceConnectionFactory factory;
    private static ReactiveStringRedisTemplate redis;

    /** 시각을 고정한다. 나이 판정이 실행 속도에 따라 갈리면 시험이 흔들린다. */
    private final Instant 지금 = Instant.ofEpochSecond(1_700_000_000L);

    private final SnapshotHolder holder = SnapshotHolder.of(
            Duration.ofSeconds(3), Duration.ofSeconds(5),
            Clock.fixed(지금, ZoneOffset.UTC));
    private final ShutdownState shutdown = ShutdownState.create();
    private final JudgingHealth judging = JudgingHealth.of(holder, shutdown);
    private final LoopAliveHealth loopAlive = LoopAliveHealth.of(holder, shutdown);

    @BeforeAll
    static void 띄운다() {
        회선 = RedisWireFaults.시작한다();
        factory = new LettuceConnectionFactory(회선.호스트(), 회선.포트());
        factory.afterPropertiesSet();
        redis = new ReactiveStringRedisTemplate(factory);
    }

    @AfterAll
    static void 내린다() {
        factory.destroy();
        회선.close();
    }

    @AfterEach
    void 걷는다() throws IOException {
        회선.걷는다();
    }

    /**
     * **갱신 루프가 넣을 수 있는 모양으로만 만든다.** 쿠폰이 빈 스냅샷은
     * 수용 판정이 거르므로 홀더에 절대 안 들어간다 — 그걸 기준으로 재면
     * 운영에 없는 상태를 검증하게 된다.
     */
    private void 재료를_넣는다() {
        holder.replace(new GatewaySnapshot(
                Map.of(쿠폰, CouponState.idle(100)),
                GatewaySnapshot.EMPTY.meta(),
                지금));
    }

    /** 하네스가 정말 끊었는지 확인한다. 조작이 실패해도 통과하면 안 재는 시험이다. */
    private void 정말_끊겼는지_본다() {
        assertThatThrownBy(() -> redis.opsForValue().get("아무거나").block(Duration.ofSeconds(2)))
                .isInstanceOf(RuntimeException.class);
    }

    /**
     * 붙어는 있는데 느린 상태인지 확인한다.
     *
     * <p>RULE-EXCEPTION(TS-4): 주입한 지연이 실제로 걸렸는지는 벽시계로만 잴 수
     * 있다. 여기서 시각을 주입하면 하네스가 아무 일도 안 해도 통과한다.
     */
    private void 정말_느린지_본다(Duration 최소) {
        long 시작 = System.nanoTime();
        redis.opsForValue().get("아무거나").block(WAIT);
        assertThat(Duration.ofNanos(System.nanoTime() - 시작)).isGreaterThanOrEqualTo(최소);
    }

    private SnapshotRefresher refresher() {
        return SnapshotRefresher.of(holder,
                () -> redis.<String, String>opsForHash().entries("gw:snapshot")
                        .collectMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Test
    @DisplayName("회선이_끊겨도_받는_것을_유지한다")
    void 회선이_끊겨도_받는_것을_유지한다() throws IOException {
        재료를_넣는다();
        assertThat(judging.health().getStatus()).isEqualTo(Status.UP);

        회선.끊는다();
        정말_끊겼는지_본다();

        assertThat(judging.health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    @DisplayName("회선이_끊겨도_들고_있던_재료를_안_지운다")
    void 회선이_끊겨도_들고_있던_재료를_안_지운다() throws IOException {
        // 지우면 그 순간 전 노드가 판정 재료를 잃는다. 낡은 값으로 판정하는 것은
        // 유계지만 재료가 없는 것은 유계가 아니다.
        재료를_넣는다();
        Instant 들고_있던_것 = holder.current().publishedAt();

        회선.끊는다();
        정말_끊겼는지_본다();
        refresher().once().block(WAIT);

        assertThat(holder.current().publishedAt()).isEqualTo(들고_있던_것);
        assertThat(judging.health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    @DisplayName("지연이_올라도_받는_것을_유지한다")
    void 지연이_올라도_받는_것을_유지한다() throws IOException {
        // 계획서의 지연 상승 시나리오다. 여기서 전 노드가 빠지면 그게 전면 장애다.
        재료를_넣는다();

        Duration 지연 = Duration.ofSeconds(2);
        회선.느리게(지연);
        정말_느린지_본다(지연);
        refresher().once().block(WAIT);

        assertThat(judging.health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    @DisplayName("회선이_끊겨도_루프가_돌면_살아_있다")
    void 회선이_끊겨도_루프가_돌면_살아_있다() throws IOException {
        // 재기동해도 안 고쳐진다. 전 노드가 동시에 재기동하면 그 자체가 전면 장애다.
        재료를_넣는다();

        회선.끊는다();
        정말_끊겼는지_본다();
        refresher().once().block(WAIT);

        assertThat(loopAlive.health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    @DisplayName("회선이_돌아오면_재료가_다시_찬다")
    void 회선이_돌아오면_재료가_다시_찬다() throws IOException {
        // **회복이 빠지면 3단계 중 둘만 잰 것이다.** 장애 중에 버티는 것과
        // 걷힌 뒤 돌아오는 것은 다른 문제다 — 돌아오지 않으면 버틴 의미가 없다.
        redis.opsForHash().put("gw:snapshot", 쿠폰, "ADAPTIVE:IDLE:0:100:0").block(WAIT);
        redis.opsForHash().put("gw:snapshot", "#published",
                String.valueOf(지금.getEpochSecond())).block(WAIT);

        회선.끊는다();
        정말_끊겼는지_본다();
        refresher().once().block(WAIT);
        assertThat(holder.isBeforeFirstTick()).isFalse();

        회선.걷는다();
        refresher().once().block(WAIT);

        assertThat(holder.current().coupons()).containsKey(쿠폰);
        assertThat(judging.health().getStatus()).isEqualTo(Status.UP);
    }
}

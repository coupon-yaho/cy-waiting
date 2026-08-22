package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.control.JudgingHealth;
import com.kafkick.waiting.control.LoopAliveHealth;
import com.kafkick.waiting.control.ShutdownState;
import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.control.SnapshotRefresher;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
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
    private static final Duration 발행_시각 = Duration.ofSeconds(0);

    private static RedisWireFaults 회선;
    private static LettuceConnectionFactory factory;
    private static ReactiveStringRedisTemplate redis;

    private final SnapshotHolder holder = SnapshotHolder.of(
            Duration.ofSeconds(3), Duration.ofSeconds(5), Clock.systemUTC());
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

    private void 재료를_넣는다() {
        holder.replace(new GatewaySnapshot(Map.of(), GatewaySnapshot.EMPTY.meta(),
                Instant.EPOCH.plus(발행_시각).plusSeconds(1_700_000_000L)));
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

        // **하네스가 정말 끊었는지 먼저 본다.** 조작이 실패해도 시험이 통과하면
        // 아무것도 안 재는 시험이 초록으로 남는다.
        assertThatThrownBy(() -> redis.opsForValue().get("아무거나").block(Duration.ofSeconds(2)))
                .isInstanceOf(RuntimeException.class);

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
        refresher().once().block(WAIT);

        assertThat(holder.current().publishedAt()).isEqualTo(들고_있던_것);
        assertThat(judging.health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    @DisplayName("지연이_올라도_받는_것을_유지한다")
    void 지연이_올라도_받는_것을_유지한다() throws IOException {
        // 계획서의 지연 상승 시나리오다. 여기서 전 노드가 빠지면 그게 전면 장애다.
        재료를_넣는다();

        회선.느리게(Duration.ofSeconds(2));
        refresher().once().block(WAIT);

        assertThat(judging.health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    @DisplayName("회선이_끊겨도_루프가_돌면_살아_있다")
    void 회선이_끊겨도_루프가_돌면_살아_있다() throws IOException {
        // 재기동해도 안 고쳐진다. 전 노드가 동시에 재기동하면 그 자체가 전면 장애다.
        재료를_넣는다();

        회선.끊는다();
        refresher().once().block(WAIT);

        assertThat(loopAlive.health().getStatus()).isEqualTo(Status.UP);
    }
}

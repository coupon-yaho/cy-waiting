package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 리더를 죽인다 (4.0.1) — 락 TTL 만료 전에 프로세스가 사라진 상태를 만든다.
 *
 * <p>곱게 내리면 해제 절차를 밟아 락이 즉시 풀린다. 그건 <b>장애가 아니라
 * 종료</b>다 — 승계가 lease 만료를 기다리는 경로를 못 본다.
 */
@Tag("chaos")
class LeaderFaultsTest {

    private static final Duration LEASE = Duration.ofSeconds(30);

    private RedisFaults redis;
    private StatefulRedisConnection<String, String> connection;
    private LeaderFaults leader;

    @BeforeEach
    void 준비() {
        redis = RedisFaults.시작한다();
        connection = redis.연결한다();
        leader = LeaderFaults.of(connection);
    }

    @AfterEach
    void 정리() {
        connection.close();
        redis.close();
    }

    @Test
    @DisplayName("죽여도_lease가_남아_다른_노드가_바로_못_잡는다")
    void 죽여도_lease가_남아_다른_노드가_바로_못_잡는다() {
        assertThat(leader.리더로_만든다("node-1", LEASE)).isTrue();

        leader.프로세스를_죽인다("node-1");

        // 해제가 아니라 죽음이다 — 락은 그대로 남아 있어야 한다.
        assertThat(leader.현재_소유자()).isEqualTo("node-1");
        // 양수인지만 보면 1ms 남은 것과 30초 남은 것이 같아진다.
        assertThat(leader.남은_lease()).isBetween(LEASE.minusSeconds(5), LEASE);
    }

    @Test
    @DisplayName("이미_잡힌_락은_뺏지_않는다")
    void 이미_잡힌_락은_뺏지_않는다() {
        // 덮어쓰면 살아 있는 남의 락이 주인을 바꾼다. 실제 획득 스크립트는
        // 그러지 않으므로 픽스처가 프로덕션에 없는 상태를 만드는 셈이다.
        leader.리더로_만든다("node-1", LEASE);

        assertThat(leader.리더로_만든다("node-2", LEASE)).isFalse();
        assertThat(leader.현재_소유자()).isEqualTo("node-1");
    }

    @Test
    @DisplayName("남의_락은_내리지_못한다")
    void 남의_락은_내리지_못한다() {
        // 이 가드가 깨져도 나머지 시험은 전부 통과한다.
        leader.리더로_만든다("node-1", LEASE);

        assertThat(leader.곱게_내린다("node-2")).isFalse();
        assertThat(leader.현재_소유자()).isEqualTo("node-1");
    }

    @Test
    @DisplayName("곱게_내리면_락이_즉시_풀린다")
    void 곱게_내리면_락이_즉시_풀린다() {
        leader.리더로_만든다("node-1", LEASE);

        assertThat(leader.곱게_내린다("node-1")).isTrue();
        assertThat(leader.현재_소유자()).isNull();
    }

    @Test
    @DisplayName("lease_만료는_지우는_것과_다르다")
    void lease_만료는_지우는_것과_다르다() {
        // 승계 경로를 재려고 실제 lease 를 기다리면 시험이 그만큼 느려진다.
        // 다만 **지우는 것과는 다르다** — 만료 임박 구간이 남아야 한다.
        assertThat(leader.리더로_만든다("node-1", LEASE)).isTrue();

        // **관측 창을 여기서 정한다.** 1밀리초로 두면 읽기 전에 멈칫하는 것만으로
        // 이미 지워져 있다 — 결함이 아니라 지연에 지는 시험이 된다.
        Duration 관측_창 = Duration.ofMillis(500);
        leader.lease를_만료시킨다(관측_창);

        // **중간 상태를 본다.** 곧바로 사라졌는지만 보면 DEL 과 구분이 안 되고,
        // 획득이 실패해도 소유자가 없으니 통과해 버린다.
        assertThat(leader.현재_소유자()).isEqualTo("node-1");
        assertThat(leader.남은_lease()).isBetween(Duration.ZERO, 관측_창);

        Awaitility.await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(20))
                .until(() -> leader.현재_소유자() == null);

        // 창이 리스보다 짧아야 만료를 재는 것이 된다. 같거나 길면 원래 리스를 잰다.
        assertThat(관측_창).isLessThan(LEASE);
    }

    @Test
    @DisplayName("빈_소유자로는_아무것도_못_한다")
    void 빈_소유자로는_아무것도_못_한다() {
        // 프로덕션이 빈 소유자를 거부하므로 픽스처도 그 상태를 만들면 안 된다.
        assertThatThrownBy(() -> leader.리더로_만든다("", LEASE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> leader.곱게_내린다(""))
                .isInstanceOf(IllegalArgumentException.class);
        // 이 경로의 검증이 지워져도 나머지 시험은 통과한다.
        assertThatThrownBy(() -> leader.프로세스를_죽인다(""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(leader.현재_소유자()).isNull();
    }
}

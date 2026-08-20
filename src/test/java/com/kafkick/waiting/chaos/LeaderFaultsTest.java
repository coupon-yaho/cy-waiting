package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
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

    private RedisFaults redis;
    private StatefulRedisConnection<String, String> connection;
    private LeaderFaults leader;

    @BeforeEach
    void 준비() {
        redis = RedisFaults.시작한다();
        connection = redis.연결한다();
        leader = new LeaderFaults(connection);
    }

    @AfterEach
    void 정리() {
        connection.close();
        redis.close();
    }

    @Test
    @DisplayName("죽여도_lease가_남아_다른_노드가_바로_못_잡는다")
    void 죽여도_lease가_남아_다른_노드가_바로_못_잡는다() {
        leader.리더로_만든다("node-1", Duration.ofSeconds(30));

        leader.프로세스를_죽인다("node-1");

        // 해제가 아니라 죽음이다 — 락은 그대로 남아 있어야 한다.
        assertThat(leader.현재_소유자()).isEqualTo("node-1");
        assertThat(leader.남은_lease()).isGreaterThan(Duration.ZERO);
    }

    @Test
    @DisplayName("곱게_내리면_락이_즉시_풀린다")
    void 곱게_내리면_락이_즉시_풀린다() {
        leader.리더로_만든다("node-1", Duration.ofSeconds(30));

        leader.곱게_내린다("node-1");

        assertThat(leader.현재_소유자()).isNull();
    }

    @Test
    @DisplayName("lease를_지금_만료시킬_수_있다")
    void lease를_지금_만료시킬_수_있다() {
        // 승계 경로를 재려고 실제 lease 를 기다리면 시험이 그만큼 느려진다.
        leader.리더로_만든다("node-1", Duration.ofSeconds(30));

        leader.lease를_만료시킨다();

        assertThat(leader.현재_소유자()).isNull();
    }
}

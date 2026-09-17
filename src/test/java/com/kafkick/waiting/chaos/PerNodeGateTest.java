package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 노드마다 다른 문으로 같은 레디스에 붙는다 (CY-862).
 *
 * <p>한쪽만 끊는 판은 이것이 없으면 못 만든다 — 프록시가 하나면 끊는 순간 전 노드가 같이 못 쓴다.
 * 비대칭 장애(리더는 정상, 동료만 못 쓴다)가 그 위에 선다.
 */
@Tag("chaos")
class PerNodeGateTest {

    private static final Duration 기다림 = Duration.ofSeconds(5);

    private static RedisWireFaults 선;

    @BeforeAll
    static void 세운다() {
        선 = RedisWireFaults.시작한다();
    }

    @AfterAll
    static void 내린다() {
        if (선 != null) {
            선.close();
        }
    }

    @Test
    @DisplayName("한_문을_끊어도_다른_문은_산다")
    void 한_문을_끊어도_다른_문은_산다() throws Exception {
        RedisWireFaults.Gate 둘째 = 선.문을_하나_더();
        // **같은 레디스다.** 다른 레디스를 띄우면 한쪽만 끊는 것이 아니라 아예 다른 저장소가 된다.
        쓴다(선.주소(), "gate", "1");
        assertThat(읽는다(둘째.주소(), "gate")).as("전제 — 두 문이 같은 곳을 본다").isEqualTo("1");

        둘째.끊는다();

        assertThat(읽는다(선.주소(), "gate")).as("안 끊은 문은 그대로").isEqualTo("1");
        assertThat(읽어진다(둘째.주소())).as("끊은 문만 막힌다").isFalse();

        둘째.걷는다();
        assertThat(읽는다(둘째.주소(), "gate")).as("걷으면 돌아온다").isEqualTo("1");
    }

    private void 쓴다(String url, String key, String value) {
        RedisClient client = RedisClient.create(url);
        try (StatefulRedisConnection<String, String> 연결 = client.connect()) {
            연결.sync().set(key, value);
        } finally {
            client.shutdown();
        }
    }

    private String 읽는다(String url, String key) {
        RedisClient client = RedisClient.create(url);
        try (StatefulRedisConnection<String, String> 연결 = client.connect()) {
            return 연결.sync().get(key);
        } finally {
            client.shutdown();
        }
    }

    /** 끊긴 문은 명령이 안 돌아온다. 붙는 것과 답이 오는 것은 다르다. */
    private boolean 읽어진다(String url) {
        RedisClient client = RedisClient.create(url);
        try (StatefulRedisConnection<String, String> 연결 = client.connect()) {
            연결.setTimeout(기다림);
            연결.sync().get("gate");
            return true;
        } catch (RuntimeException e) {
            return false;
        } finally {
            client.shutdown();
        }
    }
}

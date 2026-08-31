package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.WaitingApplication;
import com.kafkick.waiting.adapter.redis.RedisKeys;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * 하네스 자기검증 (CY-858). <b>두 노드가 서로를 못 보면</b> 승계도 스플릿
 * 브레인도 시계 스큐도 만들 수 없고, 그 위에 쌓은 시나리오는 전부 한 대짜리
 * 판을 다시 재게 된다.
 */
@Tag("chaos")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=true")
class SecondNodeTest {

    private static final Duration 기다림 = Duration.ofSeconds(60);

    /** 겹침을 지켜보는 시간. 리스(2초)보다 길어야 승계 순간이 창 안에 든다. */
    private static final Duration 리스보다_긴_관측 = Duration.ofSeconds(3);

    private static final BackendStub 뒷단 = BackendStub.항상_받는다();

    private static RedisFaults faults;

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        faults = RedisFaults.시작한다();
        registry.add("waiting.backend.uri", () -> "http://localhost:" + 뒷단.port());
        registry.add("spring.data.redis.url", faults::주소);
    }

    @Autowired
    private com.kafkick.waiting.control.Leadership 첫_노드;

    @Test
    @DisplayName("두_노드가_같은_레디스에서_서로를_센다")
    void 두_노드가_같은_레디스에서_서로를_센다() {
        try (StatefulRedisConnection<String, String> 연결 = faults.연결한다();
                SecondNode 둘째 = SecondNode.띄운다(WaitingApplication.class, faults.주소(),
                        "http://localhost:" + 뒷단.port(), true)) {
            GatewayNodes 노드 = new GatewayNodes(연결, Duration.ofSeconds(30));

            Awaitility.await().alias("두 노드가 다 하트비트를 남긴다")
                    .atMost(기다림).until(() -> 노드.살아있는_수() >= 2);

            assertThat(둘째.준비됐나()).as("두 번째 컨텍스트가 돈다").isTrue();
            assertThat(둘째.port()).as("자기 포트를 갖는다").isPositive();
            assertThat(둘째.ownerId()).as("주인 이름이 첫 노드와 갈린다")
                    .isNotEqualTo(첫_노드.ownerId());
        } finally {
            연결한다_정리();
        }
    }

    @Test
    @DisplayName("리더는_한_번에_한_노드다")
    void 리더는_한_번에_한_노드다() {
        try (SecondNode 둘째 = SecondNode.띄운다(WaitingApplication.class, faults.주소(),
                "http://localhost:" + 뒷단.port(), true)) {
            Awaitility.await().alias("둘 중 하나는 리더를 잡는다")
                    .atMost(기다림).until(() -> 첫_노드.isLeader() || 둘째.리더인가());

            // **동시에 둘 다 참이면 그것이 스플릿 브레인이다.** 정상 경로에서
            // 그 구간이 관측되면 리더 락이 제 일을 안 하는 것이다.
            //
            // 겹침을 2초 동안 계속 지켜본다. 리스가 2초라 그 안에 승계가 한 번
            // 일어날 수 있고, 겹친다면 바로 그 순간이다.
            Awaitility.await().alias("겹친 구간이 없다").during(리스보다_긴_관측)
                    .atMost(기다림)
                    .until(() -> !(첫_노드.isLeader() && 둘째.리더인가()));
        } finally {
            연결한다_정리();
        }
    }

    /** 다음 시험이 리더를 바로 잡게 락을 비운다. 리스 만료를 기다리면 느리다. */
    private void 연결한다_정리() {
        try (StatefulRedisConnection<String, String> 연결 = faults.연결한다()) {
            연결.sync().del(RedisKeys.LEADER);
        }
    }
}

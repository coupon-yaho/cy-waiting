package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.WaitingApplication;
import com.kafkick.waiting.control.ControlPlaneLifecycle;
import com.kafkick.waiting.control.Leadership;
import com.kafkick.waiting.adapter.redis.RedisKeys;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

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

    @AfterAll
    static void 내린다() {
        뒷단.close();
        if (faults != null) {
            faults.close();
        }
    }

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        faults = RedisFaults.시작한다();
        registry.add("waiting.backend.uri", () -> "http://localhost:" + 뒷단.port());
        registry.add("spring.data.redis.url", faults::주소);
    }

    @LocalServerPort
    private int port;

    @Autowired
    private Leadership 첫_노드;

    @Autowired
    private ControlPlaneLifecycle 수명;

    @Test
    @DisplayName("두_노드가_같은_레디스에서_서로를_센다")
    void 두_노드가_같은_레디스에서_서로를_센다() {
        try (StatefulRedisConnection<String, String> 연결 = faults.연결한다();
                SecondNode 둘째 = SecondNode.띄운다(WaitingApplication.class, faults.주소(),
                        "http://localhost:" + 뒷단.port(), true)) {
            GatewayNodes 노드 = new GatewayNodes(연결, Duration.ofSeconds(30));

            Awaitility.await().alias("두 노드가 다 하트비트를 남긴다")
                    .atMost(기다림).until(() -> 노드.살아있는_수() >= 2);

            assertThat(둘째.port()).as("자기 포트를 갖는다").isPositive();
            assertThat(둘째.ownerId()).as("둘째의 제어 평면이 자기 주인 이름을 갖는다")
                    .isNotBlank();
            assertThat(둘째.ownerId()).as("주인 이름이 첫 노드와 갈린다")
                    .isNotEqualTo(첫_노드.ownerId());
        } finally {
            락을_비운다();
        }
    }

    /**
     * <b>둘째가 선거에 실제로 참가하는가.</b> 하트비트는 제어 평면과 무관하게
     * 돌아서, 노드 수만 보면 락을 못 잡는 노드도 둘로 세어진다 — 그 위에 승계
     * 시나리오를 쌓으면 한 대짜리 판을 다시 재게 된다.
     */
    @Test
    @DisplayName("첫_노드의_제어_평면을_세우면_둘째가_이어받는다")
    void 첫_노드의_제어_평면을_세우면_둘째가_이어받는다() {
        try (SecondNode 둘째 = SecondNode.띄운다(WaitingApplication.class, faults.주소(),
                "http://localhost:" + 뒷단.port(), true)) {
            Awaitility.await().alias("첫 노드가 리더를 쥔다").atMost(기다림)
                    .pollInterval(Duration.ofMillis(300))
                    .until(() -> {
                        if (첫_노드.isLeader()) {
                            return true;
                        }
                        락을_비운다();
                        return false;
                    });

            수명.stop();

            Awaitility.await().alias("둘째가 이어받는다").atMost(기다림)
                    .until(둘째::리더인가);
        } finally {
            수명.start();
            락을_비운다();
        }
    }

    /**
     * 둘째를 닫아도 첫 노드가 계속 답하는가. 두 컨텍스트가 전역 자원을 나눠
     * 쓰므로, 한쪽을 닫는 것이 다른 쪽의 이벤트 루프를 같이 내리면 그 뒤의
     * 모든 시험이 원인 모를 실패를 낸다.
     */
    @Test
    @DisplayName("둘째를_닫아도_첫_노드가_계속_답한다")
    void 둘째를_닫아도_첫_노드가_계속_답한다() {
        int 닫기_전 = 발급_상태();
        try (SecondNode 둘째 = SecondNode.띄운다(WaitingApplication.class, faults.주소(),
                "http://localhost:" + 뒷단.port(), true)) {
            assertThat(둘째.port()).isPositive();
        }

        assertThat(발급_상태()).as("둘째를 닫은 뒤에도 첫 노드가 같은 답을 낸다")
                .isEqualTo(닫기_전);
        락을_비운다();
    }

    private int 발급_상태() {
        return WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(10))
                .build()
                .post().uri("/api/v1/coupons/harness-probe/issue")
                .header("X-Member-Id", String.valueOf(System.nanoTime()))
                .header("X-Member-Grade", "GOLD")
                .exchange().returnResult(Void.class).getStatus().value();
    }

    private void 락을_비운다() {
        try (StatefulRedisConnection<String, String> 연결 = faults.연결한다()) {
            연결.sync().del(RedisKeys.LEADER);
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
            락을_비운다();
        }
    }


}

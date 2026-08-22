package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/**
 * 전 스크립트를 <b>클러스터 모드에서 실제로 태운다</b> (G3.4 · RD-2).
 *
 * <p>정적 검사({@link LuaKeysDeclarationTest})는 의도를 보고 이 시험은 사실을
 * 본다. 슬롯 교차는 단독 모드에서 <b>조용히 통과하고</b> Phase 10 에서 터진다.
 */
@Tag("integration")
class ClusterModeScriptTest {

    private static final Path SCRIPTS = Path.of("src/main/resources/redis");

    /**
     * <b>슬롯 전량을 한 노드에 준다.</b> 여러 노드를 띄우면 클러스터가 내부
     * 주소를 돌려줘 컨테이너 밖에서 못 붙는다 — 그 배선을 맞추느라 정작
     * 검증 대상인 스크립트를 못 태운다.
     */
    @SuppressWarnings("resource")   // JVM 종료까지 살려 둔다
    private static final GenericContainer<?> CLUSTER =
            new GenericContainer<>(RedisContainerSupport.IMAGE)
                    .withExposedPorts(6379)
                    .withCommand("redis-server", "--cluster-enabled", "yes",
                            "--cluster-require-full-coverage", "no", "--appendonly", "no");

    private static RedisClient client;
    private static StatefulRedisConnection<String, String> connection;

    /**
     * <b>정적 초기화 블록에 두지 않는다.</b> 아래 대기가 조건을 다른 스레드에서
     * 평가하는데, 그 스레드가 초기화 중인 이 클래스를 건드리면 JVM 의 클래스
     * 초기화 락에 걸려 영영 멈춘다 — 대기는 그저 시간 초과로만 보인다.
     */
    @BeforeAll
    static void 클러스터를_세운다() {
        CLUSTER.start();
        슬롯을_전부_준다();
        클러스터가_설_때까지_기다린다();
        client = RedisClient.create(
                "redis://%s:%d".formatted(CLUSTER.getHost(), CLUSTER.getMappedPort(6379)));
        connection = client.connect();
    }

    @AfterAll
    static void 닫는다() {
        connection.close();
        client.shutdown();
    }

    /**
     * 슬롯 전량을 이 노드에 준다. 컨테이너 안의 {@code redis-cli} 로 친다 —
     * 클라이언트 API 를 거치면 무엇이 실제로 나갔는지가 한 겹 가려진다.
     */
    private static void 슬롯을_전부_준다() {
        String out = 컨테이너에서("cluster", "addslotsrange", "0", "16383");
        if (!out.contains("OK")) {
            throw new IllegalStateException("슬롯 배정 실패: " + out);
        }
    }

    /**
     * 슬롯을 받아도 상태가 {@code ok} 가 되기까지 클러스터 크론이 몇 바퀴 돈다.
     *
     * <p>못 서면 <b>마지막으로 본 상태를 붙여</b> 던진다. "안 떴다" 만 남으면
     * 슬롯이 안 붙은 것인지 크론이 늦은 것인지 가릴 수 없다.
     */
    private static void 클러스터가_설_때까지_기다린다() {
        AtomicReference<String> last = new AtomicReference<>("");
        try {
            Awaitility.await().atMost(Duration.ofSeconds(30))
                    .pollInterval(Duration.ofMillis(500))
                    .until(() -> {
                        last.set(컨테이너에서("cluster", "info"));
                        return last.get().contains("cluster_state:ok");
                    });
        } catch (ConditionTimeoutException e) {
            throw new IllegalStateException("클러스터가 서지 않았다: " + last.get(), e);
        }
    }

    private static String 컨테이너에서(String... args) {
        String[] command = new String[args.length + 1];
        command[0] = "redis-cli";
        System.arraycopy(args, 0, command, 1, args.length);
        try {
            var result = CLUSTER.execInContainer(command);
            return result.getStdout() + result.getStderr();
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("컨테이너 명령 실패", e);
        }
    }

    /** 쿠폰 하나가 쓰는 키 묶음. 해시 태그가 같아 한 슬롯에 모인다. */
    private static List<String> keysFor(String script) {
        return switch (script) {
            case "enqueue.lua" -> List.of(
                    RedisKeys.queue("c1", 1, 0),
                    RedisKeys.maxScore("c1", 1, 0),
                    RedisKeys.alive("c1", 1, 0));
            case "queue_status.lua" -> List.of(
                    RedisKeys.queue("c1", 1, 0),
                    RedisKeys.admitted("c1", 1, 0),
                    RedisKeys.alive("c1", 1, 0),
                    RedisKeys.grace("c1", 1, 0));
            case "sweep.lua" -> List.of(
                    RedisKeys.queue("c1", 1, 0),
                    RedisKeys.grace("c1", 1, 0),
                    RedisKeys.alive("c1", 1, 0));
            case "allocation_apply.lua" -> List.of(
                    RedisKeys.queue("c1", 1, 0),
                    RedisKeys.admitted("c1", 1, 0));
            case "leader_acquire.lua", "leader_release.lua" -> List.of(RedisKeys.LEADER);
            case "gateway_heartbeat.lua", "gateway_leave.lua" -> List.of(RedisKeys.INSTANCES);
            default -> throw new IllegalStateException("인자를 안 정한 스크립트: " + script);
        };
    }

    private static List<String> argsFor(String script) {
        return switch (script) {
            case "enqueue.lua" -> List.of("m1", "60", "30", "0", "1000");
            case "queue_status.lua" -> List.of("m1", "30", "1000");
            case "sweep.lua" -> List.of("10", "1000", "300", "50", "0");
            case "allocation_apply.lua" -> List.of("1");
            case "leader_acquire.lua" -> List.of("node-1", "2000");
            case "leader_release.lua" -> List.of("node-1");
            case "gateway_heartbeat.lua" -> List.of("node-1", "30");
            case "gateway_leave.lua" -> List.of("node-1");
            default -> throw new IllegalStateException("인자를 안 정한 스크립트: " + script);
        };
    }

    private static List<Path> 스크립트들() {
        try (Stream<Path> paths = Files.list(SCRIPTS)) {
            return paths.filter(p -> p.toString().endsWith(".lua")).sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    @DisplayName("모든_스크립트가_클러스터_모드에서_오류_없이_실행된다")
    void 모든_스크립트가_클러스터_모드에서_오류_없이_실행된다() throws IOException {
        RedisCommands<String, String> redis = connection.sync();
        List<String> failures = new ArrayList<>();

        List<Path> scripts = 스크립트들();
        for (Path script : scripts) {
            String name = script.getFileName().toString();
            String body = Files.readString(script, StandardCharsets.UTF_8);
            String[] keys = keysFor(name).toArray(String[]::new);
            String[] args = argsFor(name).toArray(String[]::new);
            try {
                redis.eval(body, ScriptOutputType.MULTI, keys, args);
            } catch (RuntimeException e) {
                // 무엇이 터졌는지까지 남긴다 — "실패" 만 보면 어느 것인지 모른다.
                failures.add("%s → %s".formatted(name, e.getMessage()));
            }
        }

        assertThat(failures)
                .withFailMessage("클러스터에서 실패한 스크립트 %d 건%n%s",
                        failures.size(), String.join("\n", failures))
                .isEmpty();
        // 한 건도 안 태우고 통과하면 검사가 아니다 (TS-9).
        assertThat(scripts).hasSizeGreaterThanOrEqualTo(5);
    }

    @Test
    @DisplayName("슬롯을_넘는_스크립트는_거부된다")
    void 슬롯을_넘는_스크립트는_거부된다() {
        // 통과만 하는 검사는 모든 스크립트를 통과시킨다 (TS-9).
        // 해시 태그가 다른 키 둘을 만지면 클러스터가 물어야 한다.
        String rogue = "redis.call('SET', KEYS[1], '1') "
                + "redis.call('SET', KEYS[2], '1') return 1";

        assertThatThrownBy(() -> connection.sync().eval(rogue, ScriptOutputType.INTEGER,
                new String[] {"{a}k", "{b}k"}, new String[0]))
                .hasMessageContaining("CROSSSLOT");
    }

    @Test
    @DisplayName("KEYS_밖의_키는_이_시험이_못_잡는다")
    void KEYS_밖의_키는_이_시험이_못_잡는다() {
        // **사각지대를 못 박아 둔다.** 한 노드가 전 슬롯을 가지면 선언되지
        // 않은 키도 로컬이라 클러스터가 안 문다. 그래서 정적 검사
        // (LuaKeysDeclarationTest) 를 버릴 수 없다 — 여기서 통과한다는 것이
        // 여러 노드에서 통과한다는 뜻이 아니다.
        String undeclared = "redis.call('SET', 'literal:key', '1') return 1";

        Object result = connection.sync().eval(undeclared, ScriptOutputType.INTEGER,
                new String[] {"{a}k"}, new String[0]);

        assertThat(result).isEqualTo(1L);
    }
}

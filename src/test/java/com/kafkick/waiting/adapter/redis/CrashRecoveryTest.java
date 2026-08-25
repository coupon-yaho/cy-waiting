package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Volume;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

/**
 * 잃어도 <b>줄 선 사람을 추월시키지 않는다</b> (G3.3 · C12).
 *
 * <p><b>{@code kill -9} 만으로는 아무것도 안 잃는다.</b> {@code appendfsync} 는
 * {@code fsync} 주기를 정할 뿐이라 잃으려면 커널이 죽어야 한다 — 계획 2.2절의
 * 전제와 다르다. 그래서 강제 종료(유실 0)와 전원 단절(유실 허용, 역행 0)을
 * 나눠 본다. 근거는 AIJ-0025.
 */
@Tag("chaos")
class CrashRecoveryTest {

    private static final String COUPON = "c1";
    private static final String QUEUE = RedisKeys.queue(COUPON, 1, 0);
    private static final String MAX_SCORE = RedisKeys.maxScore(COUPON, 1, 0);
    private static final String ALIVE = RedisKeys.alive(COUPON, 1, 0);
    private static final String ADMITTED = RedisKeys.admitted(COUPON, 1, 0);
    private static final String GRACE = RedisKeys.grace(COUPON, 1, 0);

    /**
     * <b>먼저 확실히 남길 사람들.</b> {@code appendfsync always} 로 넣어
     * 강제 종료해도 살아남는 것이 보장된다 — 살아남은 사람이 없으면 단조를
     * 검증할 대상 자체가 없어 시험이 헛돈다.
     */
    private static final int DURABLE = 50;

    /**
     * <b>잃힐 사람들.</b> {@code everysec} 로 돌린 뒤 곧바로 끊으므로 아직
     * 디스크에 안 내려간 구간이다. 이걸 안 만들면 {@code NOT_QUEUED} 경로가
     * 한 번도 안 돌고, 그래도 시험은 통과한다.
     */
    private static final int VOLATILE = 500;

    private static final int AFTER_CRASH = 20;

    private static final long NOW = 1_800_000_000L;

    private final List<GenericContainer<?>> 띄운것 = new ArrayList<>();
    private final List<RedisClient> 클라이언트 = new ArrayList<>();
    private final List<StatefulRedisConnection<String, String>> 연결 = new ArrayList<>();
    /**
     * <b>호스트 바인드 마운트를 안 쓴다.</b> redis 엔트리포인트가 {@code /data} 를
     * {@code chown} 해 버려서 호스트 사용자가 디렉터리를 열지도 못하게 된다 —
     * 시험이 남긴 AOF 가 임시 저장소에 그대로 쌓인다. 이름 있는 볼륨은 도커가
     * 지운다.
     */
    private String volume;

    /**
     * <b>연결 → 클라이언트 → 컨테이너 순으로 닫는다.</b> {@link RedisClient} 는
     * 제 Netty 이벤트 루프를 만들어서, {@code shutdown()} 없이는 그 스레드가
     * JVM 종료까지 남는다. 같은 워커에서 여러 시험이 돌면 계속 쌓인다.
     */
    @AfterEach
    void 정리() {
        연결.forEach(StatefulRedisConnection::close);
        클라이언트.forEach(RedisClient::shutdown);
        띄운것.forEach(GenericContainer::stop);
        연결.clear();
        클라이언트.clear();
        띄운것.clear();
        // 컨테이너를 내려도 볼륨은 남는다. 안 지우면 돌릴 때마다 쌓인다.
        if (volume != null) {
            DockerClientFactory.instance().client().removeVolumeCmd(volume).exec();
            volume = null;
        }
    }

    /** 같은 데이터 디렉터리를 물린 레디스. 컨테이너가 바뀌어도 AOF 는 남는다. */
    private StatefulRedisConnection<String, String> 레디스를_띄운다() {
        GenericContainer<?> container = new GenericContainer<>(RedisContainerSupport.IMAGE)
                .withExposedPorts(6379)
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                        .withBinds(new Bind(volume, new Volume("/data"))))
                .withCommand("redis-server", "--appendonly", "yes",
                        "--appendfsync", "everysec", "--dir", "/data");
        container.start();
        띄운것.add(container);
        RedisClient client = RedisClient.create(
                "redis://%s:%d".formatted(container.getHost(), container.getMappedPort(6379)));
        클라이언트.add(client);
        StatefulRedisConnection<String, String> connection = client.connect();
        연결.add(connection);
        return connection;
    }

    private void 강제종료한다(GenericContainer<?> container) {
        // stop() 은 곱게 내려 종료 절차를 다 밟는다. 그 경로가 아니라
        // 끊긴 경로를 봐야 하므로 SIGKILL 로 끊는다.
        container.getDockerClient()
                .killContainerCmd(container.getContainerId())
                .withSignal("KILL")
                .exec();
    }


    /**
     * 전원이 끊긴 상태를 재현한다 — 아직 {@code fsync} 안 된 AOF 꼬리가 날아간다.
     *
     * <p>파일이 {@code redis} 소유 0700 이라 밖에서 못 건드린다. 같은 볼륨을
     * 물린 컨테이너를 root 로 띄워 자른다.
     */
    private void AOF_꼬리를_자른다(double 남길비율) {
        try (GenericContainer<?> helper = new GenericContainer<>(RedisContainerSupport.IMAGE)
                .withCreateContainerCmdModifier(cmd -> {
                    cmd.withUser("root");
                    cmd.getHostConfig().withBinds(new Bind(volume, new Volume("/data")));
                })
                .withCommand("tail", "-f", "/dev/null")) {
            helper.start();
            // AOF 재작성이 일어나면 incr 파일이 여럿이다. 그때 아래 명령들은
            // 엉뚱한 파일을 대상으로 삼거나 조용히 실패한다.
            List<String> files = 도구로(helper, "ls /data/appendonlydir/*.incr.aof")
                    .lines().filter(line -> !line.isBlank()).toList();
            assertThat(files)
                    .withFailMessage("incr AOF 가 하나가 아니다: %s", files)
                    .hasSize(1);
            String path = files.get(0);
            long size = Long.parseLong(도구로(helper, "wc -c < " + path).trim());
            도구로(helper, "truncate -s %d %s".formatted((long) (size * 남길비율), path));
        }
    }

    private static String 도구로(GenericContainer<?> container, String command) {
        try {
            var result = container.execInContainer("sh", "-c", command);
            if (result.getExitCode() != 0) {
                throw new IllegalStateException("명령 실패: " + command + " → " + result.getStderr());
            }
            return result.getStdout();
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("명령 실패: " + command, e);
        }
    }

    /** score 는 문자열로 온다 — Lua 수는 2^53 위에서 정밀도를 잃는다. */
    private static long score(Object raw) {
        return Long.parseLong(raw.toString());
    }

    @SuppressWarnings("unchecked")
    private static List<Object> 등록한다(
            StatefulRedisConnection<String, String> redis, String member, long nowSec) {
        return (List<Object>) redis.sync().eval(LuaScripts.of("enqueue.lua"), ScriptOutputType.MULTI,
                new String[] {QUEUE, MAX_SCORE, ALIVE, ADMITTED},
                // 상한 없음. 0 은 이 뜻이 아니다 — 0 은 한 명도 안 받는다는 뜻이다.
                member, "86400", "30", "-1", String.valueOf(nowSec));
    }

    @SuppressWarnings("unchecked")
    private static List<Object> 조회한다(
            StatefulRedisConnection<String, String> redis, String member, long nowSec) {
        return (List<Object>) redis.sync().eval(LuaScripts.of("queue_status.lua"),
                ScriptOutputType.MULTI,
                new String[] {QUEUE, ADMITTED, ALIVE, GRACE},
                member, "30", String.valueOf(nowSec));
    }

    /**
     * 등록한 사람들. 앞 {@link #DURABLE} 명은 반드시 살아남고, 뒤
     * {@link #VOLATILE} 명은 잃힐 수 있다.
     */
    private Map<String, Long> 등록한다(StatefulRedisConnection<String, String> redis) {
        Map<String, Long> 등록한score = new LinkedHashMap<>();
        // 운영은 everysec 이다 (docker/redis.conf). 여기서만 잠깐 always 로
        // 돌리는 것은 **살아남는 쪽을 결정적으로 만들기 위한 픽스처**다.
        redis.sync().configSet("appendfsync", "always");
        for (int i = 0; i < DURABLE; i++) {
            등록한score.put("keep" + i, score(등록한다(redis, "keep" + i, NOW).get(0)));
        }
        redis.sync().configSet("appendfsync", "everysec");
        for (int i = 0; i < VOLATILE; i++) {
            등록한score.put("lose" + i, score(등록한다(redis, "lose" + i, NOW).get(0)));
        }
        return 등록한score;
    }

    @Test
    @DisplayName("kill_9_후_재기동해도_순서가_역행하지_않는다")
    void kill_9_후_재기동해도_순서가_역행하지_않는다() {
        volume = "cy-crash-" + UUID.randomUUID();
        DockerClientFactory.instance().client().createVolumeCmd().withName(volume).exec();

        var before = 레디스를_띄운다();
        등록한다(before);
        강제종료한다(띄운것.get(띄운것.size() - 1));

        var after = 레디스를_띄운다();
        List<String> 살아남은 = after.sync().zrange(QUEUE, 0, -1);
        // **강제 종료만으로는 아무도 안 잃는다.** 하나라도 비면 write() 된
        // 것이 사라졌다는 뜻이고, 그건 전제가 무너진 것이다.
        assertThat(살아남은).hasSize(DURABLE + VOLATILE);

        long 살아남은최대 = 살아남은.stream()
                .mapToLong(m -> after.sync().zscore(QUEUE, m).longValue())
                .max()
                .orElseThrow();

        for (int i = 0; i < AFTER_CRASH; i++) {
            long score = score(등록한다(after, "post" + i, NOW).get(0));
            assertThat(score)
                    .withFailMessage("재기동 후 등록이 살아남은 사람을 추월했다: %d ≤ %d",
                            score, 살아남은최대)
                    .isGreaterThan(살아남은최대);
            살아남은최대 = score;
        }
    }

    @Test
    @DisplayName("전원이_끊겨_유실된_사람은_NOT_QUEUED를_받는다")
    void 전원이_끊겨_유실된_사람은_NOT_QUEUED를_받는다() {
        volume = "cy-crash-" + UUID.randomUUID();
        DockerClientFactory.instance().client().createVolumeCmd().withName(volume).exec();

        var before = 레디스를_띄운다();
        Map<String, Long> 등록한score = 등록한다(before);
        강제종료한다(띄운것.get(띄운것.size() - 1));
        AOF_꼬리를_자른다(0.5);

        var after = 레디스를_띄운다();
        List<String> 이상한결과 = new ArrayList<>();
        int 잃음수 = 0;

        for (var entry : 등록한score.entrySet()) {
            List<Object> status = 조회한다(after, entry.getKey(), NOW);
            String state = status.get(0).toString();
            long score = score(status.get(2));

            // 둘 중 하나여야 한다. 남았으면 원래 score 그대로, 잃었으면
            // NOT_QUEUED. **조용히 다른 자리에 서 있는 경우가 없어야 한다.**
            boolean 남음 = "WAITING".equals(state) && score == entry.getValue();
            boolean 잃음 = "NOT_QUEUED".equals(state);
            if (잃음) {
                잃음수++;
            } else if (!남음) {
                이상한결과.add("%s → %s score=%d (등록시 %d)"
                        .formatted(entry.getKey(), state, score, entry.getValue()));
            }
        }

        assertThat(이상한결과)
                .withFailMessage("남지도 잃지도 않은 항목 %d 건%n%s",
                        이상한결과.size(), String.join("\n", 이상한결과))
                .isEmpty();
        // **몇 명 잃었는지가 중요하다.** 550 명 중 하나만 잃어도 통과하면
        // "꼬리 절단이 거의 안 먹혔다" 와 "의도대로 대량 유실됐다" 를 못 가린다.
        // 정확한 수는 비결정적이므로 의미 있는 하한만 둔다.
        assertThat(잃음수)
                .withFailMessage("유실이 %d 명뿐이다 — 꼬리 절단이 의도한 구간에 안 닿았다", 잃음수)
                .isGreaterThan(VOLATILE / 4);
        // **절단 지점이 의도한 구간인지 본다.** fsync 된 앞쪽까지 잘렸다면
        // 유실 수만 보고는 "많이 잘렸다" 로 읽혀 구분이 안 된다.
        assertThat(after.sync().zrange(QUEUE, 0, -1))
                .withFailMessage("확실히 남겼어야 할 구간까지 잘렸다")
                .contains("keep0", "keep" + (DURABLE - 1))
                .hasSizeGreaterThanOrEqualTo(DURABLE);
    }
}

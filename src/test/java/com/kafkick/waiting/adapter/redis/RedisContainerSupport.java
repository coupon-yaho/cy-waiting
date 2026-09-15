package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * 통합 시험용 레디스. <b>JVM 당 하나만 띄운다.</b>
 *
 * <p>클래스마다 띄우면 전체 시험이 분 단위로 늘어난다. 컨테이너를 정적으로 두고
 * 종료를 JVM 에 맡기면 재사용된다 — Testcontainers 의 ryuk 이 회수한다.
 */
public abstract class RedisContainerSupport {

    /**
     * 7.x 이상이어야 한다.
     *
     * <p>Lua 의 {@code TIME} 과 효과 기반 복제(5+), {@code ZRANDMEMBER}(6.2+) 를 쓴다.
     * 낮은 버전에서는 스크립트가 조용히 다르게 동작한다.
     */
    static final DockerImageName IMAGE = DockerImageName.parse("redis:7.4-alpine");

    /**
     * <b>운영과 같은 설정으로 띄운다.</b> 기본 설정으로 띄우면 파일만 검사하는
     * 테스트가 되고, 실제로 도는 레디스의 정책은 아무도 안 본다.
     */
    @SuppressWarnings("resource")   // JVM 종료까지 살려 둔다 — 재사용이 목적이다
    static final GenericContainer<?> REDIS = new GenericContainer<>(IMAGE)
            .withExposedPorts(6379)
            .withCopyFileToContainer(
                    MountableFile.forHostPath("docker/redis.conf"), "/etc/redis/redis.conf")
            .withCommand("redis-server", "/etc/redis/redis.conf");

    static {
        REDIS.start();
    }

    /** 상한을 건 채 돌릴 본문. */
    interface Body {
        void run() throws Exception;
    }

    /**
     * 사용량 아래로 상한을 내린 채 돌리고 되돌린다. <b>되돌린 값을 다시 읽어 확인한다</b> — 컨테이너를 JVM 안의
     * 다른 시험과 나눠 써, 복구가 조용히 실패하면 뒤 시험이 전부 엉뚱한 원인으로 깨진다.
     */
    static void 메모리_상한에서(Body body) throws Exception {
        String 원래 = 설정을_읽는다("maxmemory");
        assertThat(원래).as("전제 — 원래 상한을 읽었다").matches("\\d+");
        REDIS.execInContainer("redis-cli", "CONFIG", "SET", "maxmemory", "1");
        try {
            assertThat(메모리가_막혔다()).as("전제 — 쓰기가 거부되는 상태다").isTrue();
            body.run();
        } finally {
            REDIS.execInContainer("redis-cli", "CONFIG", "SET", "maxmemory", 원래);
            REDIS.execInContainer("redis-cli", "DEL", "test:oom-probe");
            assertThat(설정을_읽는다("maxmemory")).as("상한을 되돌렸다").isEqualTo(원래);
        }
    }

    static String 설정을_읽는다(String 이름) throws IOException, InterruptedException {
        String[] 줄 = REDIS.execInContainer("redis-cli", "CONFIG", "GET", 이름).getStdout().trim().split("\n");
        return 줄[줄.length - 1].trim();
    }

    /** 상한이 안 먹었으면 시험이 거짓 초록이다. 스크립트 밖 쓰기로 거부부터 확인한다. */
    static boolean 메모리가_막혔다() throws IOException, InterruptedException {
        return REDIS.execInContainer("redis-cli", "SET", "test:oom-probe", "x").getStdout().contains("OOM");
    }

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }
}

package com.kafkick.waiting.adapter.redis;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

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
     * 낮은 판에서는 스크립트가 조용히 다르게 동작한다.
     */
    static final DockerImageName IMAGE = DockerImageName.parse("redis:7.4-alpine");

    @SuppressWarnings("resource")   // JVM 종료까지 살려 둔다 — 재사용이 목적이다
    static final GenericContainer<?> REDIS = new GenericContainer<>(IMAGE).withExposedPorts(6379);

    static {
        REDIS.start();
    }

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }
}

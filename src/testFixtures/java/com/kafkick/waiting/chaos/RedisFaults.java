package com.kafkick.waiting.chaos;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * 레디스를 끊었다 붙인다 — {@code fetchStale} 과 {@code dataStale} 을 만든다.
 *
 * <p><b>주소를 고정한다.</b> 다시 띄울 때 포트가 바뀌면 붙어 있던 클라이언트가
 * 재연결로 회복되지 못해, 회복 시험이 회복이 아니라 재배선을 재게 된다.
 */
public final class RedisFaults implements AutoCloseable {

    private static final DockerImageName IMAGE = DockerImageName.parse("redis:7.4-alpine");

    private final GenericContainer<?> container;
    private final List<RedisClient> clients = new ArrayList<>();

    private RedisFaults(GenericContainer<?> container) {
        this.container = container;
    }

    /** 컨테이너를 띄운다. 호스트 포트를 고정해 끊었다 붙여도 주소가 유지된다. */
    @SuppressWarnings("resource")   // close() 가 닫는다
    public static RedisFaults 시작한다() {
        GenericContainer<?> container = new GenericContainer<>(IMAGE)
                .withExposedPorts(6379)
                .withCommand("redis-server", "--appendonly", "no");
        container.setPortBindings(List.of(자유포트() + ":6379"));
        container.start();
        return new RedisFaults(container);
    }

    /**
     * 비어 있는 포트를 하나 고른다.
     *
     * <p>고른 뒤 도커가 잡기까지 사이가 비어 경합이 가능하다. 대안은 무작위
     * 포트를 쓰는 것인데 그러면 주소가 바뀌어 이 클래스의 목적이 사라진다.
     */
    private static int 자유포트() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("빈 포트를 못 찾았다", e);
        }
    }

    public String 주소() {
        return "redis://%s:%d".formatted(container.getHost(), container.getFirstMappedPort());
    }

    /** 새 연결. 끊긴 상태에서 부르면 예외가 난다 — 그것이 이 픽스처의 쓰임이다. */
    public StatefulRedisConnection<String, String> 연결한다() {
        RedisClient client = RedisClient.create(주소());
        clients.add(client);
        return client.connect();
    }

    /** 프로세스를 끊는다. 곱게 내리지 않는다 — 장애는 절차를 밟지 않는다. */
    public void 끊는다() {
        container.getDockerClient()
                .killContainerCmd(container.getContainerId())
                .withSignal("KILL")
                .exec();
    }

    public void 붙인다() {
        container.getDockerClient().startContainerCmd(container.getContainerId()).exec();
        준비될_때까지_기다린다();
    }

    private void 준비될_때까지_기다린다() {
        IllegalStateException 마지막 = null;
        for (int i = 0; i < 60; i++) {
            try (StatefulRedisConnection<String, String> probe = 연결한다()) {
                if ("PONG".equals(probe.sync().ping())) {
                    return;
                }
            } catch (RuntimeException e) {
                마지막 = new IllegalStateException("아직 안 떴다", e);
            }
        }
        throw 마지막 != null ? 마지막 : new IllegalStateException("레디스가 돌아오지 않았다");
    }

    @Override
    public void close() {
        clients.forEach(RedisClient::shutdown);
        clients.clear();
        container.stop();
    }
}

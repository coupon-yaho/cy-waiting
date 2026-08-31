package com.kafkick.waiting.chaos;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
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

    /**
     * 컨테이너를 띄운다. 호스트 포트를 고정해 끊었다 붙여도 주소가 유지된다.
     *
     * <p><b>동적 매핑은 다시 켤 때 포트가 바뀐다</b>(실측). 주소가 바뀌면
     * 회복 시험이 회복이 아니라 재배선을 재게 된다. 포트를 고르고 도커가
     * 잡기까지의 경합은 없앨 수 없어 몇 번 다시 고른다.
     */
    public static RedisFaults 시작한다() {
        return 시작한다(false);
    }

    /**
     * 영속을 켜고 띄운다. <b>everysec 이라 최대 1초 분량이 증발한다</b>(E-6) —
     * 그 부분 유실이 C12 의 전제다. 영속이 없으면 통째로 날아가 "일부만 증발" 을
     * 못 만들고, 남은 대기자를 재등록자가 추월하는지가 아예 안 재진다.
     */
    public static RedisFaults 영속으로_시작한다() {
        return 시작한다(true);
    }

    private static RedisFaults 시작한다(boolean 영속) {
        RuntimeException 마지막 = null;
        for (int i = 0; i < 5; i++) {
            try {
                return 한번_띄운다(영속);
            } catch (RuntimeException e) {
                // **포트 충돌만 다시 고른다.** 전부 재시도하면 도커 부재나
                // 이미지 실패까지 다섯 번 반복한 뒤 "포트를 뺏겼다" 로
                // 보고돼, 진짜 원인이 그 문구 뒤에 묻힌다.
                if (!포트_충돌인가(e)) {
                    throw e;
                }
                마지막 = e;
            }
        }
        throw new IllegalStateException("빈 포트를 다섯 번 놓쳤다", 마지막);
    }

    private static boolean 포트_충돌인가(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            String message = t.getMessage();
            if (message != null && (message.contains("port is already allocated")
                    || message.contains("address already in use")
                    || message.contains("Address already in use")
                    || message.contains("bind: address already in use"))) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("resource")   // close() 가 닫는다
    private static RedisFaults 한번_띄운다(boolean 영속) {
        // **컨테이너를 지우지 않고 다시 켠다.** 죽였다 붙이는 것이 같은
        // 컨테이너라 파일시스템이 남고, 그래서 볼륨 없이도 AOF 가 살아난다.
        GenericContainer<?> container = new GenericContainer<>(IMAGE)
                .withExposedPorts(6379)
                .withCommand(영속
                        ? new String[] {"redis-server", "--appendonly", "yes",
                                "--appendfsync", "everysec"}
                        : new String[] {"redis-server", "--appendonly", "no"});
        container.setPortBindings(List.of(자유포트() + ":6379"));
        container.start();
        return new RedisFaults(container);
    }

    /** 비어 있는 포트를 하나 고른다. 잡히기까지의 경합은 위에서 재시도로 흡수한다. */
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

    /**
     * 다시 뜰 때까지 기다린다.
     *
     * <p>프로브는 <b>제 클라이언트를 매번 닫는다.</b> 목록에 쌓으면 실패한
     * 시도마다 Netty 이벤트 루프가 남아, 회복을 기다리는 동안 스레드가 는다.
     * 시도 사이에 대기도 둔다 — 연결 거부가 즉시 돌아오는 환경에서는 대기가
     * 없으면 컨테이너가 뜨기 전에 횟수를 다 써 버린다.
     */
    private void 준비될_때까지_기다린다() {
        RuntimeException 마지막 = null;
        for (int i = 0; i < 60; i++) {
            RedisClient probe = RedisClient.create(주소());
            try (StatefulRedisConnection<String, String> connection = probe.connect()) {
                if ("PONG".equals(connection.sync().ping())) {
                    return;
                }
            } catch (RuntimeException e) {
                마지막 = e;
            } finally {
                probe.shutdown();
            }
            잠깐_쉰다();
        }
        throw new IllegalStateException("레디스가 돌아오지 않았다", 마지막);
    }

    private static void 잠깐_쉰다() {
        try {
            TimeUnit.MILLISECONDS.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("대기 중 끊겼다", e);
        }
    }

    @Override
    public void close() {
        clients.forEach(RedisClient::shutdown);
        clients.clear();
        container.stop();
    }
}

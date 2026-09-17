package com.kafkick.waiting.chaos;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.toxiproxy.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * 레디스 <b>회선</b>에 장애를 넣는다.
 *
 * <p>운영에서 흔한 것은 레디스가 사라지는 것이 아니라 붙어는 있는데 느려지거나
 * 끊기는 쪽이다. 앱 코드를 안 건드린다 — 주소만 프록시로 준다.
 */
public final class RedisWireFaults implements AutoCloseable {

    private static final DockerImageName REDIS_IMAGE = DockerImageName.parse("redis:7.4-alpine");
    private static final DockerImageName PROXY_IMAGE =
            DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.12.0")
                    .asCompatibleSubstituteFor("shopify/toxiproxy");

    private static final String 지연 = "지연";
    private static final String 끊김 = "끊김";

    private final Network network;
    private final GenericContainer<?> redis;
    private final ToxiproxyContainer toxiproxy;
    private final Proxy proxy;

    /** 문마다 다른 듣는 포트. 8666 은 첫 문이 쓴다. */
    private final AtomicInteger 다음_포트 = new AtomicInteger(8667);

    private RedisWireFaults(Network network, GenericContainer<?> redis,
            ToxiproxyContainer toxiproxy, Proxy proxy) {
        this.network = network;
        this.redis = redis;
        this.toxiproxy = toxiproxy;
        this.proxy = proxy;
    }

    /** 운영과 같은 설정의 레디스를 프록시 뒤에 세운다. */
    public static RedisWireFaults 시작한다() {
        Network network = Network.newNetwork();
        GenericContainer<?> redis = new GenericContainer<>(REDIS_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("redis")
                .withExposedPorts(6379)
                .withCopyFileToContainer(
                        MountableFile.forHostPath("docker/redis.conf"), "/etc/redis/redis.conf")
                .withCommand("redis-server", "/etc/redis/redis.conf");
        redis.start();

        ToxiproxyContainer toxiproxy = new ToxiproxyContainer(PROXY_IMAGE).withNetwork(network);
        toxiproxy.start();
        try {
            Proxy proxy = new eu.rekawek.toxiproxy.ToxiproxyClient(
                    toxiproxy.getHost(), toxiproxy.getControlPort())
                    .createProxy("redis", "0.0.0.0:8666", "redis:6379");
            return new RedisWireFaults(network, redis, toxiproxy, proxy);
        } catch (IOException e) {
            toxiproxy.stop();
            redis.stop();
            network.close();
            throw new IllegalStateException("프록시를 못 세웠다", e);
        }
    }

    /** 시험이 직접 칠 연결. 프록시를 지나므로 앱과 같은 길을 본다. */
    public StatefulRedisConnection<String, String> 연결한다() {
        return RedisClient.create(주소()).connect();
    }

    /** 앱이 붙을 주소. 문 하나를 통째로 넘길 때 쓴다. */
    public String 주소() {
        return "redis://%s:%d".formatted(호스트(), 포트());
    }

    /**
     * 같은 레디스로 가는 문을 하나 더 연다 (CY-862). <b>노드마다 다른 문을 주면 한쪽만 끊을 수 있다</b> —
     * 문이 하나면 끊는 순간 전 노드가 같이 못 쓰고, 비대칭 장애를 아예 못 만든다.
     */
    public Gate 문을_하나_더() {
        int 듣는_포트 = 다음_포트.getAndIncrement();
        try {
            Proxy 새_문 = new eu.rekawek.toxiproxy.ToxiproxyClient(
                    toxiproxy.getHost(), toxiproxy.getControlPort())
                    .createProxy("redis-" + 듣는_포트, "0.0.0.0:" + 듣는_포트, "redis:6379");
            return new Gate(새_문, 호스트(), toxiproxy.getMappedPort(듣는_포트));
        } catch (IOException e) {
            throw new IllegalStateException("문을 더 못 열었다: " + 듣는_포트, e);
        }
    }

    /** 문 하나. 끊고 걷는 것이 이 문에만 걸린다. */
    public record Gate(Proxy proxy, String 호스트, int 포트) {

        public String 주소() {
            return "redis://%s:%d".formatted(호스트, 포트);
        }

        /**
         * 이 문만 끊는다. <b>양쪽을 다 막는다</b> — 내려오는 쪽만 막으면 쓰기는 그대로 닿아, 끊긴
         * 노드의 하트비트가 계속 찍힌다. 그러면 다른 노드가 그 노드를 죽은 것으로 안 본다.
         */
        public void 끊는다() throws IOException {
            proxy.toxics().timeout(끊김, ToxicDirection.DOWNSTREAM, 0);
            proxy.toxics().timeout(끊김 + "-위", ToxicDirection.UPSTREAM, 0);
        }

        /** 이 문의 장애만 걷는다. */
        public void 걷는다() throws IOException {
            for (var toxic : proxy.toxics().getAll()) {
                toxic.remove();
            }
        }
    }

    /** 앱이 붙을 주소. 레디스가 아니라 프록시다. */
    public String 호스트() {
        return toxiproxy.getHost();
    }

    public int 포트() {
        return toxiproxy.getMappedPort(8666);
    }

    /** 응답을 늦춘다. 명령 상한이 실제로 걸리는지 재는 데 쓴다. */
    public void 느리게(Duration 만큼) throws IOException {
        proxy.toxics().latency(지연, ToxicDirection.DOWNSTREAM, 만큼.toMillis());
    }

    /** 회선을 끊는다. 붙어는 있는데 아무것도 안 오는 상태다. */
    public void 끊는다() throws IOException {
        proxy.toxics().timeout(끊김, ToxicDirection.DOWNSTREAM, 0);
    }

    /**
     * 붙은 연결을 끊고 <b>새 연결은 받아만 주고 아무것도 안 보낸다.</b> 클라이언트는 곧바로 재연결을 시도하는데
     * 거부가 아니라 매달림이라, 시도마다 연결 상한까지 기다린다 — 프로세스가 죽은 판보다 회복이 늦는 쪽이다.
     */
    public void 재연결을_매단다() throws IOException {
        proxy.toxics().timeout(끊김, ToxicDirection.DOWNSTREAM, 0);
        proxy.disable();
        proxy.enable();
    }

    /**
     * 새 연결이 정말 매달리는가. <b>하네스 자기검증이다</b> — 프록시 동작이 바뀌어 거부로 떨어지면 매달림 판이
     * 조용히 재기동 판이 되고, 재연결 지연 상한이 빠져도 초록이다. 붙기는 붙고 PING 에 답이 안 와야 참이다.
     */
    public boolean 새_연결이_매달린다(Duration 기다림) {
        try (Socket socket = new Socket()) {
            // 붙는 것부터 실패하면 매달림이 아니라 거부나 유실이다.
            socket.connect(new InetSocketAddress(호스트(), 포트()), (int) 기다림.toMillis());
            socket.setSoTimeout((int) 기다림.toMillis());
            socket.getOutputStream().write("PING\r\n".getBytes(StandardCharsets.US_ASCII));
            socket.getOutputStream().flush();
            try {
                socket.getInputStream().read();
                // 답이 왔거나 끊겼다. 어느 쪽이든 매달림이 아니다.
                return false;
            } catch (SocketTimeoutException e) {
                return true;
            }
        } catch (IOException e) {
            return false;
        }
    }

    /** 넣은 장애를 전부 걷는다. */
    public void 걷는다() throws IOException {
        for (var toxic : proxy.toxics().getAll()) {
            toxic.remove();
        }
    }

    @Override
    public void close() {
        toxiproxy.stop();
        redis.stop();
        network.close();
    }
}

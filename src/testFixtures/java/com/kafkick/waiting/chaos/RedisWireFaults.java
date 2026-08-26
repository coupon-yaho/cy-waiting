package com.kafkick.waiting.chaos;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import java.io.IOException;
import java.time.Duration;
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

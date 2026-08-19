package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

/**
 * <b>도는 레디스</b>의 정책을 본다.
 *
 * <p>설정 파일만 검사하면 그 파일이 실제로 적용됐는지는 아무도 안 본다 —
 * 파일은 맞는데 컨테이너가 기본 설정으로 뜨는 일이 실제로 있었다.
 */
@Tag("integration")
class RuntimePersistenceTest extends RedisContainerSupport {

    private String configOf(String key) throws IOException, InterruptedException {
        Container.ExecResult result = REDIS.execInContainer("redis-cli", "CONFIG", "GET", key);
        String[] lines = result.getStdout().strip().split("\\R");
        // CONFIG GET 은 이름과 값을 줄로 번갈아 낸다
        return lines.length >= 2 ? lines[1].strip() : "";
    }

    @Test
    @DisplayName("도는_레디스가_초당_동기화다")
    void 도는_레디스가_초당_동기화다() throws IOException, InterruptedException {
        assertThat(configOf("appendonly")).isEqualTo("yes");
        assertThat(configOf("appendfsync")).isEqualTo("everysec");
    }

    @Test
    @DisplayName("도는_레디스가_조용히_지우지_않는다")
    void 도는_레디스가_조용히_지우지_않는다() throws IOException, InterruptedException {
        assertThat(configOf("maxmemory-policy")).isEqualTo("noeviction");
    }

    @Test
    @DisplayName("도는_레디스에_메모리_상한이_있다")
    void 도는_레디스에_메모리_상한이_있다() throws IOException, InterruptedException {
        // 0 이면 무제한이라 noeviction 이 아무것도 안 막는다.
        assertThat(Long.parseLong(configOf("maxmemory"))).isPositive();
    }

    @Test
    @DisplayName("도는_레디스가_재작성_중에도_동기화를_멈추지_않는다")
    void 도는_레디스가_재작성_중에도_동기화를_멈추지_않는다()
            throws IOException, InterruptedException {
        assertThat(configOf("no-appendfsync-on-rewrite")).isEqualTo("no");
    }

    @Test
    @DisplayName("설정_파일과_도는_값이_일치한다")
    void 설정_파일과_도는_값이_일치한다() throws IOException, InterruptedException {
        // 둘이 갈라지면 어느 쪽을 믿어야 할지 알 수 없다.
        Map<String, String> expected = Map.of(
                "appendonly", "yes",
                "appendfsync", "everysec",
                "maxmemory-policy", "noeviction",
                "no-appendfsync-on-rewrite", "no");

        for (Map.Entry<String, String> entry : expected.entrySet()) {
            assertThat(configOf(entry.getKey()))
                    .withFailMessage(
                            "%s: 파일은 %s 인데 도는 값은 %s",
                            entry.getKey(), entry.getValue(), configOf(entry.getKey()))
                    .isEqualTo(entry.getValue());
        }
    }
}

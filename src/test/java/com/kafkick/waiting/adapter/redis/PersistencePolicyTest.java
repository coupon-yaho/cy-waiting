package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 영속성 정책이 문서가 아니라 설정 파일에 있는지 본다.
 *
 * <p>이 설정이 지키려는 것은 <b>순서</b>이지 무결성이 아니다. 순번이 시계라
 * 유실이 생겨도 중복 발번이 없고, 증발한 사람은 재등록하면 새 순번이 더 커서
 * 이미 줄 선 사람을 추월하지 않는다 (E-6).
 */
class PersistencePolicyTest {

    private Map<String, String> config() throws IOException {
        return Files.readAllLines(Path.of("docker/redis.conf"), StandardCharsets.UTF_8).stream()
                .map(String::trim)
                .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                .map(line -> line.split("\\s+", 2))
                .collect(Collectors.toMap(parts -> parts[0], parts -> parts[1]));
    }

    @Test
    @DisplayName("AOF가_켜져_있고_초당_동기화다")
    void AOF가_켜져_있고_초당_동기화다() throws IOException {
        assertThat(config()).containsEntry("appendonly", "yes");
        assertThat(config()).containsEntry("appendfsync", "everysec");
    }

    @Test
    @DisplayName("always를_쓰지_않는다")
    void always를_쓰지_않는다() throws IOException {
        // 매 쓰기마다 fsync 를 치르면 스파이크에서 지연이 명령 타임아웃을 넘긴다.
        assertThat(config().get("appendfsync")).isNotEqualTo("always");
    }

    @Test
    @DisplayName("메모리가_차도_조용히_지우지_않는다")
    void 메모리가_차도_조용히_지우지_않는다() throws IOException {
        // 대기열 항목이 eviction 으로 사라지면 그 사람은 줄에서 증발한다.
        // 누가 증발할지를 메모리 정책이 정하게 두지 않는다.
        assertThat(config()).containsEntry("maxmemory-policy", "noeviction");
    }

    @Test
    @DisplayName("메모리_상한이_설정되어_있다")
    void 메모리_상한이_설정되어_있다() throws IOException {
        // 상한이 없으면 noeviction 은 아무것도 안 막는다. 상한에 닿아야
        // 쓰기를 거부하는데 기본값 0 은 무제한이라 그 지점이 안 온다 —
        // 대신 호스트가 OOM 으로 죽는다. 거부는 복구할 수 있고 OOM 은 못 한다.
        assertThat(config()).containsKey("maxmemory");
        assertThat(config().get("maxmemory")).isNotEqualTo("0");
    }

    @Test
    @DisplayName("다시_쓰는_동안에도_동기화를_멈추지_않는다")
    void 다시_쓰는_동안에도_동기화를_멈추지_않는다() throws IOException {
        // 멈추면 그 구간의 지연이 튀고, 튄 지연은 명령 타임아웃을 넘겨
        // 스케줄러를 멎게 한다.
        assertThat(config()).containsEntry("no-appendfsync-on-rewrite", "no");
    }
}

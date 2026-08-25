package com.kafkick.waiting.architecture;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 커버리지 게이트는 시험이 아니라 빌드가 잰다. <b>그 설정이 살아 있는지를 본다</b> —
 * 한 줄 지우면 게이트가 사라지는데 아무도 모른다.
 */
class CoverageGateTest {

    private static final Path BUILD = Path.of("build.gradle");

    @Test
    @DisplayName("판정_패키지에_분기_100_퍼센트가_걸려_있다")
    void 판정_패키지에_분기_100_퍼센트가_걸려_있다() throws IOException {
        String script = Files.readString(BUILD);

        // 판정이 틀리면 초과 발급과 공정성 붕괴로 곧장 간다. 분기 하나도 안 빠뜨린다.
        assertThat(script)
                .as("판정 패키지의 분기 임계가 사라졌다")
                .contains("com.kafkick.waiting.domain.*")
                .contains("BRANCH");
        assertThat(script)
                .as("임계가 1.00 이 아니다")
                .containsPattern("minimum\\s*=\\s*1\\.0");
    }
}

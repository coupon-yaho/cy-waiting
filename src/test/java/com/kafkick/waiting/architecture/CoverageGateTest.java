package com.kafkick.waiting.architecture;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 커버리지 게이트는 시험이 아니라 빌드가 잰다. <b>그 설정이 살아 있는지를 본다</b> —
 * 한 줄 지우면 게이트가 사라지는데 아무도 모른다.
 */
class CoverageGateTest {

    private static final Path BUILD = Path.of("build.gradle");

    /**
     * <b>한 규칙 안에서 본다.</b> 따로 찾으면 다른 규칙이나 주석에 같은 글자가
     * 남아 있는 것만으로 통과한다 — 정작 이 규칙을 지워도 초록이다.
     */
    @Test
    @DisplayName("판정_패키지에_분기_100_퍼센트가_걸려_있다")
    void 판정_패키지에_분기_100_퍼센트가_걸려_있다() throws IOException {
        String script = Files.readString(BUILD);

        // 판정이 틀리면 초과 발급과 공정성 붕괴로 곧장 간다. 분기 하나도 안 빠뜨린다.
        Matcher rule = Pattern.compile(
                        "rule\\s*\\{[^{]*?includes\\s*=\\s*\\[\\s*'com\\.kafkick\\.waiting\\.domain\\.\\*'"
                                + "[^}]*?\\}[^}]*?\\}", Pattern.DOTALL)
                .matcher(script);

        assertThat(rule.find()).as("판정 패키지 규칙이 사라졌다").isTrue();
        assertThat(rule.group())
                .as("그 규칙이 분기를 100%% 로 안 잡는다")
                .containsPattern("counter\\s*=\\s*'BRANCH'")
                .containsPattern("minimum\\s*=\\s*1(\\.0+)?");
    }
}

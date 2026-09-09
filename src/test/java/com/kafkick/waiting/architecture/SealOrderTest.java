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
 * 이탈자 청소를 지키는 것은 <b>순서</b>다 (CY-911). 청소에는 울타리 인자가 없고,
 * 유령이 못 걷는 것은 그것이 발행 뒤에 매달려 있어서다.
 *
 * <p>그 순서와, 승계가 발행의 문까지 잠그는 것을 글자로 못 박는다 — 배선을 지워도
 * 회차 시험은 초록이라 유령의 청소가 조용히 열린다.
 */
class SealOrderTest {

    private static final Path ROUND =
            Path.of("src/main/java/com/kafkick/waiting/control/AllocationRound.java");

    private static final Path CONFIG =
            Path.of("src/main/java/com/kafkick/waiting/control/ControlPlaneConfig.java");

    /** 발행이 나간 뒤의 꼬리. 여기 매달린 것만 울타리 뒤에 선다. */
    private static final Pattern TAIL = Pattern.compile(
            "publishRound\\(collected, granted, credit, readAt, current\\)(.*?)\\)\\)\\);",
            Pattern.DOTALL);

    @Test
    @DisplayName("정리와_청소가_발행_뒤에_매달린다")
    void 정리와_청소가_발행_뒤에_매달린다() throws IOException {
        Matcher tail = TAIL.matcher(Files.readString(ROUND));

        assertThat(tail.find()).as("발행 자리를 못 찾았다 — 이름이 바뀌었다").isTrue();
        assertThat(tail.group(1))
                .as("앞으로 옮기면 막힌 회차에도 청소가 나가 순번이 뒤로 간다")
                .contains("cleanUp(collected, granted)")
                .contains("sweepUp(collected, granted)");
    }

    @Test
    @DisplayName("승계가_발행의_문까지_잠근다")
    void 승계가_발행의_문까지_잠근다() throws IOException {
        assertThat(Files.readString(CONFIG))
                .as("안 잠그면 첫 발행 전까지 유령의 발행이 통과하고 청소가 따라 나간다")
                .contains("port.sealSnapshotFence(fence)");
    }
}

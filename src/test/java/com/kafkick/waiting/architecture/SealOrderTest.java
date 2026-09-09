package com.kafkick.waiting.architecture;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 이탈자 청소를 지키는 것은 <b>순서</b>다 (CY-911). 청소에는 울타리 인자가 없고,
 * 유령이 못 걷는 것은 그것이 발행 뒤에 매달려 있어서다.
 *
 * <p>그 순서를 글자로 못 박는다 — 앞으로 옮겨도 회차 시험 하나만 빨개지는데,
 * 그 하나를 같이 고치는 변경이면 아무 소리 없이 유령의 청소가 열린다.
 */
class SealOrderTest {

    private static final Path ROUND =
            Path.of("src/main/java/com/kafkick/waiting/control/AllocationRound.java");

    private static final String 발행 =
            "publishRound(collected, granted, credit, readAt, current)";

    /**
     * 미루는 것까지 같이 본다. {@code Mono.defer} 를 빼면 조립 시점에 유예 셈이
     * 먼저 돌아, 막힌 회차가 후보를 익힌다.
     */
    private static final String 정리 = ".then(Mono.defer(() -> cleanUp(collected, granted)))";

    private static final String 청소 = ".then(Mono.defer(() -> sweepUp(collected, granted)))";

    @Test
    @DisplayName("정리와_청소가_발행_뒤에_한_번씩_매달린다")
    void 정리와_청소가_발행_뒤에_한_번씩_매달린다() throws IOException {
        String source = Files.readString(ROUND);

        // **횟수를 먼저 본다.** 같은 글자가 둘이면 뒤엣것으로 순서 검사를 살 수 있다.
        assertThat(횟수(source, 발행)).as("발행 자리를 못 찾았다 — 이름이 바뀌었다").isEqualTo(1);
        assertThat(횟수(source, 정리)).as("미루는 정리가 한 번이어야 한다").isEqualTo(1);
        assertThat(횟수(source, 청소)).as("미루는 청소가 한 번이어야 한다").isEqualTo(1);

        assertThat(source.indexOf(정리))
                .as("정리를 앞으로 옮기면 막힌 회차가 줄을 지운다")
                .isGreaterThan(source.indexOf(발행));
        assertThat(source.indexOf(청소))
                .as("청소를 앞으로 옮기면 걷힌 사람이 새 score 로 다시 선다")
                .isGreaterThan(source.indexOf(정리));
        // **삼키면 순서가 아무것도 안 지킨다.** 막힌 발행을 빈 값으로 바꾸는 한 줄이면
        // 꼬리가 그대로 돌고, 위의 순서 단언은 그대로 초록이다.
        assertThat(source.substring(source.indexOf(발행), source.indexOf(청소)))
                .as("막힌 발행을 삼키면 유령의 정리와 청소가 그대로 나간다")
                .doesNotContain("onError");
    }

    /**
     * 검사가 실제로 순서를 본다. <b>뒤집은 글로 재 본다</b> — 안 그러면 무엇을 넣어도
     * 통과하는 검사가 통과하는 시험으로 남는다.
     */
    @Test
    @DisplayName("순서가_뒤집히면_검사가_문다")
    void 순서가_뒤집히면_검사가_문다() {
        String 뒤집힌 = 청소 + "\n" + 정리 + "\n" + 발행;

        assertThat(뒤집힌.indexOf(청소)).isLessThan(뒤집힌.indexOf(발행));
        assertThat(횟수(뒤집힌, 발행)).isEqualTo(1);
    }

    private static int 횟수(String source, String 조각) {
        int count = 0;
        for (int at = source.indexOf(조각); at >= 0; at = source.indexOf(조각, at + 1)) {
            count++;
        }
        return count;
    }
}

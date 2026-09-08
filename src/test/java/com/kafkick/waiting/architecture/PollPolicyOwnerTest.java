package com.kafkick.waiting.architecture;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 폴링 정책은 <b>한 곳에서만 만든다</b> (CY-897).
 *
 * <p>흔들림 비율을 자리마다 적으면 사본이 갈라진다. 한쪽만 고치면 같은 장애에 두
 * 안내가 나가고, 시험이 제 것을 만들면 운영의 값을 바꿔도 초록으로 남는다.
 */
// **자리 목록을 손으로 들지 않는다.** 사본을 막겠다는 시험이 자기 목록을 사본으로
// 들면 다섯 번째 자리가 생겼을 때 아무도 모른다. 소스를 훑는다.
class PollPolicyOwnerTest {

    private static final Path MAIN = Path.of("src/main/java");

    /** 이 정책이 사는 곳. 여기 말고는 새로 만들지 않는다. */
    private static final String HOME = "PollIntervalPolicy.java";

    private static final String FACTORY = "PollIntervalPolicy.of(";

    @Test
    @DisplayName("정책을_만드는_곳은_자기_집뿐이다")
    void 정책을_만드는_곳은_자기_집뿐이다() throws IOException {
        try (Stream<Path> paths = Files.walk(MAIN)) {
            List<String> 만드는_파일 = paths
                    .filter(p -> p.toString().endsWith(".java"))
                    .filter(p -> !p.getFileName().toString().equals(HOME))
                    .filter(p -> 읽는다(p).contains(FACTORY))
                    .map(p -> p.getFileName().toString())
                    .sorted()
                    .toList();

            assertThat(만드는_파일)
                    .describedAs("정책은 %s 가 낸 것을 받아 쓴다. 비율이 자리마다 갈리면 "
                            + "같은 장애에 두 안내가 나간다", HOME)
                    .isEmpty();
        }
    }

    private String 읽는다(Path path) {
        try {
            return Files.readString(path);
        } catch (IOException e) {
            throw new IllegalStateException("소스를 못 읽는다: " + path, e);
        }
    }
}

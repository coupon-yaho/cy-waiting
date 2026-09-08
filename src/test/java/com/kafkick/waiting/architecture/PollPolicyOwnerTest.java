package com.kafkick.waiting.architecture;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 폴링 정책은 <b>한 곳에서만 만든다</b> (CY-897).
 *
 * <p>비율을 자리마다 적으면 사본이 갈라진다. 한쪽만 고치면 같은 장애에 두 안내가
 * 나가고, 시험이 제 것을 만들면 운영의 값을 바꿔도 그 시험이 초록으로 남는다.
 */
class PollPolicyOwnerTest {

    // 자리 목록을 손으로 안 든다. 근거는 AIJ-0267 에 있다.
    private static final String TYPE = "PollIntervalPolicy";

    private static final List<Path> ROOTS = List.of(
            Path.of("src/main/java"), Path.of("src/test/java"),
            Path.of("src/testFixtures/java"));

    /**
     * 면제하는 자리. <b>이름이 아니라 경로로 든다</b> — 이름만 보면 다른 패키지에
     * 같은 이름을 두는 것으로 면제를 살 수 있다. 자기 집, 그 계약을 재는 시험,
     * 그리고 이 검사 자신이다.
     */
    private static final List<String> EXCUSED = List.of(
            "com/kafkick/waiting/domain/queue/" + TYPE + ".java",
            "com/kafkick/waiting/domain/queue/" + TYPE + "Test.java",
            "com/kafkick/waiting/architecture/PollPolicyOwnerTest.java");

    /**
     * 비율을 손에 들고 새로 만드는 호출. <b>값이 아니라 자리를 본다</b> — 값으로
     * 보면 리터럴과 지역 상수 경유가 그대로 빠져나간다. 공백·줄바꿈도 넘긴다.
     */
    private static final Pattern MAKES =
            Pattern.compile(Pattern.quote(TYPE) + "\\s*\\.\\s*of\\s*\\(");

    /** 받아 쓰는 이름. 이것이 안 보이면 이름이 바뀐 것이고, 검사가 헛돈 것이다. */
    private static final Pattern BORROWS =
            Pattern.compile(Pattern.quote(TYPE) + "\\s*\\.\\s*(standard|noJitter)\\s*\\(");

    /** 열려 있으면 타입 이름을 안 적고 만들 수 있어 위 검사가 통째로 샌다. */
    private static final Pattern STATIC_IMPORT =
            Pattern.compile("import\\s+static\\s+[\\w.]*" + Pattern.quote(TYPE) + "\\.");

    @Test
    @DisplayName("비율을_손에_들고_만드는_자리가_없다")
    void 비율을_손에_들고_만드는_자리가_없다() throws IOException {
        // 탐지가 도는지 먼저 본다. 이름이 바뀌면 아래 단언이 아무것도 안 잰다.
        assertThat(찾는다(BORROWS))
                .as("받아 쓰는 자리가 안 보이면 이름이 바뀐 것이다")
                .contains("src/main/java/com/kafkick/waiting/gateway/Rejection.java",
                        "src/main/java/com/kafkick/waiting/gateway/AdmissionGatewayFilter.java");

        assertThat(찾는다(MAKES))
                .describedAs("정책은 %s 가 낸 이름을 받아 쓴다. 비율을 자리마다 들면 "
                        + "운영값을 바꿔도 그 자리가 안 따라간다", TYPE)
                .isEmpty();
    }

    @Test
    @DisplayName("이름을_들여와_만드는_길이_없다")
    void 이름을_들여와_만드는_길이_없다() throws IOException {
        assertThat(찾는다(STATIC_IMPORT)).isEmpty();
    }

    private List<String> 찾는다(Pattern 무엇) throws IOException {
        List<String> 찾은_것 = new ArrayList<>();
        for (Path root : ROOTS) {
            try (Stream<Path> paths = Files.walk(root)) {
                paths.filter(p -> p.toString().endsWith(".java"))
                        .map(this::경로)
                        .filter(p -> EXCUSED.stream().noneMatch(p::endsWith))
                        .filter(p -> 무엇.matcher(읽는다(Path.of(p))).find())
                        .forEach(찾은_것::add);
            }
        }
        return 찾은_것.stream().sorted().toList();
    }

    private String 경로(Path path) {
        return path.toString().replace('\\', '/');
    }

    private String 읽는다(Path path) {
        try {
            return Files.readString(path);
        } catch (IOException e) {
            throw new IllegalStateException("소스를 못 읽는다: " + path, e);
        }
    }
}

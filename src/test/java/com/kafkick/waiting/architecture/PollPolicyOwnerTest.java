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
     * 같은 이름을 두는 것으로 면제를 살 수 있다. 자기 집과 그 계약을 재는 시험뿐이고,
     * 둘 다 실재하는지를 아래 시험이 따로 본다.
     */
    private static final List<String> EXCUSED = List.of(
            "src/main/java/com/kafkick/waiting/domain/queue/" + TYPE + ".java",
            "src/test/java/com/kafkick/waiting/domain/queue/" + TYPE + "Test.java");

    /** 이 검사 자신. 자기가 든 정규식을 자기 글자에서 찾으면 늘 빨갛다. */
    private static final String SELF =
            "src/test/java/com/kafkick/waiting/architecture/PollPolicyOwnerTest.java";

    /**
     * 비율을 손에 들고 새로 만드는 호출. <b>값이 아니라 자리를 본다</b> — 값으로
     * 보면 리터럴과 지역 상수 경유가 빠져나간다. 메서드 참조도 같이 문다.
     */
    private static final Pattern MAKES = Pattern.compile(
            Pattern.quote(TYPE) + "\\s*(\\.\\s*of\\s*\\(|::\\s*of\\b)");

    /** 받아 쓰는 이름. 안 보이면 이름이 바뀐 것이고, 검사가 헛돈 것이다. */
    private static final Pattern BORROWS = Pattern.compile(
            Pattern.quote(TYPE) + "\\s*\\.\\s*(standard|noJitter)\\s*\\(");

    /** 만드는 이름을 들여오는 길. 열리면 타입 이름을 안 적고 만들 수 있다. */
    private static final Pattern STATIC_IMPORT = Pattern.compile(
            "import\\s+static\\s+[\\w.]*" + Pattern.quote(TYPE)
                    + "\\.\\s*(of|standard|noJitter|\\*)\\s*;");

    @Test
    @DisplayName("비율을_손에_들고_만드는_자리가_없다")
    void 비율을_손에_들고_만드는_자리가_없다() throws IOException {
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

    /**
     * <b>탐지가 도는지 본다.</b> 팩토리 이름이 바뀌면 위 두 시험은 아무것도 안 재고
     * 초록이다. 만드는 이름과 받아 쓰는 이름 둘 다 실물에서 잡히는지 확인한다.
     */
    @Test
    @DisplayName("검사가_찾는_이름이_실재한다")
    void 검사가_찾는_이름이_실재한다() throws IOException {
        String 계약_시험 = 읽는다(Path.of(EXCUSED.get(1)));
        assertThat(MAKES.matcher(계약_시험).find())
                .as("만드는 이름이 안 잡힌다 — of 가 바뀌었으면 위 시험이 헛돈다")
                .isTrue();

        assertThat(찾는다(BORROWS))
                .as("받아 쓰는 자리가 안 보이면 이름이 바뀐 것이다")
                .contains("src/main/java/com/kafkick/waiting/gateway/Rejection.java",
                        "src/main/java/com/kafkick/waiting/gateway/AdmissionGatewayFilter.java");
    }

    /** 면제가 죽은 채로 남지 않게 한다. 없는 자리를 빼 주고 있으면 지운다. */
    @Test
    @DisplayName("면제로_둔_자리가_실재한다")
    void 면제로_둔_자리가_실재한다() {
        assertThat(EXCUSED).allSatisfy(자리 ->
                assertThat(Path.of(자리)).as("면제가 가리키는 파일이 없다").exists());
        assertThat(Path.of(SELF)).exists();
    }

    private List<String> 찾는다(Pattern 무엇) throws IOException {
        List<String> 찾은_것 = new ArrayList<>();
        for (Path root : ROOTS) {
            try (Stream<Path> paths = Files.walk(root)) {
                paths.filter(p -> p.toString().endsWith(".java"))
                        .map(this::경로)
                        .filter(p -> !EXCUSED.contains(p) && !SELF.equals(p))
                        .filter(p -> 무엇.matcher(글자만(읽는다(Path.of(p)))).find())
                        .forEach(찾은_것::add);
            }
        }
        return 찾은_것.stream().sorted().toList();
    }

    /**
     * 주석과 문자열을 공백으로 지운다. 안 지우면 이 저장소가 주석에 코드를 그대로
     * 쓰는 습관 때문에 <b>문서만 고쳐도 빨개지고</b>, 문자열에 숨긴 호출은 못 본다.
     */
    private String 글자만(String 소스) {
        StringBuilder 남긴다 = new StringBuilder(소스.length());
        int i = 0;
        while (i < 소스.length()) {
            char c = 소스.charAt(i);
            int 건너뛴다 = 여는_자리(소스, i);
            if (건너뛴다 > i) {
                남긴다.append(" ".repeat(건너뛴다 - i));
                i = 건너뛴다;
            } else {
                남긴다.append(c);
                i++;
            }
        }
        return 남긴다.toString();
    }

    /** 주석·문자열이 여기서 시작하면 그것이 끝나는 자리를, 아니면 {@code from} 을 낸다. */
    private int 여는_자리(String s, int from) {
        if (s.startsWith("//", from)) {
            int end = s.indexOf('\n', from);
            return end < 0 ? s.length() : end;
        }
        if (s.startsWith("/*", from)) {
            int end = s.indexOf("*/", from + 2);
            return end < 0 ? s.length() : end + 2;
        }
        if (s.charAt(from) == '"' || s.charAt(from) == '\'') {
            return 닫는다(s, from);
        }
        return from;
    }

    private int 닫는다(String s, int from) {
        char quote = s.charAt(from);
        // 텍스트 블록은 따옴표 셋으로 열고 닫는다. 안쪽 한 개짜리에 안 속게 한다.
        String 여는_것 = s.startsWith("\"\"\"", from) ? "\"\"\"" : String.valueOf(quote);
        int i = from + 여는_것.length();
        while (i < s.length()) {
            if (s.charAt(i) == '\\') {
                i += 2;
            } else if (s.startsWith(여는_것, i)) {
                return i + 여는_것.length();
            } else {
                i++;
            }
        }
        return s.length();
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

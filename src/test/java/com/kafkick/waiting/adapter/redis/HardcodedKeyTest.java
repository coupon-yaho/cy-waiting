package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 키 문자열은 {@link RedisKeys} 에서만 만든다 (RD-3 · PK-R1).
 *
 * <p>두 곳에서 만들어지면 <b>샤딩을 도입할 때 한쪽만 고쳐지고</b>, 그때는
 * 진행 중인 큐가 통째로 유실된다.
 */
class HardcodedKeyTest {

    /**
     * <b>어댑터만 본다.</b> 도메인은 레디스를 모르므로(DS-1) 거기 있는
     * {@code "coupon:"} 같은 문자열은 리미터 예산 키지 레디스 키가 아니다.
     * 그것까지 잡으면 오탐이고, 오탐이 나면 사람은 검사를 우회한다.
     */
    private static final Path ADAPTER = Path.of("src/main/java/com/kafkick/waiting/adapter");
    private static final Path KEYS_CLASS =
            Path.of("src/main/java/com/kafkick/waiting/adapter/redis/RedisKeys.java");

    /** 이 저장소가 쓰는 키 접두사. 새 키를 만들면 여기도 함께 는다. */
    private static final Pattern KEY_LITERAL = Pattern.compile(
            "\"(queue|maxscore|admitted|grace|alive|stock|gw|scheduler|coupons|coupon):");

    private List<String> violationsIn(Path root) throws IOException {
        List<String> violations = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path file : paths.filter(p -> p.toString().endsWith(".java")).toList()) {
                if (file.equals(KEYS_CLASS)) {
                    continue;   // 여기가 유일하게 허용되는 자리다
                }
                // 주석 속 예시는 위반이 아니다. 오탐이 나면 사람은
                // 검사를 고치는 대신 우회한다.
                String source = 주석을_지운다(
                        Files.readString(file, StandardCharsets.UTF_8));
                String[] lines = source.split("\n", -1);
                for (int i = 0; i < lines.length; i++) {
                    if (KEY_LITERAL.matcher(lines[i]).find()) {
                        violations.add("%s:%d".formatted(file.getFileName(), i + 1));
                    }
                }
            }
        }
        return violations;
    }

    /**
     * 주석만 지우고 문자열은 남긴다. 정규식으로는 못 한다 — 리터럴에 담긴
     * 주석 기호가 진짜 주석의 시작으로 잡히면 <b>검사가 눈이 먼다.</b>
     *
     * <p>지울 자리는 공백으로 덮어 <b>줄 번호를 그대로 둔다.</b> 통째로
     * 지우면 뒤따르는 위반이 엉뚱한 줄로 보고돼 엉뚱한 곳을 보게 된다.
     */
    private static String 주석을_지운다(String source) {
        char[] out = source.toCharArray();
        int n = out.length;
        int i = 0;
        while (i < n) {
            char c = out[i];
            if (c == '/' && i + 1 < n && out[i + 1] == '/') {
                while (i < n && out[i] != '\n') {
                    out[i++] = ' ';
                }
            } else if (c == '/' && i + 1 < n && out[i + 1] == '*') {
                out[i++] = ' ';
                out[i++] = ' ';
                // 닫히지 않은 주석은 파일 끝까지가 주석이다
                while (i < n && !(out[i] == '*' && i + 1 < n && out[i + 1] == '/')) {
                    if (out[i] != '\n') {
                        out[i] = ' ';
                    }
                    i++;
                }
                i = Math.min(i + 2, n);
            } else if (c == '"' && i + 2 < n && out[i + 1] == '"' && out[i + 2] == '"') {
                i += 3;   // 텍스트 블록. 안의 주석 기호는 코드가 아니다
                while (i < n && !(out[i] == '"' && i + 2 < n
                        && out[i + 1] == '"' && out[i + 2] == '"')) {
                    i += out[i] == '\\' ? 2 : 1;
                }
                i = Math.min(i + 3, n);
            } else if (c == '"' || c == '\'') {
                i++;   // 리터럴 안은 건드리지 않는다. 키를 찾는 곳이 여기다
                while (i < n && out[i] != c && out[i] != '\n') {
                    i += out[i] == '\\' ? 2 : 1;
                }
                i++;
            } else {
                i++;
            }
        }
        return new String(out);
    }

    @Test
    @DisplayName("키_문자열_리터럴이_RedisKeys_밖에_없다")
    void 키_문자열_리터럴이_RedisKeys_밖에_없다() throws IOException {
        List<String> violations = violationsIn(ADAPTER);

        assertThat(violations)
                .withFailMessage("RedisKeys 밖의 키 리터럴 %d 건%n%s",
                        violations.size(), String.join("\n", violations))
                .isEmpty();
    }

    @Test
    @DisplayName("검사가_실제로_문다")
    void 검사가_실제로_문다() throws IOException {
        // 통과만 하는 검사는 모든 코드를 통과시킨다 (TS-9).
        Path dir = Files.createTempDirectory("probe");
        Path probe = dir.resolve("Leak.java");
        Files.writeString(probe,
                "class Leak {\n    String key = \"queue:{c1}\";\n}\n", StandardCharsets.UTF_8);

        try {
            // 무엇을 잡았는지까지 본다 — 엉뚱한 것을 잡아도 통과하면 안 된다.
            assertThat(violationsIn(dir)).containsExactly("Leak.java:2");
        } finally {
            Files.deleteIfExists(probe);
            Files.deleteIfExists(dir);
        }
    }

    @Test
    @DisplayName("블록_주석_속_예시는_위반이_아니다")
    void 블록_주석_속_예시는_위반이_아니다() throws IOException {
        Path dir = Files.createTempDirectory("probe");
        Path probe = dir.resolve("Doc.java");
        Files.writeString(probe,
                "/**\n * 예: \"queue:{c1}\" 처럼 쓰면 안 된다\n */\nclass Doc {\n}\n",
                StandardCharsets.UTF_8);

        try {
            assertThat(violationsIn(dir)).isEmpty();
        } finally {
            Files.deleteIfExists(probe);
            Files.deleteIfExists(dir);
        }
    }

    @Test
    @DisplayName("주석_속_예시는_위반이_아니다")
    void 주석_속_예시는_위반이_아니다() throws IOException {
        Path dir = Files.createTempDirectory("probe");
        Path probe = dir.resolve("Fine.java");
        Files.writeString(probe,
                "class Fine {\n    // 예: \"queue:{c1}\" 처럼 쓰면 안 된다\n"
                        + "    String key = RedisKeys.queue(\"c1\", 1, 0);\n}\n",
                StandardCharsets.UTF_8);

        try {
            assertThat(violationsIn(dir)).isEmpty();
        } finally {
            Files.deleteIfExists(probe);
            Files.deleteIfExists(dir);
        }
    }

    @Test
    @DisplayName("문자열_속_주석_기호가_검사를_눈멀게_하지_않는다")
    void 문자열_속_주석_기호가_검사를_눈멀게_하지_않는다() throws IOException {
        // 주석을 정규식으로 지우면 여기서 검사가 통째로 죽는다 — 리터럴
        // 안의 "/*" 가 주석의 시작으로 잡혀 뒤따르는 진짜 위반이 주석
        // 속으로 사라진다. **못 잡는데 통과하니 없는 것보다 나쁘다.**
        Path dir = Files.createTempDirectory("probe");
        Path probe = dir.resolve("Blind.java");
        Files.writeString(probe,
                "class Blind {\n"
                        + "    String block = \"/*\";\n"
                        + "    String line = \"//\";\n"
                        + "    String key = \"queue:{c1}\";\n"
                        + "    String close = \"*/\";\n}\n",
                StandardCharsets.UTF_8);

        try {
            assertThat(violationsIn(dir)).containsExactly("Blind.java:4");
        } finally {
            Files.deleteIfExists(probe);
            Files.deleteIfExists(dir);
        }
    }

    @Test
    @DisplayName("텍스트_블록_속_주석_기호도_코드가_아니다")
    void 텍스트_블록_속_주석_기호도_코드가_아니다() throws IOException {
        Path dir = Files.createTempDirectory("probe");
        Path probe = dir.resolve("Doc.java");
        Files.writeString(probe,
                "class Doc {\n"
                        + "    String sample = \"\"\"\n"
                        + "        /* 여기는 문자열이다\n"
                        + "        \"\"\";\n"
                        + "    String key = \"queue:{c1}\";\n}\n",
                StandardCharsets.UTF_8);

        try {
            assertThat(violationsIn(dir)).containsExactly("Doc.java:5");
        } finally {
            Files.deleteIfExists(probe);
            Files.deleteIfExists(dir);
        }
    }

    @Test
    @DisplayName("도메인의_예산_키는_대상이_아니다")
    void 도메인의_예산_키는_대상이_아니다() throws IOException {
        // 도메인은 레디스를 모른다 (DS-1). 거기 있는 "coupon:" 은 리미터
        // 예산을 가르는 값이지 레디스 키가 아니다 — 잡으면 오탐이다.
        assertThat(ADAPTER.toString()).endsWith("adapter");
        assertThat(violationsIn(ADAPTER)).isEmpty();
    }

    @Test
    @DisplayName("블록_주석_뒤의_위반은_원본_줄_번호로_보고된다")
    void 블록_주석_뒤의_위반은_원본_줄_번호로_보고된다() throws IOException {
        // 주석을 통째로 지우면 줄 번호가 앞으로 밀려 엉뚱한 곳을 보게 된다.
        Path dir = Files.createTempDirectory("probe");
        Path probe = dir.resolve("Shifted.java");
        Files.writeString(probe,
                "/**\n * 여러\n * 줄\n * 주석\n */\nclass Shifted {\n"
                        + "    String key = \"queue:{c1}\";\n}\n",
                StandardCharsets.UTF_8);

        try {
            assertThat(violationsIn(dir)).containsExactly("Shifted.java:7");
        } finally {
            Files.deleteIfExists(probe);
            Files.deleteIfExists(dir);
        }
    }
}

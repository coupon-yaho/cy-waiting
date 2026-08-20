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
                List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
                for (int i = 0; i < lines.size(); i++) {
                    String line = lines.get(i).replaceAll("//.*", "");
                    if (KEY_LITERAL.matcher(line).find()) {
                        violations.add("%s:%d".formatted(file.getFileName(), i + 1));
                    }
                }
            }
        }
        return violations;
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
            assertThat(violationsIn(dir)).isNotEmpty();
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
    @DisplayName("도메인의_예산_키는_대상이_아니다")
    void 도메인의_예산_키는_대상이_아니다() throws IOException {
        // 도메인은 레디스를 모른다 (DS-1). 거기 있는 "coupon:" 은 리미터
        // 예산을 가르는 값이지 레디스 키가 아니다 — 잡으면 오탐이다.
        assertThat(ADAPTER.toString()).endsWith("adapter");
        assertThat(violationsIn(ADAPTER)).isEmpty();
    }
}

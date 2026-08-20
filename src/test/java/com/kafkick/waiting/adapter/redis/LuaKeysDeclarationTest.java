package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Lua 는 {@code KEYS} 에 선언되지 않은 키를 만지지 않는다 (RD-1 · G3.5).
 *
 * <p>클러스터에서 미선언 키 접근은 <b>런타임 오류</b>다. 여기서 못 잡으면
 * 부하 시험 중에 터진다 — 그때는 스크립트를 전부 다시 써야 한다.
 */
class LuaKeysDeclarationTest {

    private static final Path SCRIPTS = Path.of("src/main/resources/redis");

    /** {@code redis.call('CMD', <키>` — 명령 뒤 첫 인자가 키 자리다. */
    private static final Pattern KEY_ARGUMENT = Pattern.compile(
            "redis\\.(?:call|pcall)\\(\\s*'[A-Za-z]+'\\s*,\\s*([^,)]+)");

    private List<Path> scripts() throws IOException {
        try (Stream<Path> paths = Files.list(SCRIPTS)) {
            return paths.filter(p -> p.toString().endsWith(".lua")).sorted().toList();
        }
    }

    /** 주석을 걷어낸다 — 주석 속 예시가 위반으로 잡히면 검사를 안 믿게 된다. */
    private String codeOf(Path script) throws IOException {
        return Files.readAllLines(script, StandardCharsets.UTF_8).stream()
                .map(line -> line.replaceAll("--.*", ""))
                .reduce("", (a, b) -> a + "\n" + b);
    }

    private List<String> violationsIn(Path script) throws IOException {
        List<String> violations = new ArrayList<>();
        Matcher matcher = KEY_ARGUMENT.matcher(codeOf(script));

        while (matcher.find()) {
            String argument = matcher.group(1).strip();
            // KEYS[n] 이거나 KEYS[n] 에서 파생된 지역 변수여야 한다.
            boolean declared = argument.startsWith("KEYS[")
                    || argument.matches("[A-Za-z_][A-Za-z0-9_]*")
                    || argument.startsWith("prefix ..");
            if (!declared) {
                violations.add("%s: %s".formatted(script.getFileName(), argument));
            }
        }
        return violations;
    }

    @Test
    @DisplayName("모든_Lua가_KEYS에_선언된_키만_만진다")
    void 모든_Lua가_KEYS에_선언된_키만_만진다() throws IOException {
        List<String> violations = new ArrayList<>();
        for (Path script : scripts()) {
            violations.addAll(violationsIn(script));
        }

        assertThat(violations)
                .withFailMessage("KEYS 미선언 키 접근 %d 건%n%s",
                        violations.size(), String.join("\n", violations))
                .isEmpty();
    }

    @Test
    @DisplayName("모든_Lua에_KEYS와_ARGV_계약_주석이_있다")
    void 모든_Lua에_KEYS와_ARGV_계약_주석이_있다() throws IOException {
        // 계약이 없으면 호출부가 인자 순서를 추측하게 된다 (RD-10).
        for (Path script : scripts()) {
            String head = Files.readString(script, StandardCharsets.UTF_8);
            assertThat(head)
                    .withFailMessage("%s 에 KEYS 계약 주석이 없다", script.getFileName())
                    .contains("KEYS[1]");
        }
    }

    @Test
    @DisplayName("검사가_실제로_문다")
    void 검사가_실제로_문다() throws IOException {
        // 통과만 하는 검사는 모든 스크립트를 통과시킨다 (TS-9).
        Path probe = Files.createTempFile("probe", ".lua");
        Files.writeString(probe, "redis.call('ZCARD', 'queue:{c1}')\n", StandardCharsets.UTF_8);

        try {
            assertThat(violationsIn(probe)).isNotEmpty();
        } finally {
            Files.deleteIfExists(probe);
        }
    }

    @Test
    @DisplayName("주석_속_예시는_위반이_아니다")
    void 주석_속_예시는_위반이_아니다() throws IOException {
        // 오탐이 나면 사람은 검사를 고치는 대신 우회한다.
        Path probe = Files.createTempFile("probe", ".lua");
        Files.writeString(probe,
                "-- redis.call('ZCARD', 'queue:{c1}') 처럼 쓰면 안 된다\n"
                        + "redis.call('ZCARD', KEYS[1])\n",
                StandardCharsets.UTF_8);

        try {
            assertThat(violationsIn(probe)).isEmpty();
        } finally {
            Files.deleteIfExists(probe);
        }
    }
}

package com.kafkick.waiting.architecture;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 게이트마다 <b>그것을 판정하는 시험이 실제로 있는가.</b>
 *
 * <p>게이트는 계획서에 적힌 문장이다. 문장만 있고 그걸 재는 시험이 없으면
 * "통과했다" 는 말이 아무 근거가 없다 — 그리고 그 상태는 겉으로 안 보인다.
 *
 * <p>여기서 시험의 <b>내용</b>은 안 본다. 그건 그 시험이 할 일이고, 여기서는
 * 게이트와 시험 사이가 비어 있지 않은지만 본다.
 */
class Phase4GateTest {

    private static final Path PLAN = Path.of("plan/04-control-plane.md");
    private static final Path TESTS = Path.of("src/test/java");

    /** 게이트 → 그것을 판정하는 시험 메서드. 이름이 바뀌면 여기가 먼저 깨진다. */
    private static final Map<String, String> 판정하는_시험 = Map.ofEntries(
            Map.entry("G4.1", "배분은_정확히_한_대만_돈다"),
            Map.entry("G4.2", "리더가_죽으면_세_틱_안에_승계된다"),
            Map.entry("G4.3", "틱_지연이_임계_안에_있다"),
            Map.entry("G4.4", "끊긴_동안에도_판정_재료가_남는다"),
            Map.entry("G4.5", "한_번이라도_돈_뒤_멎으면_죽는다"),
            Map.entry("G4.6", "재료가_낡아도_계속_받는다"),
            Map.entry("G4.7", "노드가_늘어도_총합이_전역_크레딧을_안_넘는다"),
            Map.entry("G4.8", "콜드_복귀를_재현한다"),
            Map.entry("G4.9", "리더가_바뀌어도_평활화가_이어진다"),
            Map.entry("G4.10", "보고가_전부_낡으면_0이_나온다"),
            Map.entry("G4.11", "남의_락은_내리지_못한다"),
            Map.entry("G4.12", "회선이_끊겨도_받는_것을_유지한다"));

    private static Stream<String> 시험_본문() throws IOException {
        try (Stream<Path> files = Files.walk(TESTS)) {
            return files.filter(p -> p.toString().endsWith(".java"))
                    .map(Phase4GateTest::읽는다)
                    .toList().stream();
        }
    }

    private static String 읽는다(Path path) {
        try {
            return Files.readString(path);
        } catch (IOException e) {
            throw new IllegalStateException("시험 파일을 못 읽었다: " + path, e);
        }
    }

    @Test
    @DisplayName("계획서의_게이트가_전부_판정된다")
    void 계획서의_게이트가_전부_판정된다() throws IOException {
        // **계획서가 진실이다.** 게이트를 추가하고 시험을 안 붙이면 여기서 걸린다.
        Matcher matcher = Pattern.compile("\\|\\s*\\*{0,2}(G4\\.\\d+)\\*{0,2}\\s*\\|")
                .matcher(Files.readString(PLAN));
        List<String> 계획서의_게이트 = matcher.results().map(r -> r.group(1)).distinct().toList();

        assertThat(계획서의_게이트).isNotEmpty();
        assertThat(판정하는_시험.keySet())
                .as("게이트가 늘었는데 판정하는 시험을 안 이었다")
                .containsAll(계획서의_게이트);
    }

    @Test
    @DisplayName("이어_둔_시험이_실제로_있다")
    void 이어_둔_시험이_실제로_있다() throws IOException {
        // 시험 이름을 바꾸면서 이 표를 안 고치면, 게이트가 판정된다고 적어 놓고
        // 아무것도 안 도는 상태가 된다.
        List<String> 본문 = 시험_본문().toList();

        assertThat(판정하는_시험).allSatisfy((gate, method) ->
                assertThat(본문)
                        .as("%s 를 판정하는 시험이 없다: %s", gate, method)
                        .anyMatch(body -> body.contains("void " + method + "(")));
    }
}

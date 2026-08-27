package com.kafkick.waiting.architecture;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * 게이트마다 <b>그것을 판정하는 시험이 실제로 있는가</b> (Phase 6).
 *
 * <p>게이트는 계획서에 적힌 문장이다. 문장만 있고 그걸 재는 시험이 없으면
 * "통과했다" 는 말이 아무 근거가 없고, 그 상태는 겉으로 안 보인다.
 */
// 부하와 커버리지로만 재는 게이트는 여기 안 든다 — 단위 시험으로 못 만드는
// 값이라, 이어 두면 짝을 찾다 항상 실패하거나 가짜 짝을 만들게 된다.
class Phase6GateTest {

    private static final Path PLAN = Path.of("plan/06-protection.md");
    private static final Path TESTS = Path.of("src/test/java");

    /** 부하 판이 재는 게이트. 여기서는 짝을 안 요구하고, 목록에 있다는 것만 본다. */
    private static final List<String> 부하가_재는_것 =
            List.of("G6.11", "G6.12", "G6.16", "G6.20");

    /** 사람과 수집기가 확인하는 게이트. 기계가 대신 못 잰다. */
    private static final List<String> 사람이_보는_것 = List.of("G6.7", "G6.8", "G6.15");

    /**
     * 게이트 → 그것을 판정하는 시험. <b>클래스와 메서드까지 적는다.</b>
     *
     * <p>이름만 적으면 아무 파일에나 그 이름이 있어도 통과한다. 게이트 문장이 둘을
     * 요구하면 둘 다 적는다 — 하나만 적으면 나머지를 지워도 판정됨으로 남는다.
     */
    private static final Map<String, List<String>> 판정하는_시험 = Map.ofEntries(
            Map.entry("G6.1", List.of(
                    "BackendFallbackTest#서킷이_열리면_503을_낸다",
                    "BackendFallbackTest#fallback_주소를_받는_라우트가_있다")),
            // 뒷단이 멎어도 안 물린다. 끊는 자리가 서킷 안쪽이어야 취소가 아니라
            // 오류로 집계된다 — 그 순서까지 봐야 한다.
            Map.entry("G6.3", List.of(
                    "GatewayRoutesTest#응답_상한이_없으면_기동을_막는다",
                    "GatewayRoutesTest#응답_상한이_격벽_시한_뒤면_기동을_막는다",
                    "GatewayWiringTest#격벽_시한은_뒷단_응답_상한보다_뒤다")),
            Map.entry("G6.4", List.of(
                    "BulkheadBoundTest#상정한_2천개는_전부_들어간다",
                    "BulkheadBoundTest#상한을_넘기면_밀어내지_않고_거절한다",
                    "BulkheadBoundTest#쿠폰이_계속_갈려도_안_샌다")),
            // 종료 중 5xx 0. 드레이닝 동안 재료가 계속 갱신돼야 판정이 안 굳는다.
            Map.entry("G6.6", List.of(
                    "ShutdownStateTest#알리면_드레이닝이다",
                    "ShutdownSignalTest#앞선_리스너가_터져도_드레이닝을_알린다",
                    "SnapshotRefreshDrainOrderTest#드레이닝_동안_재료가_계속_갱신된다")),
            Map.entry("G6.9", List.of(
                    "GatewayWiringTest#라우트가_설정한_뒷단으로_간다")),
            Map.entry("G6.10", List.of(
                    "GatewayRoutesTest#뒷단_주소가_없으면_기동을_막는다")),
            Map.entry("G6.13", List.of(
                    "TunablesRefreshTest#실려_있으면_그_값을_들고_있는다",
                    "TunablesRefreshTest#키가_없으면_안_실린_것으로_둔다")),
            Map.entry("G6.14", List.of(
                    "TunablesTest#범위를_벗어나면_그_값만_기본값이_된다",
                    "TunablesTest#깨진_값이면_기본값으로_떨어진다")),
            // 개인화 응답이 모이면 검사가 실패한다. 못 막는 것까지 값으로 적어 둔다.
            Map.entry("G6.17", List.of(
                    "CoalescingPersonalizationTest#뒷단이_같게_답하면_모인다")),
            Map.entry("G6.18", List.of(
                    "QueryCoalescingFilterTest#상한을_넘는_응답은_안_담는다")),
            Map.entry("G6.19", List.of(
                    "QueryCoalescingFilterTest#장애_응답은_안_담는다")),
            Map.entry("G6.21", List.of(
                    "BackendCircuitTest#느린_호출을_실패로_집계한다",
                    "GatewayWiringTest#느림_기준은_뒷단_응답_상한보다_앞이다")),
            Map.entry("G6.22", List.of(
                    "DrainWaitTest#readiness를_먼저_내리고_기다린다",
                    "ShutdownBudgetTest#LB_제외_대기는_앞단_체크_주기의_짝이다")));

    @Test
    @DisplayName("계획서의_게이트와_표가_정확히_같다")
    void 계획서의_게이트와_표가_정확히_같다() throws IOException {
        // **양쪽으로 잠근다.** 한 방향만 보면 게이트를 지웠을 때 표에 죽은 항목이
        // 남고, 그걸 아무도 모른다.
        Matcher matcher = Pattern.compile("\\|\\s*\\*{0,2}(G6\\.\\d+)\\*{0,2}\\s*\\|")
                .matcher(Files.readString(PLAN));
        List<String> 계획서의_게이트 = matcher.results().map(r -> r.group(1)).distinct().toList();

        // 정규식이 안 물면 목록이 비고, 그러면 아래 비교가 공허하게 통과한다.
        assertThat(계획서의_게이트).hasSizeGreaterThanOrEqualTo(18);
        assertThat(Stream.of(판정하는_시험.keySet().stream(), 부하가_재는_것.stream(),
                        사람이_보는_것.stream()).flatMap(s -> s).toList())
                .as("계획서의 게이트와 표가 갈렸다")
                .containsExactlyInAnyOrderElementsOf(계획서의_게이트);
    }

    @Test
    @DisplayName("이어_둔_시험이_실제로_돌아간다")
    void 이어_둔_시험이_실제로_돌아간다() throws IOException {
        Map<String, Class<?>> 클래스들 = 시험_클래스();

        assertThat(판정하는_시험).allSatisfy((gate, 시험들) -> 시험들.forEach(짝 -> {
            String 클래스명 = 짝.substring(0, 짝.indexOf('#'));
            String 메서드명 = 짝.substring(짝.indexOf('#') + 1);

            assertThat(클래스들).as("%s 의 짝인 %s 가 없다", gate, 클래스명).containsKey(클래스명);
            Class<?> 클래스 = 클래스들.get(클래스명);
            // 있기만 하면 안 된다. 이름이 같은 헬퍼에 이어 놓으면 아무것도 안 돈다.
            List<Method> 그_클래스의_시험 = Stream.of(클래스.getDeclaredMethods())
                    .filter(m -> m.isAnnotationPresent(Test.class)).toList();
            assertThat(그_클래스의_시험).extracting(Method::getName)
                    .as("%s 를 판정한다는 %s 가 그 클래스의 시험이 아니다", gate, 짝)
                    .contains(메서드명);

            Method 메서드 = 그_클래스의_시험.stream()
                    .filter(m -> m.getName().equals(메서드명)).findFirst().orElseThrow();
            // **꺼 둔 시험은 있는 것이 아니다.** 클래스째 꺼도 마찬가지다.
            assertThat(메서드.isAnnotationPresent(Disabled.class)
                            || 클래스.isAnnotationPresent(Disabled.class))
                    .as("%s 를 판정한다는 %s 가 꺼져 있다", gate, 짝).isFalse();
        }));
    }

    private static Map<String, Class<?>> 시험_클래스() throws IOException {
        try (Stream<Path> files = Files.walk(TESTS)) {
            return files.filter(p -> p.toString().endsWith(".java"))
                    .collect(Collectors.toMap(
                            p -> p.getFileName().toString().replace(".java", ""),
                            Phase6GateTest::불러온다,
                            (a, b) -> {
                                throw new IllegalStateException(
                                        "같은 이름의 시험 클래스가 둘 이상이다");
                            }));
        }
    }

    private static Class<?> 불러온다(Path path) {
        String 이름 = TESTS.relativize(path).toString()
                .replace(".java", "").replace(File.separatorChar, '.');
        try {
            return Class.forName(이름, false, Phase6GateTest.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("시험 클래스를 못 불러왔다: " + 이름, e);
        }
    }
}

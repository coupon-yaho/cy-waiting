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
import org.junit.jupiter.api.Tag;
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

    /**
     * 부하 실행이 재는 게이트. 짝을 안 요구하고 목록에 있다는 것만 본다.
     *
     * <p>판정은 {@code test/load/evaluate-gate.sh} 가 하고, 그 판정이 헐거워지지
     * 않았는지는 {@code gate-selftest.sh} 가 본다.
     */
    private static final List<String> 부하가_재는_것 =
            List.of("G6.9", "G6.10", "G6.11", "G6.12", "G6.16", "G6.20");

    /** CI 스크립트가 재는 게이트. JUnit 이 아닐 뿐 기계가 잰다. */
    private static final List<String> CI_스크립트가_재는_것 = List.of("G6.15");

    /** 사람과 수집기가 확인하는 게이트. 기계가 대신 못 잰다. */
    private static final List<String> 사람이_보는_것 = List.of("G6.7", "G6.8");

    /**
     * 게이트 → 그것을 판정하는 시험. <b>클래스와 메서드까지 적는다.</b>
     *
     * <p>이름만 적으면 아무 파일에나 그 이름이 있어도 통과한다. 게이트 문장이 둘을
     * 요구하면 둘 다 적는다 — 하나만 적으면 나머지를 지워도 판정됨으로 남는다.
     */
    // **짝을 고르는 법.** 그 게이트 문장을 깨는 가장 작은 변경 하나를 정하고,
    // 그것을 잡는 시험을 적는다. 구성 요소의 단위 시험만 이으면 그 요소들을 잇는
    // 배선이 빠져도 전부 초록이고, 조용히 깨지는 것이 정확히 배선이다.
    /**
     * <b>아직 못 재는 게이트.</b> 계획서가 그 사실을 적어 둔 것만 여기 든다.
     *
     * <p>짝을 억지로 이으면 "못 잰다" 가 "판정됨" 으로 덮인다 — 이 시험이 막으려던
     * 상태가 이 시험 안에서 다시 생긴다.
     */
    private static final List<String> 아직_못_재는_것 = List.of("G6.17");

    private static final Map<String, List<String>> 판정하는_시험 = Map.ofEntries(
            Map.entry("G6.1", List.of(
                    "BackendFallbackTest#서킷이_열리면_503을_낸다",
                    "BackendFallbackTest#fallback_주소를_받는_라우트가_있다",
                    // **배선까지 본다.** 핸들러가 503 을 만들고 라우터가 그 주소를
                    // 받아도, 서킷이 그리로 안 넘기면 사용자는 404 를 본다.
                    "GatewayRoutesTest#발급_라우트에_서킷이_걸려_있다")),
            // 뒷단이 멎어도 안 물린다. 끊는 자리가 서킷 안쪽이어야 취소가 아니라
            // 오류로 집계된다 — 그 순서까지 봐야 한다.
            Map.entry("G6.3", List.of(
                    "GatewayRoutesTest#응답_상한이_없으면_기동을_막는다",
                    "GatewayRoutesTest#응답_상한이_격벽_시한_뒤면_기동을_막는다",
                    "GatewayWiringTest#격벽_시한은_뒷단_응답_상한보다_뒤다",
                    // **값 검증만 이으면 상한이 라우트에 안 실린 경우가 통과한다.**
                    "GatewayRoutesTest#발급_라우트가_응답_상한을_들고_있다",
                    "GatewayRoutesTest#조회_라우트도_응답_상한을_들고_있다")),
            Map.entry("G6.4", List.of(
                    "BulkheadBoundTest#상정한_2천개는_전부_들어간다",
                    "BulkheadBoundTest#상한을_넘기면_밀어내지_않고_거절한다",
                    "BulkheadBoundTest#쿠폰이_계속_갈려도_안_샌다")),
            // 종료 중 5xx 0. 드레이닝 동안 재료가 계속 갱신돼야 판정이 안 굳는다.
            Map.entry("G6.6", List.of(
                    "ShutdownStateTest#알리면_드레이닝이다",
                    "ShutdownSignalTest#앞선_리스너가_터져도_드레이닝을_알린다",
                    "SnapshotRefreshDrainOrderTest#드레이닝_동안_재료가_계속_갱신된다",
                    // **신호가 흐르는 것과 드레인이 일어나는 것은 다르다.** 우아한
                    // 종료를 꺼도 앞의 셋은 전부 초록이다.
                    "HealthWiringTest#종료_신호에_진행_중인_요청을_마친다",
                    // 문장의 뒷절 — 레지스트리에서 빠진다.
                    "GatewayHeartbeatLoopTest#종료하면_등록을_해제한다")),
            Map.entry("G6.13", List.of(
                    "TunablesRefreshTest#실려_있으면_그_값을_들고_있는다",
                    "TunablesRefreshTest#키가_없으면_안_실린_것으로_둔다")),
            Map.entry("G6.14", List.of(
                    "TunablesTest#범위를_벗어나면_그_값만_기본값이_된다",
                    "TunablesTest#깨진_값이면_기본값으로_떨어진다")),
            Map.entry("G6.18", List.of(
                    "QueryCoalescingFilterTest#상한을_넘는_응답은_안_담는다")),
            Map.entry("G6.19", List.of(
                    "QueryCoalescingFilterTest#장애_응답은_안_담는다")),
            Map.entry("G6.21", List.of(
                    "BackendCircuitTest#느린_호출을_실패로_집계한다",
                    "GatewayWiringTest#느림_기준은_뒷단_응답_상한보다_앞이다")),
            Map.entry("G6.22", List.of(
                    "DrainWaitTest#readiness를_먼저_내리고_기다린다",
                    "ShutdownBudgetTest#LB_제외_대기는_앞단_체크_주기의_짝이다",
                    // **플래그가 서는 것과 readiness 가 내려가는 것은 다르다.**
                    // 그 사이 다리를 끊어도 위의 둘은 초록이다.
                    "JudgingHealthTest#종료_신호를_받으면_안_받는다",
                    "HealthWiringTest#받는_판정_그룹이_판정_능력만_본다",
                    "HealthWiringTest#종료_신호에_진행_중인_요청을_마친다")));

    @Test
    @DisplayName("계획서의_게이트와_표가_정확히_같다")
    void 계획서의_게이트와_표가_정확히_같다() throws IOException {
        // **양쪽으로 잠근다.** 한 방향만 보면 게이트를 지웠을 때 표에 죽은 항목이
        // 남고, 그걸 아무도 모른다.
        Matcher matcher = Pattern.compile("\\|\\s*\\*{0,2}(G6\\.\\d+)\\*{0,2}\\s*\\|")
                .matcher(Files.readString(PLAN));
        // **중복을 지우지 않는다.** 지우면 같은 게이트가 두 줄 있는 계획서가
        // 그대로 통과하고, "정확히 같다" 는 주장이 거짓이 된다.
        List<String> 계획서의_게이트 = matcher.results().map(r -> r.group(1)).toList();

        // 정규식이 계획서 서식 변경에 밀리면 여기서 먼저 드러난다.
        assertThat(계획서의_게이트).hasSize(20);
        assertThat(Stream.of(판정하는_시험.keySet().stream(), 부하가_재는_것.stream(),
                        CI_스크립트가_재는_것.stream(), 사람이_보는_것.stream(),
                        아직_못_재는_것.stream()).flatMap(s -> s).toList())
                .as("계획서의 게이트와 표가 갈렸다")
                .containsExactlyInAnyOrderElementsOf(계획서의_게이트);

        // **면제 목록은 조용히 늘면 안 된다.** 게이트를 한 줄 옮기는 것만으로
        // 짝 요구가 사라지는데, 크기를 못 박으면 그 이동이 리뷰에 걸린다.
        assertThat(부하가_재는_것).hasSize(6);
        assertThat(CI_스크립트가_재는_것).hasSize(1);
        assertThat(사람이_보는_것).hasSize(2);
        assertThat(아직_못_재는_것).hasSize(1);
    }

    @Test
    @DisplayName("이어_둔_시험이_실제로_돌아간다")
    void 이어_둔_시험이_실제로_돌아간다() throws IOException {
        Map<String, Class<?>> 클래스들 = 시험_클래스();

        // **빈 맵과 빈 목록은 공허하게 통과한다.** 표를 비우면 초록이 된다.
        // 크기를 못 박으면 항목을 면제 목록으로 옮기는 것도 같이 걸린다.
        assertThat(판정하는_시험).hasSize(10);
        assertThat(판정하는_시험).allSatisfy((gate, 시험들) -> {
            assertThat(시험들).as("%s 에 짝이 하나도 없다", gate)
                    .hasSizeGreaterThanOrEqualTo(1);
            시험들.forEach(짝 -> {
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
            // **빌드를 막는 자리에서 돌아야 한다.** 야간에만 도는 계층에 이어
            // 놓으면 그 게이트는 병합을 막지 못한 채 판정됨으로 남는다.
                Tag 태그 = 클래스.getAnnotation(Tag.class);
                assertThat(태그 == null || "context".equals(태그.value()))
                        .as("%s 를 판정한다는 %s 가 check 밖에서 돈다", gate, 짝).isTrue();
            });
        });
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

package com.kafkick.waiting.architecture;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
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

    /**
     * 게이트 → 그것을 판정하는 시험. <b>클래스까지 적는다.</b>
     *
     * <p>이름만 적으면 아무 파일에나 그 이름이 있어도 통과한다. 게이트 문장이
     * 둘을 요구하면 둘 다 적는다 — 하나만 적으면 나머지를 지워도 판정됨으로 남는다.
     */
    // 한계: 문장을 몇 개로 쪼갤지는 사람이 정한다. 덜 적은 것은 기계가 못 잡고,
    // 게이트 문장이 바뀔 때 이 표를 같이 읽는 것 말고 방법이 없다.
    private static final Map<String, List<String>> 판정하는_시험 = Map.ofEntries(
            // 사본을 도는 시험은 락 자체를 재지 못한다. 배타성은 실물 스크립트를
            // 열 스레드로 두드리는 쪽이 재고, 그 위에서 배분이 하나로 좁혀지는
            // 것은 노드를 세우는 쪽이 잰다 — 둘 다 있어야 문장이 덮인다.
            Map.entry("G4.1", List.of(
                    "LeaderElectionTest#노드_열이_동시에_시도하면_정확히_한_대만_성공한다",
                    "ControlPlaneGateTest#배분은_정확히_한_대만_돈다")),
            Map.entry("G4.2", List.of(
                    "ControlPlaneGateTest#리더가_죽으면_세_틱_안에_승계된다",
                    "ControlPlaneGateTest#승계_구간의_크레딧_중복이_한_틱분을_안_넘는다")),
            Map.entry("G4.3", List.of(
                    "ControlPlaneGateTest#틱_지연이_임계_안에_있다")),
            Map.entry("G4.4", List.of(
                    "SnapshotRefreshIntegrationTest#끊긴_동안에도_판정_재료가_남는다")),
            Map.entry("G4.5", List.of(
                    "LoopAliveHealthTest#한_번이라도_돈_뒤_멎으면_죽는다",
                    "LoopAliveHealthTest#받아오기만_실패하는_것은_죽음이_아니다")),
            Map.entry("G4.6", List.of(
                    "JudgingHealthTest#재료가_낡아도_계속_받는다")),
            Map.entry("G4.7", List.of(
                    "ControlPlaneGateTest#노드가_늘어도_총합이_전역_크레딧을_안_넘는다",
                    "ControlPlaneGateTest#노드가_줄어도_총합이_전역_크레딧을_안_넘는다")),
            Map.entry("G4.8", List.of(
                    "CapacityCollectorTest#처음_본_인스턴스는_램프업_비율만_받는다",
                    "CapacityCollectorTest#램프업이_끝나면_보고를_그대로_쓴다",
                    "CapacityCollectorTest#사라진_인스턴스는_기록에서_지운다")),
            // 히스테리시스 절반은 아직 미판정이다. 이월 그릇은 만들었지만
            // 제품이 히스테리시스를 안 돌린다 — 어디서 돌릴지가 CY-324 다.
            // 여기 적으면 "판정됨" 이 되어 이 표가 거짓말을 한다.
            Map.entry("G4.9", List.of(
                    "ControlPlaneGateTest#리더가_바뀌어도_평활화가_이어진다")),
            Map.entry("G4.10", List.of(
                    "CapacityCollectorTest#신선한_보고가_없으면_하한을_쓴다")),
            // **실물 스크립트를 도는 것으로 잇는다.** 사본을 도는 시험에 이으면
            // 스크립트의 소유권 확인을 지워도 초록이다.
            Map.entry("G4.11", List.of(
                    "LeaderElectionTest#남이_잡고_있으면_획득하지_못한다",
                    // 자기 락이 지워지는 것만 보면 소유권 확인 없는 DEL 로도
                    // 똑같이 통과한다. 남의 락이 안 지워지는 쪽이 이 문장이다.
                    "LeaderElectionTest#남의_락은_지워지지_않는다")),
            // 이 게이트가 막으려는 회귀는 "레디스를 보는 기여자가 준비 그룹에
            // 섞이는 것" 이다. 지시자를 손으로 만들어 부르는 시험은 그 배선을
            // 아예 안 지나므로, 기여자가 섞여도 초록으로 남는다.
            Map.entry("G4.12", List.of(
                    "HealthGroupTest#의존성_헬스가_그룹에_안_섞인다",
                    "HealthGroupTest#레디스_기여자가_아예_안_올라온다",
                    "HealthUnderWireFaultsTest#회선이_끊겨도_받는_것을_유지한다")));

    private static Map<String, String> 시험_파일() throws IOException {
        try (Stream<Path> files = Files.walk(TESTS)) {
            return files.filter(p -> p.toString().endsWith(".java"))
                    .collect(Collectors.toMap(
                            p -> p.getFileName().toString().replace(".java", ""),
                            Phase4GateTest::읽는다,
                            // **조용히 하나만 남기지 않는다.** 패키지가 다른 두
                            // 시험이 같은 이름이면 엉뚱한 파일을 보고 판정하게 되고,
                            // 게이트가 판정됨으로 남는데 실제 시험은 없는 상태가
                            // 다시 생긴다.
                            (a, b) -> {
                                throw new IllegalStateException(
                                        "같은 이름의 시험 클래스가 둘 이상이다");
                            }));
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
    @DisplayName("계획서의_게이트와_표가_정확히_같다")
    void 계획서의_게이트와_표가_정확히_같다() throws IOException {
        // **양쪽으로 잠근다.** 한 방향만 보면 게이트를 지웠을 때 표에 죽은
        // 항목이 남고, 그걸 아무도 모른다.
        Matcher matcher = Pattern.compile("\\|\\s*\\*{0,2}(G4\\.\\d+)\\*{0,2}\\s*[|—]")
                .matcher(Files.readString(PLAN));
        List<String> 계획서의_게이트 = matcher.results().map(r -> r.group(1)).distinct().toList();

        // 정규식이 안 물면 목록이 비고, 그러면 아래 비교가 공허하게 통과한다.
        assertThat(계획서의_게이트).hasSizeGreaterThanOrEqualTo(12);
        assertThat(판정하는_시험.keySet())
                .as("계획서의 게이트와 표가 갈렸다")
                .containsExactlyInAnyOrderElementsOf(계획서의_게이트);
    }

    @Test
    @DisplayName("이어_둔_시험이_그_파일에_실제로_있다")
    void 이어_둔_시험이_그_파일에_실제로_있다() throws IOException {
        // 이름만 보면 엉뚱한 시험에 이어 놓아도 안 걸린다. 클래스까지 본다.
        Map<String, String> 파일 = 시험_파일();

        assertThat(판정하는_시험).allSatisfy((gate, 시험들) -> 시험들.forEach(짝 -> {
            String 클래스 = 짝.substring(0, 짝.indexOf('#'));
            String 메서드 = 짝.substring(짝.indexOf('#') + 1);

            assertThat(파일).as("%s 의 짝인 %s 가 없다", gate, 클래스).containsKey(클래스);
            String 본문 = 파일.get(클래스);
            String 선언 = "void " + 메서드 + "(";
            assertThat(본문).as("%s 를 판정한다는 %s 가 그 파일에 없다", gate, 짝).contains(선언);

            // **꺼 둔 시험은 있는 것이 아니다.** 선언만 보면 실행에서 빼 놓아도
            // 통과한다 — 아무것도 안 도는데 게이트는 판정됨으로 남는다.
            String 앞 = 본문.substring(0, 본문.indexOf(선언));
            assertThat(앞.substring(앞.lastIndexOf("@Test")))
                    .as("%s 를 판정한다는 %s 가 꺼져 있다", gate, 짝)
                    // 애노테이션 이름만 본다. 정규화해 적어도 걸리게 하려는 것이다.
                    .doesNotContain("Disabled");
        }));
    }
}

package com.kafkick.waiting.architecture;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.lang.reflect.Method;
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
 * 게이트마다 <b>그것을 판정하는 시험이 실제로 있는가.</b>
 *
 * <p>게이트는 계획서에 적힌 문장이다. 문장만 있고 그걸 재는 시험이 없으면
 * "통과했다" 는 말이 아무 근거가 없다 — 그리고 그 상태는 겉으로 안 보인다.
 *
 * <p>여기서 시험의 <b>내용</b>은 안 본다. 그건 그 시험이 할 일이고, 여기서는
 * 게이트와 시험 사이가 비어 있지 않은지만 본다.
 */
class Phase5GateTest {

    private static final Path PLAN = Path.of("plan/05-data-plane.md");
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
            // 통과 경로가 레디스를 안 친다. 토큰으로 통과하는 쪽도 같이 봐야
            // 한다 — 검증이 조회를 부르면 그 왕복이 통과 인원에 비례한다.
            Map.entry("G5.1", List.of(
                    "AdmissionGatewayFilterTest#통과_판정은_줄을_안_친다",
                    "AdmissionGatewayFilterTest#차례가_온_사람은_토큰으로_통과한다")),
            Map.entry("G5.2", List.of(
                    "AdmissionGatewayFilterTest#미지_쿠폰을_만_번_불러도_줄을_안_만든다")),
            // 순번 조회 경로가 문장에 들어 있다. 발급 쪽만 이으면 그 경로를 뚫어도 초록이다.
            Map.entry("G5.3", List.of(
                    "AdmissionGatewayFilterTest#남의_토큰으로는_안_통한다",
                    "AdmissionGatewayFilterTest#남이_받은_토큰으로는_안_통한다",
                    "QueueStatusFilterTest#토큰이_없으면_줄을_안_친다",
                    "EntryTokenTest#정한_수명을_안_넘는다")),
            Map.entry("G5.4", List.of(
                    "MemberIdentityFilterTest#식별자가_없으면_막는다",
                    "MemberIdentityFilterTest#등급이_없으면_막는다")),
            // 라우트에 실제로 붙었는지는 라우터를 세워 봐야 안다.
            Map.entry("G5.5", List.of(
                    "GatewayRoutesTest#발급_라우트에_판정이_붙어_있다")),
            Map.entry("G5.6", List.of(
                    "QueueStatusFilterTest#기다리는_중이면_순번을_준다",
                    "QueueStatusFilterTest#남의_경로는_그대로_흘려보낸다")),
            Map.entry("G5.7", List.of(
                    "AdmissionGatewayFilterTest#재시도_안내가_충분히_흩어진다")),
            // 약한 키로 조용히 돌면 서명이 있다는 사실이 무의미해진다.
            Map.entry("G5.9", List.of(
                    "QueueTokenTest#짧은_비밀키로는_안_만들어진다",
                    "EntryTokenTest#짧은_비밀키로는_안_만들어진다")),
            // 커버리지는 시험이 아니라 게이트 작업이 잰다. 그 설정이 살아 있는지를 본다.
            Map.entry("G5.10", List.of(
                    "CoverageGateTest#판정_패키지에_분기_100_퍼센트가_걸려_있다",
                    "CoverageGateTest#게이트웨이_패키지에도_분기_임계가_걸려_있다",
                    "CoverageGateTest#게이트웨이_임계가_check에_물려_있다")),
            Map.entry("G5.12", List.of(
                    "AbuseLimitFilterTest#한_사람이_너무_빨리_두드리면_막는다",
                    "AbuseLimitFilterTest#식별자를_바꿔도_주소로_막는다")),
            Map.entry("G5.13", List.of(
                    "AbuseLimitFilterTest#식별자를_만_개로_바꿔도_맵이_유계다")),
            Map.entry("G5.14", List.of(
                    "AbuseLimitFilterTest#폴링은_발급보다_느슨하다")),
            Map.entry("G5.15", List.of(
                    "QueueStatusFilterTest#헤더만_바꿔서는_남의_순번을_못_본다")),
            Map.entry("G5.16", List.of(
                    "AdmissionGatewayFilterTest#줄을_세운_뒤에는_신규_유입이_못_넘는다",
                    "AdmissionGatewayFilterTest#래치는_다음_창까지_버틴다",
                    "AdmissionGatewayFilterTest#래치는_스냅샷이_유효한_동안_버틴다",
                    "AdmissionGatewayFilterTest#소수부_한계에서도_래치가_더_오래_산다")),
            Map.entry("G5.17", List.of(
                    "AdmissionGatewayFilterTest#래치가_풀리면_무대기_통과가_되살아난다")));

    /**
     * 짝의 클래스 이름 → 그 클래스.
     *
     * <p>소스를 읽지 않고 <b>실제 클래스</b>를 집는다. 문자열로 찾으면 주석 안의
     * 같은 시그니처나 애노테이션의 위치·표기에 판정이 흔들린다.
     */
    private static Map<String, Class<?>> 시험_클래스() throws IOException {
        try (Stream<Path> files = Files.walk(TESTS)) {
            return files.filter(p -> p.toString().endsWith(".java"))
                    .collect(Collectors.toMap(
                            p -> p.getFileName().toString().replace(".java", ""),
                            Phase5GateTest::불러온다,
                            // **조용히 하나만 남기지 않는다.** 패키지가 다른 두
                            // 시험이 같은 이름이면 엉뚱한 클래스를 보고 판정하게
                            // 되고, 게이트가 판정됨으로 남는데 실제 시험은 없는
                            // 상태가 다시 생긴다.
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
            return Class.forName(이름, false, Phase5GateTest.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("시험 클래스를 못 불러왔다: " + 이름, e);
        }
    }

    @Test
    @DisplayName("계획서의_게이트와_표가_정확히_같다")
    void 계획서의_게이트와_표가_정확히_같다() throws IOException {
        // **양쪽으로 잠근다.** 한 방향만 보면 게이트를 지웠을 때 표에 죽은
        // 항목이 남고, 그걸 아무도 모른다.
        Matcher matcher = Pattern.compile("\\|\\s*\\*{0,2}(G5\\.\\d+)\\*{0,2}\\s*[|—]")
                .matcher(Files.readString(PLAN));
        List<String> 계획서의_게이트 = matcher.results().map(r -> r.group(1)).distinct().toList();

        // 정규식이 안 물면 목록이 비고, 그러면 아래 비교가 공허하게 통과한다.
        assertThat(계획서의_게이트).hasSizeGreaterThanOrEqualTo(15);
        assertThat(판정하는_시험.keySet())
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

}

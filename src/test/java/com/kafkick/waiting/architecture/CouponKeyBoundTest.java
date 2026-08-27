package com.kafkick.waiting.architecture;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 쿠폰 키 상한은 <b>한 곳에서만 정한다</b> (6.3.5).
 *
 * <p>쿠폰별 표를 들고 있는 자리가 셋이다 — 판정 리미터, 격벽, 등록 래치. 상한을
 * 자리마다 적으면 사본이 갈라지고, 그때 한쪽만 찬 상태에서 판정이 어긋난다.
 *
 * <p><b>자리 목록을 손으로 들지 않는다.</b> 사본이 갈라지는 것을 막겠다는 시험이
 * 자기 목록을 사본으로 들면, 네 번째 자리가 생겼을 때 아무도 모른다. 소스 전체를
 * 훑고 <b>예외만</b> 이름으로 뺀다.
 */
class CouponKeyBoundTest {

    private static final Path MAIN = Path.of("src/main/java");

    /** 이 상한이 사는 곳. 쿠폰으로 세는 자리는 모두 여기를 가리켜야 한다. */
    private static final String HOME = "CouponKeys.MAX";

    /** 쿠폰별 표를 만드는 호출들. 인자 앞 공백과 줄바꿈을 허용한다. */
    private static final List<String> CALLS = List.of(
            "Bulkhead.withMaxKeys(",
            "EnqueueLatch.covering(",
            "SecondWindowLimiter.withMaxKeys(");

    /**
     * 쿠폰으로 세지 <b>않는</b> 자리. 여기 들려면 근거가 있어야 한다.
     *
     * <p>남용 리미터는 클라이언트 식별자로 센다. 키 공간이 다르고 상한도 열 배
     * 넓다 — 쿠폰 상한으로 묶으면 주소를 바꿔가며 밀어 넣는 것에 좁은 상한을 준다.
     *
     * <p>{@code SingleFlight} 는 여기 안 든다. 그쪽 키는 경로와 {@code Vary} 헤더로
     * 만들고 팩토리 이름도 달라 위 호출 목록에 애초에 안 걸린다.
     */
    private static final Set<String> EXCUSED = Set.of("AbuseLimitFilter.java");

    private record Passed(String file, String call, String argument) {

        @Override
        public String toString() {
            return file + " 의 " + call + argument;
        }
    }

    @Test
    @DisplayName("쿠폰으로_세는_자리는_모두_같은_상한을_쓴다")
    void 쿠폰으로_세는_자리는_모두_같은_상한을_쓴다() throws IOException {
        List<Passed> found = allCallSites();

        assertThat(found)
                .describedAs("쿠폰별 표를 만드는 자리를 하나도 못 찾았다 — "
                        + "이름이 바뀌었고 이 시험은 아무것도 안 보고 있다")
                .isNotEmpty();
        assertThat(found)
                .describedAs("쿠폰 키 상한은 %s 한 곳에서만 정한다. 새 자리라면 그것을 "
                        + "넘기고, 쿠폰으로 안 세는 자리라면 근거와 함께 예외에 넣는다", HOME)
                .filteredOn(p -> !EXCUSED.contains(p.file()))
                .allSatisfy(p -> assertThat(p.argument()).isEqualTo(HOME));
    }

    /** 예외 목록이 죽은 채로 남지 않게 한다. 없는 자리를 빼 주고 있으면 지운다. */
    @Test
    @DisplayName("예외로_둔_자리가_실재한다")
    void 예외로_둔_자리가_실재한다() throws IOException {
        List<String> files = allCallSites().stream().map(Passed::file).distinct().toList();

        assertThat(files).containsAll(EXCUSED);
    }

    private List<Passed> allCallSites() throws IOException {
        List<Passed> found = new ArrayList<>();
        try (Stream<Path> files = Files.walk(MAIN)) {
            for (Path file : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                collect(file, found);
            }
        }
        return found;
    }

    /**
     * 한 파일의 호출을 <b>전부</b> 모읍니다.
     *
     * <p><b>값이 아니라 무엇을 적었는지를 봅니다.</b> 값으로 보면 같은 수를 자리마다
     * 적어 놓은 상태가 통과합니다 — 그것이 바로 막으려는 것입니다.
     */
    private void collect(Path file, List<Passed> found) throws IOException {
        String source = Files.readString(file);
        for (String call : CALLS) {
            Matcher m = Pattern.compile(Pattern.quote(call) + "\\s*([A-Za-z0-9_.]+)")
                    .matcher(source);
            while (m.find()) {
                found.add(new Passed(file.getFileName().toString(), call, m.group(1)));
            }
        }
    }
}

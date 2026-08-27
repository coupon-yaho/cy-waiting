package com.kafkick.waiting.architecture;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 쿠폰 키 상한은 <b>한 곳에서만 정한다</b> (6.3.5).
 *
 * <p>쿠폰별 표를 들고 있는 자리가 셋이다 — 판정 리미터, 격벽, 등록 래치. 상한을
 * 자리마다 적으면 사본이 갈라지고, 그때 한쪽만 찬 상태에서 판정이 어긋난다.
 *
 * <p>남용 리미터는 여기 안 든다. 그쪽은 쿠폰이 아니라 <b>클라이언트 식별자</b>로
 * 세므로 키 공간이 다르고, 상한을 같이 둘 이유가 없다.
 */
class CouponKeyBoundTest {

    /** 이 상한이 사는 곳. 세 자리 모두 여기를 가리켜야 한다. */
    private static final String HOME = "CouponKeys.MAX";

    /** 쿠폰으로 세는 자리들. 무엇을 넘겼는지를 첫 인자에서 본다. */
    private static final List<Site> SITES = List.of(
            new Site("src/main/java/com/kafkick/waiting/gateway/AdmissionGatewayFilter.java",
                    "Bulkhead.withMaxKeys("),
            new Site("src/main/java/com/kafkick/waiting/gateway/AdmissionGatewayFilter.java",
                    "EnqueueLatch.covering("),
            new Site("src/main/java/com/kafkick/waiting/gateway/IdentityConfig.java",
                    "SecondWindowLimiter.withMaxKeys("));

    private record Site(String file, String call) {
    }

    @Test
    @DisplayName("쿠폰으로_세는_자리는_모두_같은_상한을_쓴다")
    void 쿠폰으로_세는_자리는_모두_같은_상한을_쓴다() throws IOException {
        List<String> passed = new ArrayList<>();
        for (Site site : SITES) {
            passed.add(site.call() + firstArgument(site));
        }

        assertThat(passed)
                .describedAs("쿠폰 키 상한은 %s 한 곳에서만 정한다 — 사본이 갈라지면 "
                        + "리미터와 격벽이 서로 다른 상한을 쓰게 된다", HOME)
                .allSatisfy(call -> assertThat(call).endsWith(HOME));
    }

    /**
     * 첫 인자를 그대로 꺼냅니다.
     *
     * <p><b>값이 아니라 무엇을 적었는지를 봅니다.</b> 값으로 보면 같은 수를 자리마다
     * 적어 놓은 상태가 통과합니다 — 그것이 바로 막으려는 것입니다.
     */
    private String firstArgument(Site site) throws IOException {
        String source = Files.readString(Path.of(site.file()));
        Matcher m = Pattern.compile(Pattern.quote(site.call()) + "([A-Za-z0-9_.]+)")
                .matcher(source);
        assertThat(m.find())
                .describedAs("%s 에 %s 호출이 없다 — 시험이 없는 자리를 보고 있다",
                        site.file(), site.call())
                .isTrue();
        return m.group(1);
    }
}

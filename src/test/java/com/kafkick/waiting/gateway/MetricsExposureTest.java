package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.http.HttpMethod;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 지표를 밖에서 긁어 갈 수 있는가.
 *
 * <p><b>만드는 것과 내보내는 것은 다르다.</b> Micrometer 로 세고 있어도 수집기가
 * 읽을 자리가 없으면 아무도 못 본다. 그때 대시보드는 비어 있고, 사고 중에야 그
 * 사실을 안다.
 */
@Tag("context")
@SpringBootTest
class MetricsExposureTest {

    @Autowired
    private PrometheusMeterRegistry registry;

    @Autowired
    private RouteLocator locator;

    /** 레지스트리가 프로메테우스 형식으로 내놓아야 수집기가 읽는다. */
    @Test
    @DisplayName("프로메테우스_형식으로_긁힌다")
    void 프로메테우스_형식으로_긁힌다() {
        registry.counter("waiting.admission", "outcome", "PASS_UNDER_CAP", "cause", "none")
                .increment();

        assertThat(registry.scrape()).contains("waiting_admission_total");
    }

    /**
     * <b>레지스트리는 태그 키가 갈려도 안 막는다.</b> 실측으로 두 벌이 그대로
     * 나갔다 — 그걸 버리는 것은 수집기이고, 버려진 줄은 조용히 사라진다.
     * 그래서 등록이 거절되기를 기대하지 않고 <b>나가는 문자열</b>에서 직접 본다.
     */
    @Test
    @DisplayName("우리_지표는_이름마다_태그_키가_한_벌이다")
    void 우리_지표는_이름마다_태그_키가_한_벌이다() {
        registry.counter("waiting.admission", "outcome", "PASS_UNDER_CAP", "cause", "none")
                .increment();

        assertThat(태그_키_집합들())
                .containsKey("waiting_admission_total")
                .allSatisfy((name, keys) ->
                        assertThat(keys).as("%s 의 태그 키 집합", name).hasSize(1));
    }

    /** 나간 줄을 이름별로 모아 태그 키 집합이 몇 벌인지 센다. */
    private Map<String, Set<List<String>>> 태그_키_집합들() {
        Map<String, Set<List<String>>> 이름별 = new LinkedHashMap<>();
        Pattern 줄 = Pattern.compile("^(waiting_[^{ ]+)\\{([^}]*)}");
        registry.scrape().lines()
                .map(줄::matcher)
                .filter(Matcher::find)
                .forEach(m -> 이름별
                        .computeIfAbsent(m.group(1), ignored -> new LinkedHashSet<>())
                        .add(태그_키(m.group(2))));
        return 이름별;
    }

    private List<String> 태그_키(String 태그들) {
        return Arrays.stream(태그들.split(","))
                .map(t -> t.substring(0, t.indexOf('=')))
                .sorted()
                .toList();
    }

    /**
     * <b>관리 경로가 프록시 대상이면 안 된다.</b> 라우트가 잡으면 그 요청이 뒷단
     * 쿠폰 서비스로 가고, 지표는 밖에서 못 읽는데 뒷단만 이상한 요청을 받는다.
     */
    @Test
    @DisplayName("관리_경로는_라우트가_안_잡는다")
    void 관리_경로는_라우트가_안_잡는다() {
        ServerWebExchange 관리 = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.GET, "/actuator/prometheus"));

        assertThat(locator.getRoutes().collectList().block())
                .noneSatisfy(r -> assertThat(잡는가(r, 관리)).isTrue());
    }

    private boolean 잡는가(Route route, ServerWebExchange exchange) {
        return Boolean.TRUE.equals(Mono.from(route.getPredicate().apply(exchange)).block());
    }

    /**
     * <b>만들어 두고 안 걸면 지표가 안 나온다.</b> 단위 시험은 거는 코드가 맞는지만
     * 보고, 그 코드를 아무도 안 부르는 상황은 못 본다.
     */
    @Test
    @DisplayName("판정_재료_지표가_값과_함께_긁힌다")
    void 판정_재료_지표가_값과_함께_긁힌다() {
        // **이름만 보면 안 된다.** 게이지가 약한 참조로 등록되면 대상이 수거된
        // 뒤 NaN 을 내는데, 프로메테우스는 그 줄을 그대로 내보낸다. 이름만 재는
        // 시험은 세 지표가 전부 죽은 상태에서도 통과한다.
        System.gc();

        // **세 개를 다 짚는다.** 둘만 짚으면 나머지 하나가 등록에서 빠져도
        // 통과한다 — 실제로 나이 게이지를 지우고 돌려 봤더니 초록이었다.
        assertThat(registry.scrape())
                .containsPattern("waiting_queue_waiting\\{[^}]*\\} [0-9.E-]+\\n")
                .containsPattern("waiting_snapshot_coupons\\{[^}]*\\} [0-9.E-]+\\n")
                .containsPattern("waiting_bulkhead_in_flight\\{[^}]*\\} [0-9.E-]+\\n")
                .containsPattern("waiting_bulkhead_coupons\\{[^}]*\\} [0-9.E-]+\\n")
                .containsPattern("waiting_snapshot_age\\{[^}]*\\} [0-9.E-]+\\n")
                .doesNotContain("NaN");
    }
}

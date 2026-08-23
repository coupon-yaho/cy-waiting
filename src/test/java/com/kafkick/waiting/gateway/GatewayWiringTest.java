package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.gateway.handler.RoutePredicateHandlerMapping;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.web.reactive.function.server.support.RouterFunctionMapping;

/**
 * 라우트가 <b>실제로 뜬 컨텍스트에</b> 있는가.
 *
 * <p>단위로 만든 라우트 정의가 맞아도, 설정 이름이 어긋나거나 자동 구성이
 * 덮으면 뜬 애플리케이션에는 다른 것이 올라간다.
 */
@Tag("context")
@SpringBootTest
class GatewayWiringTest {

    @Autowired
    private RouteLocator locator;

    @Autowired
    private CorsPolicy.Origins origins;

    @Autowired
    private RouterFunctionMapping routerFunctionMapping;

    @Autowired
    private RoutePredicateHandlerMapping gatewayMapping;

    @Test
    @DisplayName("적은_라우트만_올라온다")
    void 적은_라우트만_올라온다() {
        // **정확히 같은지 본다.** 자동 구성이 하나 더 얹으면 프록시 범위가
        // 조용히 넓어지는데, 포함만 보면 그걸 못 잡는다.
        assertThat(locator.getRoutes().collectList().block())
                .extracting(Route::getId)
                .containsExactlyInAnyOrder("issue", "coupons");
    }

    @Test
    @DisplayName("순번_조회_핸들러가_게이트웨이보다_먼저_잡는다")
    void 순번_조회_핸들러가_게이트웨이보다_먼저_잡는다() {
        // 게이트웨이가 먼저 잡으면 폴링이 통째로 백엔드로 간다. 순서는 기본값에
        // 기대는 부분이라, 프레임워크가 바뀌면 조용히 뒤집힌다.
        assertThat(routerFunctionMapping.getOrder()).isLessThan(gatewayMapping.getOrder());
    }

    @Test
    @DisplayName("설정에_적은_오리진이_그대로_올라온다")
    void 설정에_적은_오리진이_그대로_올라온다() {
        // 설정 이름이 어긋나면 목록이 빈 채로 뜬다. 그러면 프론트가 통째로 막히는데
        // 기동은 성공한다.
        // 값까지 본다. 비지 않았는지만 보면 설정 파일의 오타가 그대로 통과한다.
        // 값까지 못 박는다. 비지 않았는지만 보면 엉뚱한 오리진이 들어가도 통과한다.
        assertThat(origins.allowed()).containsExactly("http://localhost:5173");
    }
}

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
    private GatewayRoutes.Backend backend;

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
    @DisplayName("라우터_함수가_게이트웨이보다_먼저_잡는다")
    void 라우터_함수가_게이트웨이보다_먼저_잡는다() {
        // **프레임워크가 정한 순서만 본다.** 우리 코드가 사이에 없으므로, 이건
        // 순번 조회가 안 새는 것을 재는 게 아니라 그 전제가 아직 성립하는지만
        // 본다. 안 새는 것은 라우트가 그 경로를 안 잡는 쪽이 잰다.
        //
        // 핸들러 자체는 아직 없다 — CY-402 가 붙일 때 이 순서 위에 선다.
        assertThat(routerFunctionMapping.getOrder()).isLessThan(gatewayMapping.getOrder());
    }

    @Test
    @DisplayName("라우트가_설정한_뒷단으로_간다")
    void 라우트가_설정한_뒷단으로_간다() {
        // id 만 보면 어디로 보내는지는 아무도 안 본다. 주소가 어긋나도 초록이다.
        assertThat(locator.getRoutes().collectList().block())
                .allSatisfy(r -> assertThat(r.getUri().toString())
                        .isEqualTo(backend.uri()));
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

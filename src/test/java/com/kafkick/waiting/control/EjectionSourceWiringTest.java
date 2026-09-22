package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.routing.RoutingProperties;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.StaticListableBeanFactory;

/**
 * 수집이 어느 배제를 보는가. <b>라우팅이 꺼졌으면 아무것도 안 뺀다</b> — 하트비트가 목록을
 * 안 싣는 것에 기대는 겹과 따로 둔다. 한 겹이 풀려도 예산이 조용히 깎이지 않게.
 */
@Tag("unit")
class EjectionSourceWiringTest {

    private final ControlPlaneConfig 배선 = new ControlPlaneConfig();

    private GatewayRegistry 과반이_x_를_뺐다() {
        GatewayRegistry registry = GatewayRegistry.of(3, 1);
        registry.ejectionObserved(3, Map.of("x", 2));
        return registry;
    }

    /** 기동 때 등록부는 비어 있다. 값을 그때 떠 두면 예산에서 영영 아무것도 안 뺀다. */
    @Test
    @DisplayName("공급자는_읽을_때마다_등록부를_본다")
    void 공급자는_읽을_때마다_등록부를_본다() {
        GatewayRegistry registry = GatewayRegistry.of(3, 1);
        var source = 배선.ejectionSource(라우팅(true).getBeanProvider(RoutingProperties.class), registry);

        registry.ejectionObserved(3, Map.of("x", 2));

        assertThat(source.get()).containsExactly("x");
    }

    private StaticListableBeanFactory 라우팅(boolean enabled) {
        StaticListableBeanFactory beans = new StaticListableBeanFactory();
        beans.addBean("routing", new RoutingProperties(enabled, null, null, null, null, null,
                null, null, List.of(".internal"), List.of(9000)));
        return beans;
    }

    @Test
    @DisplayName("라우팅이_켜졌으면_등록부의_배제를_본다")
    void 라우팅이_켜졌으면_등록부의_배제를_본다() {
        assertThat(배선.ejectionSource(라우팅(true).getBeanProvider(RoutingProperties.class),
                과반이_x_를_뺐다()).get()).containsExactly("x");
    }

    @Test
    @DisplayName("라우팅이_꺼졌으면_아무것도_안_뺀다")
    void 라우팅이_꺼졌으면_아무것도_안_뺀다() {
        assertThat(배선.ejectionSource(라우팅(false).getBeanProvider(RoutingProperties.class),
                과반이_x_를_뺐다()).get()).isEmpty();
        assertThat(배선.ejectionSource(
                new StaticListableBeanFactory().getBeanProvider(RoutingProperties.class),
                과반이_x_를_뺐다()).get()).as("노브 자체가 없다").isEmpty();
    }
}

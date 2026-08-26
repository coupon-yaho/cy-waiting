package com.kafkick.waiting.gateway;

import static org.springframework.web.reactive.function.server.RequestPredicates.path;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

/**
 * 서킷이 넘긴 요청을 받는 자리.
 *
 * <p><b>핸들러만 만들어 두면 오히려 위험하다.</b> 서킷 필터는 {@code forward:}
 * 로 넘길 뿐이라, 받는 라우트가 없으면 404 다. 코드에 핸들러가 보이므로 아무도
 * 그 경로를 의심하지 않고, 장애 때만 드러난다.
 */
@Configuration
public class BackendFallbackRoutes {

    /** 서킷 필터의 {@code fallbackUri} 가 가리키는 주소. 양쪽이 같아야 한다. */
    public static final String FALLBACK_ISSUE = "/fallback/issue";

    /**
     * <b>메서드로 좁히지 않는다.</b> {@code forward:} 는 원래 메서드를 그대로
     * 들고 오는데, 서킷은 발급(POST) 말고 다른 라우트에도 붙을 수 있다. 좁히면
     * 그때 이 자리가 조용히 404 로 돌아간다.
     */
    @Bean
    public RouterFunction<ServerResponse> fallbackRoutes(BackendFallback fallback) {
        return route(path(FALLBACK_ISSUE), fallback::respond);
    }

    /** 응답 시각은 주입받은 시계로 찍는다 — 판정 경로와 갈리면 안 된다 (TS-4). */
    @Bean
    public BackendFallback backendFallback(Clock clock, MeterRegistry meters) {
        return BackendFallback.of(clock, meters);
    }
}

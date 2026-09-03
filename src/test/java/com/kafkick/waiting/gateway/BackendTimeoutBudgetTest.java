package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;

/**
 * 뒷단 상한 둘을 <b>배포되는 파일에서</b> 읽어 못 박는다.
 *
 * <p>시험이 손으로 만든 값만 보면 운영으로 나가는 숫자를 아무도 안 본다.
 * 실제로 연결 상한을 11초로 바꿔도 스위트가 전건 초록이었다 — 30초 매달림을
 * 없애려고 넣은 값이 배포 설정 한 줄로 되돌아가도 CI 는 조용했다.
 */
class BackendTimeoutBudgetTest {

    private GatewayRoutes.Backend 값() throws IOException {
        List<PropertySource<?>> sources = new YamlPropertySourceLoader()
                .load("application", new ClassPathResource("application.yml"));
        return new Binder(ConfigurationPropertySources.from(sources))
                .bind("waiting.backend", GatewayRoutes.Backend.class)
                .orElseThrow(() -> new AssertionError("waiting.backend 가 없다"));
    }

    /**
     * <b>양쪽을 다 건다</b> (G9.4). 짧으면 첫 SYN 재전송(리눅스 1초) 전에 포기해
     * accept 큐가 잠깐 넘친 멀쩡한 인스턴스를 접고, 길면 죽은 인스턴스로 간
     * 요청이 그만큼 매달린다. 실측 전이라 등호로는 못 박는다 (TS-12).
     */
    @Test
    @DisplayName("연결_상한이_SYN_재전송_뒤이고_매달림_전이다")
    void 연결_상한이_SYN_재전송_뒤이고_매달림_전이다() throws IOException {
        assertThat(값().connectTimeout())
                .isGreaterThanOrEqualTo(Duration.ofSeconds(1))
                .isLessThanOrEqualTo(Duration.ofSeconds(4));
    }

    /**
     * <b>재시도까지 쳐도 응답 예산 안이어야 한다.</b> 연결 상한 두 번이 응답
     * 상한을 넘으면 두 번째 시도가 시작도 못 하고 끊긴다 — 재시도를 넣은 이유가
     * 사라지고, 그 사실은 죽은 인스턴스가 생긴 날에만 드러난다.
     */
    @Test
    @DisplayName("연결_상한_두_번이_응답_상한_안이다")
    void 연결_상한_두_번이_응답_상한_안이다() throws IOException {
        GatewayRoutes.Backend backend = 값();

        assertThat(backend.connectTimeout().multipliedBy(2))
                .as("연결 시도 두 번")
                .isLessThan(backend.responseTimeout());
    }
}

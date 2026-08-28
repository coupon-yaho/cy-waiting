package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 캐시가 차오르는 중인지를 <b>막히기 전에</b> 본다 (7.2.7).
 *
 * <p>상한에 닿으면 새 관찰을 못 받고, 그때부터 뒷단이 다시 다 맞습니다. 막힌
 * 뒤에 오르는 카운터로는 그 순간을 못 봅니다.
 */
class SoldOutCacheMetricsTest {

    private static final Instant 지금 = Instant.parse("2026-08-28T00:00:00Z");

    private final MeterRegistry meters = new SimpleMeterRegistry();

    private double 게이지(String name) {
        return meters.get(name).gauge().value();
    }

    @Test
    @DisplayName("담긴_수와_상한을_함께_낸다")
    void 담긴_수와_상한을_함께_낸다() {
        SoldOutCache 캐시 = SoldOutCache.of(Duration.ofSeconds(10), 7);
        캐시.bindMetrics(meters);

        캐시.observed("c1", 지금);
        캐시.observed("c2", 지금);

        assertThat(게이지("waiting.soldout.cache.size")).as("담긴 수").isEqualTo(2);
        // **상한을 같이 안 내면 담긴 수만으로는 여유를 모른다.** 7 이 큰지
        // 작은지는 상한을 봐야 알 수 있다.
        assertThat(게이지("waiting.soldout.cache.capacity")).as("상한").isEqualTo(7);
    }

    /** 게이지는 살아 있는 값이어야 합니다. 등록 시점 값이 박히면 늘 그 값입니다. */
    @Test
    @DisplayName("게이지가_뒤이은_변화를_따라간다")
    void 게이지가_뒤이은_변화를_따라간다() {
        SoldOutCache 캐시 = SoldOutCache.of(Duration.ofSeconds(10), 7);
        캐시.bindMetrics(meters);
        캐시.observed("c1", 지금);

        캐시.restocked("c1", 지금.plusSeconds(1));

        assertThat(게이지("waiting.soldout.cache.size")).isZero();
    }
}

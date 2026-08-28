package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.admission.CouponKeys;
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
 * 매진 캐시 설정을 <b>배포되는 파일에서</b> 읽어 못 박습니다.
 *
 * <p>시험이 손으로 만든 값만 보면 운영으로 나가는 숫자를 아무도 안 봅니다.
 */
class SoldOutCacheBudgetTest {

    private Binder 운영설정() throws IOException {
        List<PropertySource<?>> sources = new YamlPropertySourceLoader()
                .load("application", new ClassPathResource("application.yml"));
        return new Binder(ConfigurationPropertySources.from(sources));
    }

    private SoldOutCacheProperties 값() throws IOException {
        return 운영설정().bind("waiting.sold-out-cache", SoldOutCacheProperties.class)
                .orElseThrow(() -> new AssertionError("waiting.sold-out-cache 가 없다"));
    }

    /**
     * <b>양쪽을 다 겁니다.</b>
     *
     * <p>짧으면 안전판이 정상 경로를 앞질러 재입고를 보기도 전에 방패가
     * 풀립니다. 길면 해제 신호를 놓쳤을 때 재입고된 쿠폰이 그만큼 이 노드에서만
     * 안 팔립니다 — B-11 이 경고하는 방향이 그쪽입니다. 하한만 걸면 값이 위로
     * 새는 것을 아무도 안 막습니다.
     */
    @Test
    @DisplayName("TTL_이_낡음_한계와_그_스무_배_사이다")
    void TTL_이_낡음_한계와_그_스무_배_사이다() throws IOException {
        assertThat(값().ttl())
                .isGreaterThan(Duration.ofSeconds(5))
                .isLessThanOrEqualTo(Duration.ofSeconds(100));
    }

    /** 키가 클라이언트 입력에서 옵니다. 쿠폰 키 상한과 같은 값이라야 합니다. */
    @Test
    @DisplayName("키_상한이_쿠폰_키_상한과_같다")
    void 키_상한이_쿠폰_키_상한과_같다() throws IOException {
        assertThat(값().maxKeys()).isEqualTo(CouponKeys.MAX);
    }

    /** 배포 값이 코드가 거부하지 않는 값이어야 합니다. 안 그러면 기동에서 터집니다. */
    @Test
    @DisplayName("배포되는_값으로_캐시를_만들_수_있다")
    void 배포되는_값으로_캐시를_만들_수_있다() throws IOException {
        SoldOutCacheProperties props = 값();

        assertThat(SoldOutCache.of(props.ttl(), props.maxKeys()).size()).isZero();
    }
}

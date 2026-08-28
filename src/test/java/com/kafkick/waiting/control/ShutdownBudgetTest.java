package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

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
 * 종료 예산을 <b>배포되는 파일에서</b> 읽어 못 박는다.
 *
 * <p>시험 프로파일이 대기를 1ms 로 덮으므로, 덮은 값만 보면 운영으로 나가는 숫자를
 * 아무도 안 본다. 그러면 오타 하나가 기동까지 조용히 통과한다.
 */
class ShutdownBudgetTest {

    private Binder 운영설정() throws IOException {
        List<PropertySource<?>> sources = new YamlPropertySourceLoader()
                .load("application", new ClassPathResource("application.yml"));
        return new Binder(ConfigurationPropertySources.from(sources));
    }

    private Duration 값(String key) throws IOException {
        return 운영설정().bind(key, Duration.class)
                .orElseThrow(() -> new AssertionError(key + " 가 application.yml 에 없다"));
    }

    /**
     * 드레인 상한은 <b>컨테이너의 단계별 상한과 같아야</b> 합니다.
     *
     * <p>크면 우리가 기다리는 동안 컨테이너가 먼저 끊고, 작으면 아직 빠질 수 있는
     * 요청을 두고 "상한 초과" 라고 적습니다 — 어느 쪽이든 로그가 사실과 어긋납니다.
     */
    @Test
    @DisplayName("드레인_상한이_컨테이너_상한과_같다")
    void 드레인_상한이_컨테이너_상한과_같다() throws IOException {
        assertThat(값("waiting.shutdown.drain-limit"))
                .isEqualTo(값("spring.lifecycle.timeout-per-shutdown-phase"));
    }

    /**
     * <b>앞단 설정과 짝이다.</b> 앞단 체크가 2초 간격에 2회 실패로 제외하므로 4초,
     * 여기에 여유 2초다. 한쪽만 바꾸면 어긋난다.
     */
    @Test
    @DisplayName("LB_제외_대기는_앞단_체크_주기의_짝이다")
    void LB_제외_대기는_앞단_체크_주기의_짝이다() throws IOException {
        assertThat(값("waiting.shutdown.lb-removal-wait")).isEqualTo(Duration.ofSeconds(6));
    }

    /**
     * <b>기본값에 안 맡긴다.</b> 종료 예산을 계산하려면 이 값이 파일에 보여야 하고,
     * 기본값에 기대면 프레임워크가 바꿀 때 조용히 어긋난다.
     */
    @Test
    @DisplayName("드레인_상한을_기본값에_안_맡긴다")
    void 드레인_상한을_기본값에_안_맡긴다() throws IOException {
        assertThat(값("spring.lifecycle.timeout-per-shutdown-phase"))
                .isEqualTo(Duration.ofSeconds(30));
    }

    /**
     * <b>이 대기는 단계별 상한 밖이다.</b> 프레임워크가 못 끊으므로 코드가 건 상한을
     * 배포 값이 실제로 지키는지 여기서 본다 — 안 지키면 기동에서 터진다.
     */
    @Test
    @DisplayName("배포되는_대기_값이_코드_상한_안에_있다")
    void 배포되는_대기_값이_코드_상한_안에_있다() throws IOException {
        Duration 대기 = 값("waiting.shutdown.lb-removal-wait");

        assertThatCode(() -> DrainWait.of(ShutdownState.create(), 대기))
                .doesNotThrowAnyException();
    }
}

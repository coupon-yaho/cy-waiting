package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;

/**
 * 관리 엔드포인트를 <b>서비스 포트로 열지 않는다.</b>
 *
 * <p>부하 분산기는 관리 포트로 확인하고 서비스 포트로 보낸다. 한 포트로 묶으면
 * 밖에서 진단 정보와 종료 조작이 닿는다.
 *
 * <p>설정 파일을 직접 읽는다. 컨텍스트를 띄워 확인하면 무엇이 그 값을 만들었는지
 * 흐려지고, <b>정작 지켜야 할 것은 파일에 적힌 값</b>이다.
 */
class HealthWiringTest {

    private final PropertySource<?> 설정 = 설정을_읽는다();

    private PropertySource<?> 설정을_읽는다() {
        try {
            List<PropertySource<?>> sources = new YamlPropertySourceLoader()
                    .load("application", new ClassPathResource("application.yml"));
            return sources.getFirst();
        } catch (IOException e) {
            throw new IllegalStateException("설정을 못 읽었다", e);
        }
    }

    private Object 값(String key) {
        return 설정.getProperty(key);
    }

    @Test
    @DisplayName("관리_포트가_서비스_포트와_다르다")
    void 관리_포트가_서비스_포트와_다르다() {
        assertThat(값("management.server.port")).isNotNull()
                .isNotEqualTo(값("server.port"));
    }

    @Test
    @DisplayName("받는_판정_그룹이_판정_능력만_본다")
    void 받는_판정_그룹이_판정_능력만_본다() {
        // 레디스나 뒷단 상태를 넣으면 공유 장애가 전 노드 동시 이탈이 된다.
        // 그게 이 그룹을 따로 두는 이유다.
        assertThat(값("management.endpoint.health.group.readiness.include"))
                .isEqualTo("judging");
    }

    @Test
    @DisplayName("살아_있음_그룹이_루프만_본다")
    void 살아_있음_그룹이_루프만_본다() {
        // 여기에 의존성을 넣으면 레디스가 흔들릴 때 전 노드가 동시에 재기동한다.
        assertThat(값("management.endpoint.health.group.liveness.include"))
                .isEqualTo("loopAlive");
    }

    @Test
    @DisplayName("서비스_포트로_노출되는_것은_헬스뿐이다")
    void 서비스_포트로_노출되는_것은_헬스뿐이다() {
        assertThat(값("management.endpoints.web.exposure.include")).isEqualTo("health");
    }
}

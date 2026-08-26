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
        // 그룹을 직접 정의하면 프레임워크가 넣어 주던 것이 빠진다. 종료 신호가
        // 받는 판정에 안 닿으면 드레이닝이 주석으로만 존재하게 된다.
        assertThat(값("management.endpoint.health.group.readiness.include"))
                .isEqualTo("judging,readinessState");
    }

    @Test
    @DisplayName("살아_있음_그룹이_루프만_본다")
    void 살아_있음_그룹이_루프만_본다() {
        // 여기에 의존성을 넣으면 레디스가 흔들릴 때 전 노드가 동시에 재기동한다.
        assertThat(값("management.endpoint.health.group.liveness.include"))
                .isEqualTo("loopAlive");
    }

    @Test
    @DisplayName("관리_포트에_헬스와_지표만_노출한다")
    void 관리_포트에_헬스와_지표만_노출한다() {
        // **이 값은 관리 포트를 지배한다.** 포트를 나눈 뒤로는 서비스 포트와
        // 무관하므로, 서비스 포트 격리는 실제로 찔러 봐야 안다.
        //
        // **정확히 같은지 본다.** 목록이 늘어나면 `env`·`configprops` 같은 것이
        // 딸려 올라오는데, 관리 포트는 인증이 없어 클러스터 안에서 누구나 읽는다.
        // 지표를 연 것은 수집기가 읽을 자리가 필요해서다 — 세기만 하고 내보낼
        // 곳이 없으면 대시보드가 비고, 사고 중에야 그 사실을 안다.
        assertThat(값("management.endpoints.web.exposure.include"))
                .isEqualTo("health,prometheus");
    }

    @Test
    @DisplayName("진단은_그룹에만_싣는다")
    void 진단은_그룹에만_싣는다() {
        // 자동으로 올라오는 확인들이 레디스 주소와 배포 경로를 담는다. 관리
        // 포트는 인증이 없어 클러스터 안에서 누구나 읽는다.
        assertThat(값("management.endpoint.health.show-details")).isEqualTo("never");
        assertThat(값("management.endpoint.health.group.readiness.show-details"))
                .isEqualTo("always");
        assertThat(값("management.endpoint.health.group.liveness.show-details"))
                .isEqualTo("always");
    }

    @Test
    @DisplayName("종료_신호에_진행_중인_요청을_마친다")
    void 종료_신호에_진행_중인_요청을_마친다() {
        // 즉시 끊으면 부하 분산기가 아직 보내는 동안 도착한 요청이 커넥션째
        // 끊긴다. 배포마다 그만큼의 오류가 난다.
        assertThat(값("server.shutdown")).isEqualTo("graceful");
    }
}

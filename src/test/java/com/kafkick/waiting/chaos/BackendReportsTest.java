package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 백엔드 자기보고를 조작한다 (4.0.4) — 콜드 복귀를 재현한다.
 *
 * <p>재기동 직후 인스턴스는 커넥션 풀이 비어 <b>"유휴 200" 을 보고</b>하지만
 * 실제로는 JIT 콜드라 즉시 포화된다 (F6). 그 상태를 만들 수 없으면 램프업이
 * 동작하는지 확인할 방법이 없다.
 */
@Tag("chaos")
class BackendReportsTest {

    private RedisFaults redis;
    private StatefulRedisConnection<String, String> connection;
    private BackendReports reports;

    @BeforeEach
    void 준비() {
        redis = RedisFaults.시작한다();
        connection = redis.연결한다();
        reports = new BackendReports(connection, Duration.ofSeconds(10));
    }

    @AfterEach
    void 정리() {
        connection.close();
        redis.close();
    }

    @Test
    @DisplayName("신선한_보고만_합산된다")
    void 신선한_보고만_합산된다() {
        reports.보고한다("i-1", 100);
        reports.보고한다("i-2", 50);
        reports.낡은_보고를_심는다("i-3", 999, Duration.ofSeconds(30));

        assertThat(reports.신선한_총_가용량()).isEqualTo(150);
    }

    @Test
    @DisplayName("보고가_전부_낡으면_0이_나온다")
    void 보고가_전부_낡으면_0이_나온다() {
        // 0 이 나와야 하한 적용 경로(4.4.4)를 시험할 수 있다.
        // 여기서 임의로 하한을 넣으면 그 경로가 영영 안 돈다.
        reports.낡은_보고를_심는다("i-1", 100, Duration.ofSeconds(30));

        assertThat(reports.신선한_총_가용량()).isZero();
    }

    @Test
    @DisplayName("콜드_복귀를_재현한다")
    void 콜드_복귀를_재현한다() {
        // 방금 뜬 인스턴스가 제 여유를 크게 부른다 — F6 의 시작 지점이다.
        reports.콜드로_복귀한다("i-1", 200);

        Map<String, Long> 관측 = reports.신선한_보고();

        assertThat(관측).containsEntry("i-1", 200L);
        assertThat(reports.처음_관측된_시각("i-1")).isNotNegative();
    }

    @Test
    @DisplayName("깨진_보고는_격리된다")
    void 깨진_보고는_격리된다() {
        reports.보고한다("i-1", 100);
        reports.깨진_보고를_심는다("i-2");

        // 하나가 깨졌다고 나머지 판정을 버리면 장애 하나가 전면 차단이 된다.
        assertThat(reports.신선한_총_가용량()).isEqualTo(100);
    }
}

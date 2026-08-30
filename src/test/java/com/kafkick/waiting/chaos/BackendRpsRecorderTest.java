package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 뒷단이 실제로 받은 초당 건수를 잰다 (8.3.3 · RC4).
 *
 * <p>RC4 는 여섯 중 가장 중요하다. 나머지가 다 통과해도 이것이 깨지면 회복이
 * 곧 2차 장애다.
 */
// 게이트웨이가 보냈다고 세면 안 된다. 재는 것은 뒷단이 받은 수다 — 그 사이에
// 커넥션 풀과 재시도가 있어서 둘이 갈린다.
@Tag("chaos")
class BackendRpsRecorderTest {

    private static final Instant 시작 = Instant.parse("2026-08-30T00:00:00Z");

    @Test
    @DisplayName("받은_수를_초_단위로_센다")
    void 받은_수를_초_단위로_센다() {
        BackendRpsRecorder 기록 = new BackendRpsRecorder();
        기록.received(시작);
        기록.received(시작.plusMillis(500));
        기록.received(시작.plusSeconds(1));

        assertThat(기록.total()).isEqualTo(3);
        assertThat(기록.peakRps(시작, 시작.plusSeconds(2))).isEqualTo(2);
    }

    /** 구간 밖은 안 센다. 정상 구간과 회복 구간이 섞이면 비교가 성립하지 않는다. */
    @Test
    @DisplayName("구간_밖은_안_센다")
    void 구간_밖은_안_센다() {
        BackendRpsRecorder 기록 = new BackendRpsRecorder();
        기록.received(시작.minusSeconds(5));
        기록.received(시작.plusSeconds(1));

        assertThat(기록.peakRps(시작, 시작.plusSeconds(3))).isEqualTo(1);
    }

    /** 평균도 낸다. 버스트는 최댓값으로 보고 정상은 평균으로 본다. */
    @Test
    @DisplayName("구간_평균도_낸다")
    void 구간_평균도_낸다() {
        BackendRpsRecorder 기록 = new BackendRpsRecorder();
        for (int i = 0; i < 4; i++) {
            기록.received(시작.plusSeconds(i));
        }

        assertThat(기록.averageRps(시작, 시작.plusSeconds(4))).isEqualTo(1.0);
    }

    /** 아무것도 안 받은 구간의 평균은 0 이다. 나눗셈이 터지면 시나리오가 못 끝난다. */
    @Test
    @DisplayName("빈_구간의_평균은_영이다")
    void 빈_구간의_평균은_영이다() {
        assertThat(new BackendRpsRecorder().averageRps(시작, 시작.plusSeconds(3))).isZero();
    }

    /** 길이가 0 인 구간도 안 터진다. 시나리오가 구간을 잘못 주는 일이 있다. */
    @Test
    @DisplayName("길이가_없는_구간도_안_터진다")
    void 길이가_없는_구간도_안_터진다() {
        BackendRpsRecorder 기록 = new BackendRpsRecorder();
        기록.received(시작);

        assertThat(기록.averageRps(시작, 시작)).isZero();
        assertThat(기록.peakRps(시작, 시작)).isZero();
    }

    /**
     * <b>스텁이 센 수와 기록이 일치한다.</b>
     *
     * <p>기록기가 스텁과 따로 세면 둘이 갈리고, 그때 RC4 는 실제로 도착한 양이
     * 아니라 기록기가 믿는 양을 재게 된다.
     */
    @Test
    @DisplayName("스텁이_센_수와_일치한다")
    void 스텁이_센_수와_일치한다() {
        BackendRpsRecorder 기록 = new BackendRpsRecorder();
        int 스텁이_받은_수 = 0;

        for (int i = 0; i < 250; i++) {
            기록.received(시작.plusMillis(i * 7L));
            스텁이_받은_수++;
        }

        assertThat(기록.total()).isEqualTo(스텁이_받은_수);
    }

    /** RC4 판정에 그대로 넘길 수 있어야 한다. 여기서 다시 계산하면 기준이 갈린다. */
    @Test
    @DisplayName("회복_버스트_판정에_그대로_쓴다")
    void 회복_버스트_판정에_그대로_쓴다() {
        BackendRpsRecorder 정상 = new BackendRpsRecorder();
        BackendRpsRecorder 회복 = new BackendRpsRecorder();
        for (int i = 0; i < 10; i++) {
            정상.received(시작.plusSeconds(i));
        }
        for (int i = 0; i < 5; i++) {
            회복.received(시작.plusSeconds(20));
        }

        assertThat(RecoveryCriteria.recoveryBurst(
                정상.averageRps(시작, 시작.plusSeconds(10)),
                회복.peakRps(시작.plusSeconds(20), 시작.plusSeconds(21))))
                .hasValueSatisfying(v -> assertThat(v).contains("RC4"));
    }
}

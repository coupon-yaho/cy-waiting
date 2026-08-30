package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
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
//
// **요청마다 기록하지 않는다.** 재는 도구가 재는 대상에 부하를 주면 그 부하가
// 결과에 섞인다. 100K RPS 를 재는 자리라 실제로 문제가 된다. 이미 도는 계수를
// 1초마다 한 번 읽어 차분을 낸다 — 요청 경로의 비용이 0 이다.
@Tag("chaos")
class BackendRpsRecorderTest {

    private static final Instant 시작 = Instant.parse("2026-08-30T00:00:00Z");

    /** 스텁이 이미 세고 있는 누적값. 시험에서는 손으로 민다. */
    private final AtomicLong 스텁이_센_수 = new AtomicLong();

    private BackendRpsRecorder 기록기() {
        return new BackendRpsRecorder(스텁이_센_수::get);
    }

    /** 한 초 동안 몇 건이 늘었는지를 기록한다. */
    private void 초가_지난다(BackendRpsRecorder 기록, Instant 언제, long 그_초에_받은_수) {
        스텁이_센_수.addAndGet(그_초에_받은_수);
        기록.sample(언제);
    }

    @Test
    @DisplayName("초마다_차분을_기록한다")
    void 초마다_차분을_기록한다() {
        BackendRpsRecorder 기록 = 기록기();
        기록.sample(시작);
        초가_지난다(기록, 시작.plusSeconds(1), 2);
        초가_지난다(기록, 시작.plusSeconds(2), 1);

        assertThat(기록.total()).isEqualTo(3);
        assertThat(기록.peakRps(시작, 시작.plusSeconds(3))).isEqualTo(2);
    }

    /** 구간 밖은 안 센다. 정상 구간과 회복 구간이 섞이면 비교가 성립하지 않는다. */
    @Test
    @DisplayName("구간_밖은_안_센다")
    void 구간_밖은_안_센다() {
        BackendRpsRecorder 기록 = 기록기();
        기록.sample(시작.minusSeconds(5));
        초가_지난다(기록, 시작.minusSeconds(4), 99);
        초가_지난다(기록, 시작.plusSeconds(1), 1);

        assertThat(기록.peakRps(시작, 시작.plusSeconds(3))).isEqualTo(1);
    }

    /** 평균도 낸다. 버스트는 최댓값으로 보고 정상은 평균으로 본다. */
    @Test
    @DisplayName("구간_평균도_낸다")
    void 구간_평균도_낸다() {
        BackendRpsRecorder 기록 = 기록기();
        기록.sample(시작);
        for (int i = 1; i <= 4; i++) {
            초가_지난다(기록, 시작.plusSeconds(i), 1);
        }

        assertThat(기록.averageRps(시작.plusSeconds(1), 시작.plusSeconds(5))).isEqualTo(1.0);
    }

    /** 아무것도 안 받은 구간의 평균은 0 이다. 나눗셈이 터지면 시나리오가 못 끝난다. */
    @Test
    @DisplayName("빈_구간의_평균은_영이다")
    void 빈_구간의_평균은_영이다() {
        assertThat(기록기().averageRps(시작, 시작.plusSeconds(3))).isZero();
    }

    /** 길이가 0 인 구간도 안 터진다. 시나리오가 구간을 잘못 주는 일이 있다. */
    @Test
    @DisplayName("길이가_없는_구간도_안_터진다")
    void 길이가_없는_구간도_안_터진다() {
        BackendRpsRecorder 기록 = 기록기();
        기록.sample(시작);
        초가_지난다(기록, 시작.plusSeconds(1), 5);

        assertThat(기록.averageRps(시작, 시작)).isZero();
        assertThat(기록.peakRps(시작, 시작)).isZero();
    }

    /**
     * <b>스텁이 센 수와 기록이 일치한다.</b>
     *
     * <p>기록기가 따로 세면 둘이 갈리고, 그때 RC4 는 실제로 도착한 양이 아니라
     * 기록기가 믿는 양을 재게 된다. 차분으로 내면 갈릴 수가 없다.
     */
    @Test
    @DisplayName("스텁이_센_수와_일치한다")
    void 스텁이_센_수와_일치한다() {
        BackendRpsRecorder 기록 = 기록기();
        기록.sample(시작);
        for (int i = 1; i <= 10; i++) {
            초가_지난다(기록, 시작.plusSeconds(i), 25);
        }

        assertThat(기록.total()).isEqualTo(스텁이_센_수.get());
    }

    /** 되돌아간 누적값은 안 센다. 스텁이 재시작하면 0 부터 다시 오른다. */
    @Test
    @DisplayName("누적값이_되돌아가면_안_센다")
    void 누적값이_되돌아가면_안_센다() {
        BackendRpsRecorder 기록 = 기록기();
        기록.sample(시작);
        초가_지난다(기록, 시작.plusSeconds(1), 10);
        스텁이_센_수.set(0);
        기록.sample(시작.plusSeconds(2));

        assertThat(기록.peakRps(시작, 시작.plusSeconds(3))).isEqualTo(10);
        // **총합까지 본다.** 봉우리만 보면 음수가 다른 버킷에 얹혀도 안 잡힌다 —
        // 그러면 되돌아간 값이 조용히 총합을 깎는다.
        assertThat(기록.total()).as("음수 차분이 총합을 안 깎는다").isEqualTo(10);
        assertThat(기록.averageRps(시작, 시작.plusSeconds(3)))
                .as("평균도 음수에 안 끌린다").isGreaterThan(0);
    }

    /** RC4 판정에 그대로 넘길 수 있어야 한다. 여기서 다시 계산하면 기준이 갈린다. */
    @Test
    @DisplayName("회복_버스트_판정에_그대로_쓴다")
    void 회복_버스트_판정에_그대로_쓴다() {
        BackendRpsRecorder 기록 = 기록기();
        기록.sample(시작);
        for (int i = 1; i <= 10; i++) {
            초가_지난다(기록, 시작.plusSeconds(i), 10);
        }
        초가_지난다(기록, 시작.plusSeconds(11), 30);

        assertThat(RecoveryCriteria.recoveryBurst(
                기록.averageRps(시작.plusSeconds(1), 시작.plusSeconds(11)),
                기록.peakRps(시작.plusSeconds(11), 시작.plusSeconds(12))))
                .hasValueSatisfying(v -> assertThat(v).contains("RC4"));
    }
}

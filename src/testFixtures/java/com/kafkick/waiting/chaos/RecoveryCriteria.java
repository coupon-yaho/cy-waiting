package com.kafkick.waiting.chaos;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * 회복 판정의 공통 기준 RC1~RC6 (계획 08-resilience 4절).
 *
 * <p><b>시나리오마다 판정을 다시 쓰지 않는다.</b> 다시 쓰면 "전 시나리오 초과
 * 발급 0" 같은 게이트가 시나리오마다 다른 것을 재게 된다.
 *
 * <p>각 판정기는 <b>위반일 때만</b> 사유를 돌려준다. 통과를 값으로 돌려주면
 * 부르는 쪽이 그것을 안 보고 넘겨도 초록이다.
 */
public final class RecoveryCriteria {

    /** 회복 버스트의 허용 배수. 이보다 크면 회복이 곧 2차 장애다. */
    public static final double BURST_LIMIT = 1.2;

    /** 지표가 돌아왔다고 볼 오차. 정확히 같기를 요구하면 EWMA 가 영영 못 닿는다. */
    public static final double CONVERGENCE_TOLERANCE = 0.10;

    private RecoveryCriteria() {
    }

    /**
     * RC1 — 초과 발급 0.
     *
     * @param issued 장애 전·중·후를 합산한 발급 수
     * @param stock  처음 재고
     */
    public static Optional<String> overIssued(long issued, long stock) {
        if (stock < 0) {
            throw new IllegalArgumentException("재고는 0 이상이어야 한다: %d".formatted(stock));
        }
        // 미달은 지연이지 사고가 아니다. 넘은 것만 본다.
        return issued <= stock ? Optional.empty()
                : Optional.of("RC1 초과 발급 — 재고 %d 인데 %d 건 나갔다".formatted(stock, issued));
    }

    /**
     * RC2 — 순번 역행 0.
     *
     * @param ranks 한 사용자가 시간 순으로 받은 순번. 같은 값이 이어지는 것은
     *              배분이 안 돈 틱이라 역행이 아니다
     */
    public static Optional<String> rankRegressed(List<Long> ranks) {
        for (int i = 1; i < ranks.size(); i++) {
            long before = ranks.get(i - 1);
            long now = ranks.get(i);
            if (now > before) {
                return Optional.of(
                        "RC2 순번 역행 — %d 번째에 %d 에서 %d 로 밀렸다".formatted(i, before, now));
            }
        }
        return Optional.empty();
    }

    /**
     * RC3 — 회복 뒤 판정 분포가 한계 안에 돌아온다.
     *
     * @param returnedAfter 회복 시점부터 정상 분포까지 걸린 시간.
     *                      {@code null} 이면 끝내 안 돌아왔다는 뜻이다
     */
    public static Optional<String> slowVerdictReturn(Duration returnedAfter, Duration limit) {
        if (returnedAfter == null) {
            return Optional.of("RC3 판정 분포가 끝내 안 돌아왔다");
        }
        return returnedAfter.compareTo(limit) <= 0 ? Optional.empty()
                : Optional.of("RC3 판정 복귀가 늦다 — %s 걸렸다 (한계 %s)"
                        .formatted(returnedAfter, limit));
    }

    /**
     * RC4 — 회복 순간의 뒷단 유입이 정상의 1.2배를 안 넘는다.
     *
     * <p><b>여섯 중 가장 중요하다.</b> 나머지가 다 통과해도 이것이 깨지면
     * 회복이 곧 2차 장애다.
     *
     * @param baselineRps 정상 구간의 뒷단 수신 RPS
     * @param peakRps     회복 직후의 최대 수신 RPS
     */
    public static Optional<String> recoveryBurst(double baselineRps, double peakRps) {
        // **못 잰 것을 통과로 넘기지 않는다.** 비교 대상이 없으면 이 기준은
        // 아무것도 안 재는 것이고, 그러면 게이트가 사라진 채로 초록이다.
        if (baselineRps <= 0) {
            return Optional.of("RC4 정상 구간 RPS 를 못 쟀다 — 버스트를 판정할 수 없다");
        }
        double ratio = peakRps / baselineRps;
        return ratio <= BURST_LIMIT ? Optional.empty()
                : Optional.of("RC4 회복 버스트 %.2f 배 (한계 %.1f)".formatted(ratio, BURST_LIMIT));
    }

    /**
     * RC5 — 장애 중 줄에 있던 사람이 자기 자리를 지킨다.
     *
     * @param before 장애 전 사용자별 score
     * @param after  회복 뒤 같은 순서의 score
     */
    public static Optional<String> seatLost(List<Double> before, List<Double> after) {
        // 수가 줄었으면 누군가 걷힌 것이다. 재입장은 새 score 라 순번 역행이다.
        if (after.size() < before.size()) {
            return Optional.of("RC5 줄에서 %d 명이 사라졌다".formatted(before.size() - after.size()));
        }
        for (int i = 0; i < before.size(); i++) {
            if (!before.get(i).equals(after.get(i))) {
                return Optional.of("RC5 자리를 잃었다 — %d 번째가 %s 에서 %s 로 바뀌었다"
                        .formatted(i, before.get(i), after.get(i)));
            }
        }
        return Optional.empty();
    }

    /**
     * RC6 — 지표가 장애 이전 값으로 수렴한다.
     *
     * <p>양쪽을 다 본다. 위로만 보면 회복 뒤에 값이 주저앉은 것을 놓친다.
     */
    public static Optional<String> notConverged(String name, double before, double after) {
        if (before == 0) {
            return after == 0 ? Optional.empty()
                    : Optional.of("RC6 %s 가 0 에서 %.2f 로 벌어졌다".formatted(name, after));
        }
        double drift = Math.abs(after - before) / Math.abs(before);
        return drift <= CONVERGENCE_TOLERANCE ? Optional.empty()
                : Optional.of("RC6 %s 가 안 수렴했다 — %.2f → %.2f (오차 %.0f%%)"
                        .formatted(name, before, after, drift * 100));
    }

    /** 여러 판정을 모은다. 깨진 것의 이름이 남아야 원인을 찾는다. */
    @SafeVarargs
    public static List<String> violations(Optional<String>... judged) {
        List<String> found = new ArrayList<>();
        Arrays.stream(judged).forEach(one -> one.ifPresent(found::add));
        return List.copyOf(found);
    }
}

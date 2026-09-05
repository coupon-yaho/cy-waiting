package com.kafkick.waiting.chaos;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

    /**
     * 중복 발신의 한계. <b>버스트와 다른 기준이라 상수를 안 나눠 쓴다</b> —
     * 여기서 1 건 초과는 발급 요청 1 건 중복이고, 그건 곧 초과 발급이다.
     */
    public static final double DUPLICATE_LIMIT = 1.0;

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
        // **음수는 관측이 아니다.** 재고보다 작다는 이유로 통과시키면 세다가
        // 깨진 실행이 "초과 발급 0" 으로 기록된다.
        if (issued < 0) {
            return Optional.of("RC1 발급 수를 못 쟀다: %d".formatted(issued));
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
        // **빈 목록은 통과가 아니다.** 루프가 안 돌아 빈 값이 나가는데, 그것은
        // "역행이 없었다" 가 아니라 "아무것도 안 넘겼다" 다.
        //
        // **한 개짜리는 막지 않는다.** 한 사람을 한 번만 본 것은 정상이다 —
        // 그 사람에게 역행이 없었을 뿐이다. "두 번 이상 본 사람이 하나도 없다"
        // 는 모으는 쪽(RankTracker)이 본다.
        if (ranks == null || ranks.isEmpty()) {
            return Optional.of("RC2 순번을 하나도 안 넘겼다 — 비교할 것이 없다");
        }
        for (int i = 1; i < ranks.size(); i++) {
            Long beforeBoxed = ranks.get(i - 1);
            Long nowBoxed = ranks.get(i);
            if (beforeBoxed == null || nowBoxed == null) {
                return Optional.of("RC2 순번이 빈 자리가 있다 — %d 번째".formatted(i));
            }
            long before = beforeBoxed;
            long now = nowBoxed;
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
        // 음수 경과는 측정이 깨진 것이다. 한계보다 작다고 통과시키면 못 잰
        // 실행이 "빨리 돌아왔다" 로 기록된다.
        if (returnedAfter.isNegative()) {
            return Optional.of("RC3 복귀 시간을 못 쟀다: %s".formatted(returnedAfter));
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
        //
        // 양쪽을 다 본다. 정상값만 막으면 음수 봉우리가 음수 비율을 내어 한계
        // 아래로 통과하고, 무한대 정상값은 어떤 봉우리도 0 으로 보이게 한다.
        if (!Double.isFinite(baselineRps) || baselineRps <= 0) {
            return Optional.of("RC4 정상 구간 RPS 를 못 쟀다 — 버스트를 판정할 수 없다");
        }
        if (!Double.isFinite(peakRps) || peakRps < 0) {
            return Optional.of("RC4 회복 구간 RPS 를 못 쟀다: %s".formatted(peakRps));
        }
        // **0 은 버스트가 없는 것이 아니라 뒷단이 하나도 못 받은 것이다.**
        // 나눗셈만 두면 0 이 가장 조용히 통과한다 — 아직 안 돌아온 실행이
        // 가장 잘 돌아온 실행으로 읽힌다.
        if (peakRps == 0) {
            return Optional.of("RC4 회복 구간에 뒷단이 하나도 안 받았다");
        }
        double ratio = peakRps / baselineRps;
        return ratio <= BURST_LIMIT ? Optional.empty()
                : Optional.of("RC4 회복 버스트 %.2f 배 (한계 %.1f)".formatted(ratio, BURST_LIMIT));
    }

    /**
     * <b>뒷단 도착이 클라이언트가 보낸 수를 넘으면 게이트웨이가 스스로 만든
     * 유입이다.</b> 발급 경로에서는 그 1 건이 곧 초과 발급이다.
     *
     * @param sent    시험이 그 구간에 보낸 요청 수
     * @param arrived 같은 구간에 뒷단이 받은 수
     */
    // **버스트가 아니라 중복을 잰다.** 닫힌 루프로 부하를 만드는 하네스에서는
    // 발신 속도가 게이트웨이 지연으로 정해지므로, 게이트웨이가 몰아쳐도 시험이
    // 같이 빨라질 뿐 비율이 안 움직인다. 버스트를 재려면 고정 속도로 쏘는
    // 열린 루프가 필요하다.
    //
    // 대신 이 비는 재전송·풀 재시도로 요청이 불어나는 것을 잡는다. 한계가
    // 1.0 인 이유이자, 분모가 커져도 감도가 안 떨어지는 이유다.
    public static Optional<String> amplified(long sent, long arrived) {
        if (sent <= 0) {
            return Optional.of("RC4 보낸 수를 못 쟀다 — 증폭을 판정할 수 없다");
        }
        if (arrived < 0) {
            return Optional.of("RC4 뒷단 도착 수를 못 쟀다: %d".formatted(arrived));
        }
        // **보냈는데 하나도 안 닿았으면 위반이다.** 비율 0 은 "증폭 없음" 으로
        // 읽혀 가장 조용히 통과한다 — 봉우리 0 을 막은 것과 같은 상황이다.
        if (arrived == 0) {
            return Optional.of("RC4 회복 구간에 보낸 %d 건이 뒷단에 하나도 안 닿았다"
                    .formatted(sent));
        }
        double ratio = (double) arrived / sent;
        return ratio <= DUPLICATE_LIMIT ? Optional.empty()
                : Optional.of("RC4 회복 증폭 %.2f 배 — 보낸 %d, 도착 %d (한계 %.1f)"
                        .formatted(ratio, sent, arrived, DUPLICATE_LIMIT));
    }

    /**
     * RC5 — 장애 중 줄에 있던 사람이 자기 자리를 지킨다.
     *
     * @param before 장애 전 사용자별 score
     * @param after  회복 뒤 사용자별 score
     */
    // **사람으로 짚는다.** score 목록만 비교하면, 같은 값을 가진 사람이 여럿일 때
    // 목록은 같은데 주인이 바뀐 것을 못 본다. 새 사람이 느는 것은 기존 사람의
    // 자리와 무관하므로 위반이 아니다.
    public static Optional<String> seatLost(Map<String, Double> before,
            Map<String, Double> after) {
        // **장애 전 자리를 하나도 못 모았으면 못 잰 것이다.** 비교 대상이 없으면
        // 루프가 안 돌아 빈 값이 나가고, 줄에 아무도 없던 회차와 줄을 못 읽은
        // 회차가 같은 값을 낸다.
        if (before == null || before.isEmpty()) {
            return Optional.of("RC5 장애 전 자리를 못 모았다 — 비교할 것이 없다");
        }
        for (Map.Entry<String, Double> was : before.entrySet()) {
            Double now = after.get(was.getKey());
            if (now == null) {
                return Optional.of("RC5 줄에서 사라졌다 — %s".formatted(was.getKey()));
            }
            if (!now.equals(was.getValue())) {
                return Optional.of("RC5 자리를 잃었다 — %s 가 %s 에서 %s 로 옮겨졌다"
                        .formatted(was.getKey(), was.getValue(), now));
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

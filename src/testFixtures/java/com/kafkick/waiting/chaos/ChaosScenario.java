package com.kafkick.waiting.chaos;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * 카오스 시나리오의 3단계 뼈대 (8.0.3).
 *
 * <p>정상·장애·회복을 따로 지나고 구간마다 판정을 건다.
 */
// 판정 기준은 시나리오 밖에 둔다. 안에 두면 시나리오마다 기준이 갈리고, 그때부터
// "전 시나리오 초과 발급 0" 같은 게이트가 이름만 남는다. RecoveryCriteria 를
// 그대로 받아 쓴다.
public final class ChaosScenario {

    /** 한 구간의 판정. 위반 사유만 돌려준다 — 통과는 빈 목록이다. */
    @FunctionalInterface
    public interface Verdict {

        List<String> judge();

        /** 이 구간은 일부러 안 잰다. <b>빠뜨린 것과 구분하려고 적는다.</b> */
        static Verdict none() {
            return List::of;
        }
    }

    private final String name;

    private Runnable baseline = () -> { };
    private Runnable inject;
    private Runnable duringFault = () -> { };
    private Runnable recover;
    private Runnable afterRecovery = () -> { };

    private Verdict entry;
    private Verdict during;
    private Verdict recovery;

    private ChaosScenario(String name) {
        this.name = name;
    }

    /** 이름이 없으면 어느 시나리오가 깨졌는지 못 찾는다. */
    public static ChaosScenario named(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("시나리오 이름은 필수다");
        }
        return new ChaosScenario(name);
    }

    /** 정상 지표를 모은다. <b>주입보다 먼저다</b> — 뒤면 비교 기준이 이미 장애다. */
    public ChaosScenario baseline(Runnable step) {
        this.baseline = Objects.requireNonNull(step, "baseline 은 필수다");
        return this;
    }

    public ChaosScenario inject(Runnable step) {
        this.inject = Objects.requireNonNull(step, "inject 는 필수다");
        return this;
    }

    public ChaosScenario duringFault(Runnable step) {
        this.duringFault = Objects.requireNonNull(step, "duringFault 는 필수다");
        return this;
    }

    public ChaosScenario recover(Runnable step) {
        this.recover = Objects.requireNonNull(step, "recover 는 필수다");
        return this;
    }

    public ChaosScenario afterRecovery(Runnable step) {
        this.afterRecovery = Objects.requireNonNull(step, "afterRecovery 는 필수다");
        return this;
    }

    public ChaosScenario assertEntry(Verdict verdict) {
        this.entry = Objects.requireNonNull(verdict, "entry 판정은 필수다");
        return this;
    }

    public ChaosScenario assertDuring(Verdict verdict) {
        this.during = Objects.requireNonNull(verdict, "during 판정은 필수다");
        return this;
    }

    public ChaosScenario assertRecovery(Verdict verdict) {
        this.recovery = Objects.requireNonNull(verdict, "recovery 판정은 필수다");
        return this;
    }

    /**
     * 세 구간을 지나고 모은 위반을 한 번에 터뜨린다.
     *
     * <p>깨져도 복구는 돈다. 중간에 멈추면 장애를 켠 채 끝나고, 다음 시험이 그
     * 상태를 물려받아 원인이 엉뚱한 곳에서 드러난다.
     */
    public void run() {
        if (inject != null && recover == null) {
            throw new IllegalStateException("장애를 주입하면 복구도 정해야 한다: " + name);
        }
        // **판정을 안 건 구간이 있으면 거절한다.** 기본값을 통과로 두면 아무것도
        // 안 재는 시나리오가 초록이 된다 — 이 뼈대가 막겠다던 실패 모드다.
        if (entry == null || during == null || recovery == null) {
            throw new IllegalStateException("판정을 안 건 구간이 있다: " + name);
        }
        List<String> broken = new ArrayList<>();
        baseline.run();
        try {
            if (inject != null) {
                inject.run();
            }
            // **판정은 그 구간이 살아 있는 동안 돈다.** 복구 뒤로 미루면 이미
            // 걷힌 상태를 읽는다 — dataStale 진입도, 서킷이 열린 동안의 유입도
            // 그때는 정상으로 보인다. 유지 구간 판정이 통째로 이름만 남는다.
            label(broken, "진입", entry);
            duringFault.run();
            label(broken, "유지", during);
        } finally {
            if (recover != null) {
                // **복구가 터져도 모은 위반을 안 잃는다.** 여기서 예외가 나가면
                // 아래 throw 에 못 닿고, 초과 발급이 "복구 스텝이 불안정하다" 로
                // 읽힌다.
                try {
                    recover.run();
                } catch (RuntimeException e) {
                    broken.add("  복구 — 장애를 걷다가 터졌다: " + e);
                }
            }
        }
        afterRecovery.run();
        label(broken, "회복", recovery);
        if (!broken.isEmpty()) {
            throw new AssertionError("[%s] 깨진 기준 %d 건%n%s"
                    .formatted(name, broken.size(), String.join(System.lineSeparator(), broken)));
        }
    }

    // 어느 구간에서 깨졌는지를 붙인다. 사유만 있으면 같은 값이 세 구간 중
    // 어디서 나왔는지 못 가린다.
    private void label(List<String> into, String phase, Verdict verdict) {
        verdict.judge().forEach(one -> into.add("  " + phase + " — " + one));
    }
}

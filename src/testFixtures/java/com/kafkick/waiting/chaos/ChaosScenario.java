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
        // **어느 단계에서 터져도 모은 위반을 안 잃는다.** 스텝 예외가 그대로
        // 나가면 초과 발급이 "부하 스텝이 불안정하다" 로 읽힌다. 예외도 위반의
        // 하나로 담고, 판정은 끝까지 돈다.
        List<String> broken = new ArrayList<>();
        step(broken, "정상", baseline);
        try {
            if (inject != null) {
                step(broken, "주입", inject);
            }
            // **판정은 그 구간이 살아 있는 동안 돈다.** 복구 뒤로 미루면 이미
            // 걷힌 상태를 읽는다 — dataStale 진입도, 서킷이 열린 동안의 유입도
            // 그때는 정상으로 보인다. 유지 구간 판정이 통째로 이름만 남는다.
            label(broken, "진입", entry);
            step(broken, "유지", duringFault);
            label(broken, "유지", during);
        } finally {
            if (recover != null) {
                step(broken, "복구", recover);
            }
        }
        step(broken, "회복", afterRecovery);
        label(broken, "회복", recovery);
        if (!broken.isEmpty()) {
            throw new AssertionError("[%s] 깨진 기준 %d 건%n%s"
                    .formatted(name, broken.size(), String.join(System.lineSeparator(), broken)));
        }
    }

    /**
     * 한 단계를 돌리고, 터지면 그것도 위반으로 담는다.
     *
     * <p><b>{@code Throwable} 을 잡는다.</b> {@code RuntimeException} 만 잡으면
     * 시험용 콜백이 던진 {@code AssertionError} 가 그대로 나가, 이미 모은 기준
     * 위반을 전부 가린다.
     */
    private void step(List<String> into, String phase, Runnable body) {
        try {
            body.run();
        } catch (Throwable e) {
            치명적이면_다시_던진다(e);
            into.add("  " + phase + " — 단계가 터졌다: " + e);
        }
    }

    /**
     * <b>회복이 불가능한 것은 안 삼킨다.</b>
     *
     * <p>메모리가 없거나 스택이 넘친 뒤에 남은 단계를 계속 도는 것은 아무 뜻이
     * 없고, 원래 실패가 보고서 문장 뒤로 숨는다.
     */
    // **`Error` 를 통째로 던지면 안 된다.** `AssertionError` 도 `Error` 인데,
    // 단계 안의 단언을 위반으로 담는 것이 이 자리의 존재 이유다.
    private static void 치명적이면_다시_던진다(Throwable e) {
        if (e instanceof VirtualMachineError || e instanceof LinkageError) {
            throw (Error) e;
        }
    }

    // 어느 구간에서 깨졌는지를 붙인다. 사유만 있으면 같은 값이 세 구간 중
    // 어디서 나왔는지 못 가린다.
    //
    // **판정이 던져도 앞의 위반을 안 잃는다.** 단계와 같은 이유다 — 판정 하나가
    // NPE 를 내면 그때까지 모은 것이 통째로 가려지고, 보고서에는 그 예외만
    // 남아 원인이 엉뚱한 곳을 가리킨다.
    private void label(List<String> into, String phase, Verdict verdict) {
        try {
            verdict.judge().forEach(one -> into.add("  " + phase + " — " + one));
        } catch (Throwable e) {
            치명적이면_다시_던진다(e);
            into.add("  " + phase + " — 판정이 터졌다: " + e);
        }
    }
}

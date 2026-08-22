package com.kafkick.waiting.control;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * 이어지는 실패를 한 번만 알리고, 걷힐 때 얼마나 오래 몇 판이었는지 남긴다.
 *
 * <p>매 판 찍으면 <b>정작 조사가 필요한 순간에 로그가 묻힌다.</b> 그렇다고
 * 지속 시간만 남기면 한 판이 실패한 것인지 수백 판인지 사후에 못 가린다.
 */
public final class FailureWindow {

    /** 따로 두면 비우는 것과 읽는 것 사이에 들어온 실패가 어느 쪽에도 안 들어간다. */
    private record Failing(long since, int swallowed) {
    }

    private final AtomicReference<Failing> state = new AtomicReference<>();
    private final LongSupplier nanoTicker;

    private FailureWindow(LongSupplier nanoTicker) {
        this.nanoTicker = Objects.requireNonNull(nanoTicker, "nanoTicker 는 필수다");
    }

    public static FailureWindow of(LongSupplier nanoTicker) {
        return new FailureWindow(nanoTicker);
    }

    public static FailureWindow create() {
        return new FailureWindow(System::nanoTime);
    }

    /** 이 실패가 <b>구간의 시작</b>이면 참. 이어지는 실패는 거짓이고 세기만 한다. */
    public boolean entered() {
        long now = nanoTicker.getAsLong();
        Failing before = state.getAndUpdate(failing -> failing == null
                ? new Failing(now, 1)
                : new Failing(failing.since(), failing.swallowed() + 1));
        return before == null;
    }

    /** 실패 구간이 있었으면 그 요약. 없었으면 비어 있다. */
    public Optional<Recovered> exited() {
        Failing before = state.getAndSet(null);
        return before == null
                ? Optional.empty()
                : Optional.of(new Recovered(nanoTicker.getAsLong() - before.since(),
                        before.swallowed()));
    }

    /**
     * @param elapsedNanos 실패가 이어진 시간
     * @param swallowed    그동안 삼킨 판의 수
     */
    public record Recovered(long elapsedNanos, int swallowed) {

        public long elapsedSeconds() {
            return NANOSECONDS.toSeconds(elapsedNanos);
        }
    }
}

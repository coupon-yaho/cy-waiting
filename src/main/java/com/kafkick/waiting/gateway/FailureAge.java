package com.kafkick.waiting.gateway;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 실패가 <b>얼마나 이어졌는지</b>를 든다 (F7).
 *
 * <p>백오프 단계를 요청 수로 세면 안 된다. 피크에서는 한 노드가 초당 수천 건을
 * 처리하므로, 레디스가 끊긴 순간 카운터가 밀리초 만에 상한에 닿는다. 첫 실패자와
 * 1초 뒤 실패자가 같은 안내를 받으면 백오프가 있으나 마나다.
 */
public final class FailureAge {

    /** 실패가 시작된 시각. {@code null} 이면 지금은 성공 중이다. */
    private final AtomicReference<Instant> since = new AtomicReference<>();

    /**
     * 지금의 백오프 단계. <b>1 부터 센다.</b>
     *
     * @param now  지금
     * @param unit 한 계단의 폭
     */
    public int stepAt(Instant now, Duration unit) {
        Instant began = since.compareAndExchange(null, now);
        if (began == null) {
            return 1;
        }
        long elapsed = Duration.between(began, now).toMillis();
        // **음수를 안 나눈다.** 복제본 승격이나 NTP 보정으로 시각이 되돌아가면
        // 경과가 음수가 되고, 그대로 나누면 단계가 상한 아래로 떨어진다.
        if (elapsed <= 0) {
            return 1;
        }
        long step = 1 + elapsed / Math.max(1, unit.toMillis());
        return (int) Math.min(step, Integer.MAX_VALUE);
    }

    /** 성공했다. 안 지우면 회복한 뒤에도 멀리 보낸다. */
    public void cleared() {
        since.set(null);
    }
}

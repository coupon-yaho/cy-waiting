package com.kafkick.waiting.control;

import java.time.Duration;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 승계 첫 회차를 앞 리더의 마지막 발행에서 한 틱 떨어뜨린다. <b>정상 인계는 틈이 짧다</b> —
 * 새 리더가 곧 첫 회차를 돌면 두 리더의 한 틱 몫이 1초 안에 겹쳐 뒷단 유입이 두 배가 된다.
 * 리더가 죽은 승계는 마지막 발행이 이미 리스 넘게 지나 기다리지 않는다.
 */
public final class HandoverSpacing implements BooleanSupplier {

    private static final Logger log = LoggerFactory.getLogger(HandoverSpacing.class);

    /** 발행 시각이 초 단위로 실린다. 실제 발행은 그 초 안 어디쯤이라 한 초를 더한다. */
    private static final Duration PUBLISHED_ROUNDING = Duration.ofSeconds(1);

    private final LongSupplier nanoTicker;
    private final Duration ceiling;

    /** 쉬는 중인가. <b>시각의 부호로 표시하지 않는다</b> — 단조 시계는 음수일 수 있다. */
    private volatile boolean waiting;

    /** 쉬는 중이면 이 시각(나노)까지 리더로 안 친다. */
    private volatile long notBefore;

    /** 쉬는 구간을 건 시각. 해제 로그의 길이를 잰다. */
    private volatile long armedAt;

    private HandoverSpacing(LongSupplier nanoTicker, Duration tick) {
        this.nanoTicker = Objects.requireNonNull(nanoTicker, "nanoTicker 는 필수다");
        this.ceiling = PUBLISHED_ROUNDING.plus(Objects.requireNonNull(tick, "tick 은 필수다"));
    }

    public static HandoverSpacing of(LongSupplier nanoTicker, Duration tick) {
        return new HandoverSpacing(nanoTicker, tick);
    }

    /**
     * 리더가 됐다. <b>발행의 나이로 남은 대기를 구한다</b> — 발행 시각은 레디스 시계라 이 노드
     * 벽시계로 빼면 시계 차이만큼 대기가 늘거나 0 이 된다. 대기는 한 초와 한 틱을 안 넘는다.
     *
     * @param publishedAge 레디스 시계로 잰 마지막 발행의 나이. 모르면 null
     */
    public void armedFrom(Duration publishedAge) {
        long now = nanoTicker.getAsLong();
        // **쉬는 중에 다시 걸면 앞 구간부터 닫는다.** 안 닫으면 앞 진입 로그가 짝을 잃는다.
        released(now);
        if (publishedAge == null || publishedAge.compareTo(ceiling) >= 0) {
            return;
        }
        Duration wait = publishedAge.isNegative() ? ceiling : ceiling.minus(publishedAge);
        notBefore = now + wait.toNanos();
        armedAt = now;
        waiting = true;
        log.info("승계 첫 회차를 앞 발행에서 떨어뜨린다 — {}ms 쉰다", wait.toMillis());
    }

    @Override
    public boolean getAsBoolean() {
        if (!waiting) {
            return true;
        }
        long now = nanoTicker.getAsLong();
        // 나노 시각은 차이로만 견준다. 값끼리 크기를 보면 넘침 경계에서 뒤집힌다.
        if (now - notBefore < 0) {
            return false;
        }
        released(now);
        return true;
    }

    /** 쉬는 구간을 닫는다. 쉬는 중이 아니면 아무것도 안 한다. */
    private void released(long now) {
        if (waiting) {
            waiting = false;
            log.info("승계 첫 회차 대기 끝 — {}ms 쉬었다",
                    Duration.ofNanos(now - armedAt).toMillis());
        }
    }
}

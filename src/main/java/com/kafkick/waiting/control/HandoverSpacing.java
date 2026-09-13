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

    /** 이 시각(나노) 전에는 리더로 안 친다. 쉬지 않을 때는 0 이다. */
    private volatile long notBefore;

    /** 쉰 구간을 건 시각. 해제 로그를 한 번만 남기려고 둔다. 쉬는 중이 아니면 -1 이다. */
    private volatile long armedAt = -1;

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
        if (publishedAge == null || publishedAge.compareTo(ceiling) >= 0) {
            notBefore = now;
            armedAt = -1;
            return;
        }
        Duration wait = publishedAge.isNegative() ? ceiling : ceiling.minus(publishedAge);
        notBefore = now + wait.toNanos();
        armedAt = now;
        log.info("승계 첫 회차를 앞 발행에서 떨어뜨린다 — {}ms 쉰다", wait.toMillis());
    }

    @Override
    public boolean getAsBoolean() {
        long now = nanoTicker.getAsLong();
        if (now < notBefore) {
            return false;
        }
        long since = armedAt;
        if (since >= 0) {
            armedAt = -1;
            log.info("승계 첫 회차 대기 끝 — {}ms 쉬었다",
                    Duration.ofNanos(now - since).toMillis());
        }
        return true;
    }
}

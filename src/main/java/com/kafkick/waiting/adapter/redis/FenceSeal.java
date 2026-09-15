package com.kafkick.waiting.adapter.redis;

import java.time.Duration;
import java.util.Optional;

/**
 * 승계 잠금의 결과.
 *
 * @param locked       잠근 쿠폰 수
 * @param lastApplyAge 잠그기 직전 가장 최근 적용의 나이. 적용 표가 없으면 비어 있다
 */
public record FenceSeal(long locked, Optional<Duration> lastApplyAge) {

    /** 아무것도 안 잠갔고 나이도 모른다. */
    public static final FenceSeal NONE = new FenceSeal(0, Optional.empty());

    /** 쿠폰 결과를 합친다. 잠근 수는 더하고 나이는 가장 어린 쪽을 둔다. */
    public FenceSeal plus(FenceSeal other) {
        Optional<Duration> youngest = lastApplyAge.isEmpty() ? other.lastApplyAge
                : other.lastApplyAge.isEmpty() || lastApplyAge.get().compareTo(other.lastApplyAge.get()) <= 0
                        ? lastApplyAge : other.lastApplyAge;
        return new FenceSeal(locked + other.locked, youngest);
    }
}

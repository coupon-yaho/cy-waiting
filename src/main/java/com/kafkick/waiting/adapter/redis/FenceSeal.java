package com.kafkick.waiting.adapter.redis;

import java.time.Duration;
import java.util.Optional;

/**
 * 승계 잠금의 결과. <b>나이를 같이 싣는다</b> — 새 리더의 첫 적용이 앞 리더의 마지막 적용과 한 틱 안에 겹치면
 * 두 틱 몫이 1초 안에 들어간다. 따로 읽으면 왕복이 늘고 읽기와 잠금 사이의 적용이 빠진다.
 *
 * @param locked       잠근 쿠폰 수
 * @param lastApplyAge 잠그기 직전 가장 최근 적용의 나이. 적용 표가 없으면 비어 있다
 */
public record FenceSeal(long locked, Optional<Duration> lastApplyAge) {

    /** 아무것도 안 잠갔고 나이도 모른다. */
    public static final FenceSeal NONE = new FenceSeal(0, Optional.empty());

    /**
     * 쿠폰 하나의 잠금 결과. 적용과 잠금이 표에 같은 수명을 새로 걸어 {@code 수명 - 남은 수명} 이 곧 나이다. 남은 수명이
     * 없거나(-1) 표가 없으면(-2) 나이를 모른다. 남은 수명이 수명보다 길면 0 으로 잘라 더 기다리는 쪽으로 틀린다.
     */
    public static FenceSeal of(long locked, long leftMillis, Duration ttl) {
        if (leftMillis <= 0) {
            return new FenceSeal(locked, Optional.empty());
        }
        Duration left = Duration.ofMillis(leftMillis);
        return new FenceSeal(locked, Optional.of(left.compareTo(ttl) >= 0 ? Duration.ZERO : ttl.minus(left)));
    }

    /** 쿠폰 결과를 합친다. <b>나이는 가장 어린 쪽이다</b> — 가장 최근 적용에서 띄워야 어느 쿠폰에서도 안 겹친다. */
    public FenceSeal plus(FenceSeal other) {
        Optional<Duration> youngest = lastApplyAge.isEmpty() ? other.lastApplyAge
                : other.lastApplyAge.isEmpty() || lastApplyAge.get().compareTo(other.lastApplyAge.get()) <= 0
                        ? lastApplyAge : other.lastApplyAge;
        return new FenceSeal(locked + other.locked, youngest);
    }
}

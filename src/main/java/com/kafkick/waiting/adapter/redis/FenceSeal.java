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
}

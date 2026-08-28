package com.kafkick.waiting.gateway;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 매진 관찰의 수명과 크기 (7.2.5·7.2.6).
 *
 * @param ttl 해제 신호를 놓쳐도 이만큼만 막는다. <b>안전판이지 정상 경로가
 *            아니다</b> — 정상 해제는 재입고를 본 순간이다
 * @param maxKeys 담을 수 있는 쿠폰 수. 키가 클라이언트 입력에서 온다
 */
@ConfigurationProperties("waiting.sold-out-cache")
public record SoldOutCacheProperties(Duration ttl, int maxKeys) {

    // 검증은 SoldOutCache 생성자가 진다. 여기서 또 하면 두 곳이 갈린다.
}

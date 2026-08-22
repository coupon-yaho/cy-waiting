package com.kafkick.waiting.control;

import java.util.Map;
import reactor.core.publisher.Mono;

/**
 * 판정 재료를 어디서 받아 오는가.
 *
 * <p>제어 평면이 어댑터 구현을 직접 알면 <b>의존 방향이 뒤집힌다.</b> 그리고
 * 장애를 주입해 보려면 감쌀 수 있어야 하는데, 구현 타입에 묶이면 그것도 막힌다.
 */
public interface SnapshotSource {

    /** 발행된 판정 재료 전체. 없으면 빈 것이 온다. */
    Mono<Map<String, String>> load();
}

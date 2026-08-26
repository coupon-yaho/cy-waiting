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

    /**
     * 재료와 <b>그것을 읽은 레디스 시각</b>.
     *
     * <p>기본은 시각 없이(0) 돌려준다 — 그때는 홀더가 자기 시계로 나이를 잰다.
     * 실배선은 이것을 재정의해 한 왕복으로 같이 읽는다.
     */
    default Mono<TimedSnapshot> loadTimed() {
        return load().map(hash -> new TimedSnapshot(hash, 0));
    }
}

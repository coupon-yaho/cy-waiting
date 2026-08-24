package com.kafkick.waiting.gateway;

import com.kafkick.waiting.domain.queue.QueueEntry;
import java.time.Instant;
import reactor.core.publisher.Mono;

/**
 * 요청 경로가 줄에 대고 하는 일.
 *
 * <p><b>인터페이스로 둔다.</b> 어댑터를 직접 물면 필터를 재는 시험이 레디스를
 * 띄워야 하고, 그러면 판정 경로의 시험이 컨테이너 속도에 걸린다.
 */
public interface QueuePort {

    /** 줄에 세운다. {@code maxLen} 이 0 이면 상한 없음. */
    Mono<QueueEntry> enqueue(String couponId, String memberId, long maxLen, Instant now);

    /** 지금 어디쯤인가. 조회가 곧 생존 신호다. */
    Mono<QueueEntry> status(String couponId, String memberId, Instant now);
}

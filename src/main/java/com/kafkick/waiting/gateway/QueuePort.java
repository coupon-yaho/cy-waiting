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

    /**
     * 줄 길이 상한 없음.
     *
     * <p><b>0 이 아니다.</b> 0 은 "한 명도 안 받는다" 는 뜻이고 스크립트도 같게
     * 읽는다. 이 계약을 거꾸로 읽어서 배분 전 쿠폰이 줄을 못 세우는 회귀가 났다.
     *
     * <p>판정 경로는 이 값을 안 보낸다 — 도메인이 늘 유한한 상한을 준다.
     */
    long NO_LIMIT = -1;

    /**
     * 줄에 세운다.
     *
     * @param maxLen 큐 길이 상한. {@link #NO_LIMIT} 만 상한 없음이고,
     *               0 은 상한이 0 이라 아무도 안 받는다는 뜻이다.
     *               <b>쿠폰 전체의 수다</b> — 샤드가 여럿이면 어댑터가 나눠야
     *               한다. 지금은 샤드 1 만 허용해 두 수가 같다
     */
    Mono<QueueEntry> enqueue(String couponId, String memberId, long maxLen, Instant now);

    /** 지금 어디쯤인가. 조회가 곧 생존 신호다. */
    Mono<QueueEntry> status(String couponId, String memberId, Instant now);
}

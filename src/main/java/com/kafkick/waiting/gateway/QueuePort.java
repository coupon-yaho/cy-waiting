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
     * 줄 길이 상한 없음. <b>0 이 아니다</b> — 0 은 한 명도 안 받는다는 뜻이고
     * 스크립트도 같게 읽는다. 거꾸로 읽어서 회귀가 났다 (AIJ-0073).
     */
    long NO_LIMIT = -1;

    /**
     * 줄에 세운다. <b>상한은 신규 등록에만 걸린다</b> — 이미 선 사람은 자기
     * 순번을 돌려받는다.
     *
     * @param maxLen 쿠폰 전체의 수. {@link #NO_LIMIT} 만 상한 없음이다
     */
    Mono<QueueEntry> enqueue(String couponId, String memberId, long maxLen, Instant now);

    /** 지금 어디쯤인가. 조회가 곧 생존 신호다. */
    Mono<QueueEntry> status(String couponId, String memberId, Instant now);
}

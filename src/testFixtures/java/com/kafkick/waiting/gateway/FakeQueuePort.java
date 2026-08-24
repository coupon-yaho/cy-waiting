package com.kafkick.waiting.gateway;

import com.kafkick.waiting.domain.queue.QueueEntry;
import com.kafkick.waiting.domain.queue.QueueState;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import reactor.core.publisher.Mono;

/**
 * 시험용 줄. <b>레디스 없이 순서만 흉내낸다</b> — 필터가 재려는 것은 저장이
 * 아니라 판정 뒤의 행동이다.
 */
public final class FakeQueuePort implements QueuePort {

    private final Map<String, Long> queued = new LinkedHashMap<>();
    private final AtomicInteger 등록_호출 = new AtomicInteger();
    private final AtomicInteger 조회_호출 = new AtomicInteger();

    private RuntimeException 터뜨릴_것;
    private boolean 가득_참;
    private QueueState 돌려줄_상태 = QueueState.WAITING;

    public static FakeQueuePort create() {
        return new FakeQueuePort();
    }

    /** 레디스가 죽은 구간을 만든다. */
    public FakeQueuePort 터진다(RuntimeException e) {
        this.터뜨릴_것 = e;
        return this;
    }

    /** 판정과 실제가 갈린 구간. 스냅샷은 한 틱 늦으므로 실제로 일어난다. */
    public FakeQueuePort 가득_찼다() {
        this.가득_참 = true;
        return this;
    }

    public FakeQueuePort 상태는(QueueState state) {
        this.돌려줄_상태 = state;
        return this;
    }

    public int 등록_횟수() {
        return 등록_호출.get();
    }

    public int 조회_횟수() {
        return 조회_호출.get();
    }

    /** 요청 경로가 레디스를 몇 번 쳤는가. 통과 경로에서는 0 이어야 한다 (RD-4). */
    public int 왕복() {
        return 등록_호출.get() + 조회_호출.get();
    }

    @Override
    public Mono<QueueEntry> enqueue(String couponId, String memberId, long maxLen, Instant now) {
        등록_호출.incrementAndGet();
        if (터뜨릴_것 != null) {
            return Mono.error(터뜨릴_것);
        }
        boolean 있던_사람 = queued.containsKey(memberId);
        if (!있던_사람 && (가득_참 || (maxLen > 0 && queued.size() >= maxLen))) {
            return Mono.just(new QueueEntry(QueueState.REJECTED, -1, -1, false, false));
        }
        queued.putIfAbsent(memberId, (long) queued.size() + 1);
        return Mono.just(new QueueEntry(QueueState.WAITING, rankOf(memberId),
                queued.get(memberId), 있던_사람, false));
    }

    @Override
    public Mono<QueueEntry> status(String couponId, String memberId, Instant now) {
        조회_호출.incrementAndGet();
        if (터뜨릴_것 != null) {
            return Mono.error(터뜨릴_것);
        }
        if (!queued.containsKey(memberId)) {
            return Mono.just(new QueueEntry(QueueState.NOT_QUEUED, -1, -1, false, false));
        }
        return Mono.just(new QueueEntry(돌려줄_상태, rankOf(memberId),
                queued.get(memberId), true, false));
    }

    private long rankOf(String memberId) {
        return queued.keySet().stream().takeWhile(id -> !id.equals(memberId)).count();
    }
}

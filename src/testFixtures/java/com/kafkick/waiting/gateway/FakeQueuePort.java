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
    private boolean 차례가_옴;
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

    /**
     * 배분이 임계를 맨 앞 사람까지 올린 뒤. <b>자유형 세터를 안 둔다</b> — 아무
     * 상태나 받으면 운영이 못 만드는 조합이 생기고, 그 조합을 전제로 통과하는
     * 시험이 만들어진다.
     */
    public FakeQueuePort 차례가_왔다() {
        this.차례가_옴 = true;
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
            return Mono.just(QueueEntry.rejected());
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
            return Mono.just(QueueEntry.notQueued());
        }
        // **차례는 맨 앞부터 온다.** 뒤에 선 사람까지 입장으로 만들면 앞에
        // 사람이 있는 입장이 되고, 그건 운영이 못 만드는 조합이다.
        long rank = rankOf(memberId);
        if (차례가_옴 && rank == 0) {
            return Mono.just(new QueueEntry(QueueState.ADMITTED, 0,
                    queued.get(memberId), true, false));
        }
        return Mono.just(new QueueEntry(QueueState.WAITING, rank,
                queued.get(memberId), true, false));
    }

    private long rankOf(String memberId) {
        return queued.keySet().stream().takeWhile(id -> !id.equals(memberId)).count();
    }
}

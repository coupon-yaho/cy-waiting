package com.kafkick.waiting.gateway;

import com.kafkick.waiting.domain.queue.QueueEntry;
import com.kafkick.waiting.domain.queue.QueueState;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import reactor.core.publisher.Mono;

/**
 * 시험용 줄. <b>레디스 없이 순서만 흉내낸다</b> — 필터가 재려는 것은 저장이
 * 아니라 판정 뒤의 행동이다.
 */
public final class FakeQueuePort implements QueuePort {

    /** 쿠폰마다 다른 줄이다. 실물이 쿠폰별 ZSET 이라 여기서 합치면 그 차이가 안 보인다. */
    private final Map<String, Map<String, Long>> queues = new LinkedHashMap<>();
    private final AtomicInteger 등록_호출 = new AtomicInteger();
    private final AtomicInteger 조회_호출 = new AtomicInteger();

    /** 자리를 비웠다 돌아온 사람. <b>새로 서는 등록 한 번에만</b> 소비된다. */
    private final Set<String> 돌아온_사람 = ConcurrentHashMap.newKeySet();

    /** 이 사람을 재방문자로 만든다. */
    public void 돌아온_사람으로_만든다(String memberId) {
        돌아온_사람.add(memberId);
    }

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

    /** 장애가 걷힌다. 진입만 만들 수 있으면 해제 쪽 전이를 못 잰다. */
    public FakeQueuePort 나았다() {
        this.터뜨릴_것 = null;
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

    /** 그 쿠폰의 줄에 선 인원. <b>쿠폰을 가려서 센다</b> — 합쳐 세면 키가 섞여도 안 보인다. */
    public int 줄_길이(String couponId) {
        return queues.getOrDefault(couponId, Map.of()).size();
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
        // **잘못된 상한을 받아 주지 않는다.** 실물은 거절하는데 여기서 통과시키면
        // 그 회귀를 게이트웨이 시험이 못 본다.
        if (maxLen < QueuePort.NO_LIMIT) {
            return Mono.error(new IllegalArgumentException(
                    "큐 길이 상한은 %d 이상이어야 한다: %d".formatted(QueuePort.NO_LIMIT, maxLen)));
        }
        if (터뜨릴_것 != null) {
            return Mono.error(터뜨릴_것);
        }
        Map<String, Long> queued = queues.computeIfAbsent(couponId, id -> new LinkedHashMap<>());
        boolean 있던_사람 = queued.containsKey(memberId);
        // **0 도 상한이다.** 스크립트와 다르게 읽으면 게이트웨이 시험이 실제
        // 거절을 놓친다 — 픽스처만 받아 주기 때문이다.
        if (!있던_사람 && (가득_참 || (maxLen >= 0 && queued.size() >= maxLen))) {
            return Mono.just(QueueEntry.rejected());
        }
        queued.putIfAbsent(memberId, (long) queued.size() + 1);
        return Mono.just(new QueueEntry(QueueState.WAITING, rankOf(couponId, memberId),
                queued.get(memberId), 있던_사람, false,
                // **이미 줄에 선 사람은 기록을 안 본다.** 실물이 그 분기에서
                // 먼저 돌아가므로, 여기서 소비하면 픽스처가 실제와 달라진다.
                !있던_사람 && 돌아온_사람.remove(memberId)));
    }

    @Override
    public Mono<QueueEntry> status(String couponId, String memberId, Instant now) {
        조회_호출.incrementAndGet();
        if (터뜨릴_것 != null) {
            return Mono.error(터뜨릴_것);
        }
        Map<String, Long> queued = queues.getOrDefault(couponId, Map.of());
        if (!queued.containsKey(memberId)) {
            return Mono.just(QueueEntry.notQueued());
        }
        // **차례는 맨 앞부터 온다.** 뒤에 선 사람까지 입장으로 만들면 앞에
        // 사람이 있는 입장이 되고, 그건 운영이 못 만드는 조합이다.
        long rank = rankOf(couponId, memberId);
        if (차례가_옴 && rank == 0) {
            return Mono.just(new QueueEntry(QueueState.ADMITTED, 0,
                    queued.get(memberId), true, false, false));
        }
        return Mono.just(new QueueEntry(QueueState.WAITING, rank,
                queued.get(memberId), true, false, false));
    }

    private long rankOf(String couponId, String memberId) {
        return queues.getOrDefault(couponId, Map.of()).keySet().stream()
                .takeWhile(id -> !id.equals(memberId)).count();
    }
}

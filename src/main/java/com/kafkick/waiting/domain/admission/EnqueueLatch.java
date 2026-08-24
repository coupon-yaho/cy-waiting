package com.kafkick.waiting.domain.admission;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 줄에 세운 직후의 한 구간을 메운다.
 *
 * <p>스냅샷은 한 틱 늦다. 그 사이 다음 초 창이 열리면 <b>방금 줄 선 사람을 신규
 * 유입이 넘어간다</b> — 스냅샷이 아직 한산하다고 말하기 때문이다 (불변식 4).
 *
 * <p>노드 로컬이다. 다른 노드의 등록은 스냅샷으로만 보인다.
 */
public final class EnqueueLatch {

    private final Map<String, Long> marked = new ConcurrentHashMap<>();
    private final int maxKeys;
    private final long ttlSec;

    private EnqueueLatch(int maxKeys, long ttlSec) {
        if (ttlSec < 1) {
            throw new IllegalArgumentException(
                    "래치 수명은 양수여야 한다: %d".formatted(ttlSec));
        }
        this.maxKeys = Math.max(1, maxKeys);
        this.ttlSec = ttlSec;
    }

    /**
     * @param maxKeys 상한. <b>무제한이면 쿠폰 식별자를 바꿔가며 메모리를 밀어낸다</b>
     * @param ttlSec  걸려 있는 시간. 스냅샷이 따라잡는 데 걸리는 시간보다 길어야 한다
     */
    public static EnqueueLatch of(int maxKeys, long ttlSec) {
        return new EnqueueLatch(maxKeys, ttlSec);
    }

    /** 이 쿠폰을 방금 줄로 보냈다. */
    public void mark(String couponKey, long epochSecond) {
        if (marked.size() >= maxKeys && !marked.containsKey(couponKey)) {
            // **상한을 넘으면 통째로 비운다.** 하나씩 골라 버리면 무엇을 버릴지
            // 정하는 데 순회가 들고, 그 순회가 요청 경로에 붙는다.
            marked.clear();
        }
        marked.put(couponKey, epochSecond);
    }

    /**
     * 아직 걸려 있는가.
     *
     * <p><b>미래의 표식은 안 믿는다.</b> 시계가 뒤로 가면 그 표식이 영원히 살아
     * 한산한 쿠폰이 영영 안 풀린다.
     */
    public boolean latched(String couponKey, long epochSecond) {
        Long at = marked.get(couponKey);
        if (at == null) {
            return false;
        }
        long age = epochSecond - at;
        return age >= 0 && age < ttlSec;
    }

    /** 지금 들고 있는 키의 수. 상한이 실제로 도는지 보려면 이것이 필요하다. */
    public int size() {
        return marked.size();
    }
}

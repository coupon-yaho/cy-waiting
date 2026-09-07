package com.kafkick.waiting.domain.admission;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 줄에 세운 직후의 한 구간을 메운다. 스냅샷은 한 틱 늦어 아직 한산하다고 말하고,
 * 그동안 <b>방금 줄 선 사람을 신규 유입이 추월한다.</b> 노드 로컬이라
 * 다른 노드의 등록은 스냅샷으로만 보인다.
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
        // 보정하면 0 을 넘긴 사람이 상한 0 을 기대하는데 하나가 남는다.
        if (maxKeys < 1) {
            throw new IllegalArgumentException("키 상한은 양수여야 한다: %d".formatted(maxKeys));
        }
        this.maxKeys = maxKeys;
        this.ttlSec = ttlSec;
    }

    /**
     * @param maxKeys 상한. <b>무제한이면 쿠폰 식별자를 바꿔가며 메모리를 밀어낸다</b>
     * @param ttlSec  걸려 있는 시간. 스냅샷이 따라잡는 데 걸리는 시간보다 길어야 한다
     */
    public static EnqueueLatch of(int maxKeys, long ttlSec) {
        return new EnqueueLatch(maxKeys, ttlSec);
    }

    /**
     * 주어진 기간을 <b>반드시 덮는</b> 래치. 초로 자른 시각을 재 실효 수명이
     * {@code (ttl-1, ttl]} 이라, 올림한 뒤 한 초를 더한다.
     */
    public static EnqueueLatch covering(int maxKeys, Duration atLeast) {
        // 0 이하를 받으면 아래 셈이 수명 1 초짜리 래치를 만든다. 그건 래치가
        // 없는 것과 같은데, 있는 것처럼 보여서 더 나쁘다.
        if (atLeast == null || atLeast.isNegative() || atLeast.isZero()) {
            throw new IllegalArgumentException("덮을 기간은 양수여야 한다: " + atLeast);
        }
        long seconds = atLeast.toSeconds();
        long rounded = atLeast.minusSeconds(seconds).isZero() ? seconds : seconds + 1;
        return new EnqueueLatch(maxKeys, rounded + 1);
    }

    /**
     * 이 쿠폰을 방금 줄로 보냈다. <b>검사와 삽입을 한 걸음으로 묶는다</b> — 나누면
     * 여럿이 동시에 "아직 여유 있다" 를 보고 다 같이 넣어 상한을 넘긴다.
     */
    public void mark(String couponKey, long epochSecond) {
        // **이미 걸려 있으면 시각을 안 고친다.** 대기 판정이 다시 여기로 돌아오는
        // 닫힌 고리가 있어, 갱신하면 트래픽이 이어지는 동안 영영 안 풀린다 —
        // 한산해진 뒤에도 줄이 선다. 래치가 덮는 것은 스냅샷이 따라잡기까지다.
        if (marked.putIfAbsent(couponKey, epochSecond) != null) {
            return;
        }
        if (marked.size() > maxKeys) {
            // **상한을 넘으면 통째로 비운다.** 하나씩 골라 버리면 무엇을 버릴지
            // 정하는 순회가 요청 경로에 붙는다. 넘긴 뒤에 비우니 잠깐은 상한을
            // 넘지만, 그 초과는 동시에 들어온 수만큼이다.
            marked.clear();
            marked.put(couponKey, epochSecond);
        }
    }

    /**
     * 아직 걸려 있는가. <b>수명만큼은 뒤로 간 시계도 걸린 것으로 본다</b> —
     * 거기서 풀어 주면 그 방향이 추월이다. 더 앞선 표식은 잘못 찍힌 값이다.
     */
    public boolean latched(String couponKey, long epochSecond) {
        Long at = marked.get(couponKey);
        if (at == null) {
            return false;
        }
        long age = epochSecond - at;
        // 오판은 몇 명이 괜히 줄 서는 쪽으로만 한다. 다만 무한정은 아니다 —
        // 수명만큼 앞선 표식까지 인정하면 그 쿠폰이 영영 안 풀린다.
        if (age < ttlSec && age > -ttlSec) {
            return true;
        }
        // **만료된 것은 그 자리에서 지운다.** 안 지우면 맵이 프로세스 수명 동안
        // 단조 증가하고, 상한을 넘는 순간 살아 있던 래치까지 통째로 날아간다.
        marked.remove(couponKey, at);
        return false;
    }


    /** 지금 들고 있는 키의 수. 상한이 실제로 도는지 보려면 이것이 필요하다. */
    public int size() {
        return marked.size();
    }
}

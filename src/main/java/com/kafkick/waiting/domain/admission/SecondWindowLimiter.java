package com.kafkick.waiting.domain.admission;

import java.util.HashMap;
import java.util.Map;

/**
 * 초 단위 고정 윈도우 리미터.
 *
 * <p><b>경로별로 나누지 않는다.</b> 각자 카운터를 들면 회복 전이 순간 두 상한이
 * 동시에 열려 1.5× 버스트가 나간다(F4). 리미터는 하나고 상한만 인자로 받는다.
 */
public class SecondWindowLimiter {

    /** 키가 클라이언트 입력에서 오므로 상한이 없으면 메모리가 무한히 는다. */
    private final int maxKeys;

    private final Map<String, Long> used = new HashMap<>();
    private long windowSecond = Long.MIN_VALUE;

    /** 키 상한을 정해 만든다. 0 이하는 1 로 올린다 — 상한이 없는 리미터는 없다. */
    public static SecondWindowLimiter withMaxKeys(int maxKeys) {
        return new SecondWindowLimiter(maxKeys);
    }

    SecondWindowLimiter(int maxKeys) {
        this.maxKeys = Math.max(1, maxKeys);
    }

    /**
     * 상한 안이면 차감하고 {@code true}.
     *
     * @param key          예산을 나누는 단위. 쿠폰 ID 또는 노드 전역 키
     * @param cap          이번 판정에 적용할 상한. 경로마다 다른 값이 온다
     * @param epochSecond  주입받은 시각. 도메인은 시계를 부르지 않는다 (DS-1)
     */
    public synchronized boolean tryAcquire(String key, long cap, long epochSecond) {
        if (cap <= 0) {
            return false;
        }
        rollWindow(epochSecond);

        long current = used.getOrDefault(key, 0L);
        if (current >= cap) {
            return false;
        }
        if (current == 0 && used.size() >= maxKeys) {
            // 새 키를 받을 자리가 없다. 통과시키면 상한이 무의미해지므로 거부한다.
            return false;
        }
        used.put(key, current + 1);
        return true;
    }

    /**
     * 두 예산을 <b>전부-아니면-전무</b>로 획득한다.
     *
     * <p>순서대로 치면 앞엣것을 소비한 뒤 뒤엣것이 거부할 때 <b>통과하지 않은 요청이
     * 예산을 깎는다.</b> 반납 방식도 쓰지 않는다 — 반납 누락이 조용한 예산 유실이다.
     *
     * @return 획득 결과. 실패면 어느 쪽이 부족했는지 담는다
     */
    public synchronized AcquireResult tryAcquireAll(
            String couponKey, long couponCap, String globalKey, long globalCap, long epochSecond) {

        rollWindow(epochSecond);

        // 두 키가 같으면 예산도 하나다. 따로 차감하면 요청 하나가 2 를 소비해
        // 상한의 절반만 통과시킨다.
        if (couponKey.equals(globalKey)) {
            long cap = Math.min(couponCap, globalCap);
            if (!hasRoom(couponKey, cap)) {
                return couponCap <= globalCap
                        ? AcquireResult.COUPON_EXHAUSTED
                        : AcquireResult.GLOBAL_EXHAUSTED;
            }
            if (!hasSlots(used.containsKey(couponKey) ? 0 : 1)) {
                return AcquireResult.KEY_SATURATED;
            }
            used.merge(couponKey, 1L, Long::sum);
            return AcquireResult.ACQUIRED;
        }

        // 신규 키가 몇 개 들어오는지 먼저 센다. 하나씩 검사하면 마지막 슬롯
        // 하나를 두 키가 함께 차지해 상한을 넘긴다.
        int incoming = (used.containsKey(couponKey) ? 0 : 1)
                + (used.containsKey(globalKey) ? 0 : 1);

        // 예산을 먼저 본다. 예산이 마른 것과 자리가 없는 것은 대응이 다르고,
        // 예산이 말랐으면 그 키는 이미 자리를 잡고 있어 자리 문제가 아니다.
        if (!hasRoom(couponKey, couponCap)) {
            return AcquireResult.COUPON_EXHAUSTED;
        }
        if (!hasRoom(globalKey, globalCap)) {
            return AcquireResult.GLOBAL_EXHAUSTED;
        }
        if (!hasSlots(incoming)) {
            return AcquireResult.KEY_SATURATED;
        }

        used.merge(couponKey, 1L, Long::sum);
        used.merge(globalKey, 1L, Long::sum);
        return AcquireResult.ACQUIRED;
    }

    /** 지금 들고 있는 키 수. 상한이 지켜지는지 시험하려고 노출한다. */
    public synchronized int size() {
        return used.size();
    }

    private boolean hasRoom(String key, long cap) {
        return cap > 0 && used.getOrDefault(key, 0L) < cap;
    }

    /** 새로 들어올 키 {@code incomingKeys} 개를 받을 자리가 남았는가. */
    private boolean hasSlots(int incomingKeys) {
        return used.size() + incomingKeys <= maxKeys;
    }

    /**
     * 초가 바뀌면 윈도우를 통째로 버린다.
     *
     * <p>키별로 만료시키지 않는다 — 만료 시각을 키마다 들고 있어야 해서 그 자체가
     * 메모리다. 초 하나만 들고 바뀌면 전부 버리는 쪽이 싸다.
     */
    private void rollWindow(long epochSecond) {
        // == 이 아니라 > 다. 노드 간 시계 스큐나 NTP 보정으로 과거 초가 들어오면
        // == 비교로는 현재 윈도우를 통째로 날려 예산이 리셋된다. 뒤로 가지 않는다.
        if (epochSecond <= windowSecond) {
            return;
        }
        windowSecond = epochSecond;
        used.clear();
    }

    /** 획득 실패 시 어느 예산이 부족했는지. 대응이 다르므로 구분한다. */
    public enum AcquireResult {
        /** 둘 다 여유가 있어 함께 차감했다. */
        ACQUIRED,
        /** 그 쿠폰이 유휴 몫을 다 썼다. 그 쿠폰만 조이면 된다. */
        COUPON_EXHAUSTED,
        /** 이 노드가 초당 감당량을 다 썼다. 노드를 늘려야 한다. */
        GLOBAL_EXHAUSTED,
        /**
         * 예산은 남았는데 키를 더 못 들고 있다.
         *
         * <p>예산 고갈로 뭉뚱그리면 운영자가 엉뚱한 데를 조인다. 여기를 보면
         * 조일 것은 쿠폰도 노드도 아니고 {@code maxKeys} 다.
         */
        KEY_SATURATED
    }

}

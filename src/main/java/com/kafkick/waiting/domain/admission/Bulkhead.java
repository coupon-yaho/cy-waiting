package com.kafkick.waiting.domain.admission;

import java.util.HashMap;
import java.util.Map;

/**
 * 동시에 몇 건이 뒷단에 걸려 있는지를 셉니다.
 *
 * <p><b>레이트 리밋으로는 못 막는 것이 있습니다.</b> 초당 100건이어도 각각 10초가
 * 걸리면 동시 1,000건입니다. 리미터는 초당 건수를, 격벽은 동시 건수를 셉니다.
 *
 * <p>느려진 뒷단 한 대가 게이트웨이의 커넥션을 다 붙잡으면, 한산한 쿠폰의 통과
 * 경로까지 같이 죽습니다.
 */
public class Bulkhead {

    /** 담을 수 있는 쿠폰 수. 프로덕션 배선은 {@link CouponKeys#MAX} 를 넘깁니다. */
    private final int maxKeys;

    /** 쿠폰별로 지금 걸려 있는 건수. 0 이 되면 지웁니다. */
    private final Map<String, Integer> inFlight = new HashMap<>();

    /**
     * 전체 합. <b>들고 다니지 않으면 읽을 때마다 맵을 훑습니다.</b>
     *
     * <p>그 훑기가 자리를 잡는 것과 같은 자물쇠 안이라, 지표를 한 번 긁는 동안
     * 요청 경로가 통째로 멈춥니다 — 키가 만 개면 그 길이만큼.
     */
    private int total;

    Bulkhead(int maxKeys) {
        if (maxKeys < 1) {
            throw new IllegalArgumentException("maxKeys 는 1 이상이어야 한다: " + maxKeys);
        }
        this.maxKeys = maxKeys;
    }

    public static Bulkhead withMaxKeys(int maxKeys) {
        return new Bulkhead(maxKeys);
    }

    /**
     * 자리를 하나 잡습니다.
     *
     * <p><b>쿠폰마다 따로 셉니다.</b> 하나로 세면 몰리는 쿠폰이 자리를 다 쓰고
     * 한산한 쿠폰이 그 뒤에 밀립니다 — R1 이 뒤집힙니다.
     *
     * @param cap 이 쿠폰이 동시에 걸어 둘 수 있는 상한. 배분된 크레딧에서 옵니다
     * @return 들어갔으면 참. 거짓이면 {@link #exit} 를 부르면 안 됩니다
     */
    public synchronized boolean tryEnter(String couponId, long cap) {
        int current = inFlight.getOrDefault(couponId, 0);
        if (current >= cap) {
            return false;
        }
        // **새 쿠폰만 상한을 봅니다.** 이미 담긴 쿠폰을 막으면 그 쿠폰이 자기
        // 자리를 못 쓰고, 맵은 어차피 안 커집니다.
        if (current == 0 && inFlight.size() >= maxKeys) {
            return false;
        }
        inFlight.put(couponId, current + 1);
        total++;
        return true;
    }

    /**
     * 자리를 돌려줍니다.
     *
     * <p><b>안 돌려주면 격벽이 한 번 차고 나서 영영 안 열립니다.</b> 성공·실패·
     * 취소 어느 쪽으로 끝나든 반드시 불러야 합니다. 안 들어간 것을 내보내도
     * 음수로 안 내려갑니다 — 내려가면 그만큼 상한이 늘어납니다.
     */
    public synchronized void exit(String couponId) {
        Integer current = inFlight.get(couponId);
        if (current == null) {
            return;
        }
        total--;
        if (current <= 1) {
            // 비면 지웁니다. 안 지우면 끝난 쿠폰이 맵을 차지한 채 남아, 새
            // 캠페인이 열릴 때 그 쿠폰이 못 들어갑니다.
            inFlight.remove(couponId);
            return;
        }
        inFlight.put(couponId, current - 1);
    }

    /** 지금 걸려 있는 전체 건수. 지표가 이 값을 읽습니다 (6.3.6). */
    public synchronized int inFlight() {
        return total;
    }

    /** 담고 있는 쿠폰 수. 맵이 상한에 붙었는지 보는 값입니다. */
    public synchronized int size() {
        return inFlight.size();
    }

    /** 담을 수 있는 쿠폰 수. <b>지표가 분모로 읽습니다</b> (6.3.6). */
    public int maxKeys() {
        return maxKeys;
    }
}

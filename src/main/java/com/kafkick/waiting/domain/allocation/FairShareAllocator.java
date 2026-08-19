package com.kafkick.waiting.domain.allocation;

import java.util.ArrayList;
import java.util.List;

/**
 * 전역 크레딧을 쿠폰에 나눈다. <b>기아 불가와 유휴 낭비 0 을 함께 만족시킨다.</b>
 *
 * <p>균등하게만 나누면 한산한 쿠폰이 못 쓰고 남긴 몫이 버려지고, 요구량 비례로만
 * 나누면 몰리는 쿠폰 하나가 전부 가져가 나머지가 굶는다 (C-1·C-3).
 */
public class FairShareAllocator {

    private FairShareAllocator() {
    }

    /**
     * 배분기를 만든다.
     *
     * <p>상태가 없어 static 으로 둘 수도 있지만 <b>배분은 도메인 규칙이다</b>
     * (JS-14). 두 번째 정책이 생길 때 호출부를 안 고치려면 인스턴스여야 한다.
     */
    public static FairShareAllocator create() {
        return new FairShareAllocator();
    }

    /**
     * 굶주린 쿠폰에게 균등하게 나누고, 못 쓴 몫을 다시 굶주린 쪽으로 돌린다.
     *
     * <p>정수 나눗셈의 나머지는 <b>배분하지 않는다.</b> 누구에게 주든 그 쿠폰만
     * 이득이고, 노드마다 다른 쪽을 고르면 총합이 전역 크레딧을 넘는다. 남긴
     * 나머지는 다음 틱 배분에 다시 들어간다.
     */
    public List<Grant> allocate(long globalCredit, List<CouponDemand> demands) {
        List<CouponDemand> active = demands.stream().filter(CouponDemand::isActive).toList();
        if (active.isEmpty()) {
            return List.of();
        }

        long[] granted = new long[active.size()];
        long pool = Math.max(0, globalCredit);
        // 2패스가 하한이지 상한이 아니다. 쿠폰이 많고 요구량이 들쭉날쭉하면
        // 두 번으로는 못 채우고, 남긴 몫만큼 대기자가 이유 없이 기다린다.
        // 몫이 굶주린 수보다 적어지면 distribute 가 0 을 돌려주므로 반드시 멎는다.
        while (pool > 0) {
            pool = distribute(active, granted, pool);
        }

        List<Grant> result = new ArrayList<>(active.size());
        for (int i = 0; i < active.size(); i++) {
            result.add(new Grant(active.get(i).couponId(), granted[i]));
        }
        return result;
    }

    /**
     * 굶주린 쿠폰에게 균등하게 나눠 주고 <b>다음 패스로 넘길 몫</b>을 돌려준다.
     *
     * <p><b>못 쓴 몫은 전부 넘긴다.</b> 나눗셈 나머지를 그 자리에서 버리면
     * 패스마다 조금씩 새어 그만큼 대기자가 이유 없이 기다린다. 넘긴 나머지는
     * 다음 패스에서 더 적은 수로 다시 나뉘고, 결국 굶주린 수보다 작아지면
     * 멎는다 — 그때는 균등하게 나눌 방법이 없어서 다음 틱 몫이 된다.
     */
    private long distribute(List<CouponDemand> active, long[] granted, long pool) {
        // 호출부가 pool > 0 을 보장한다. 0 이면 애초에 돌 이유가 없다.
        int hungry = 0;
        for (int i = 0; i < active.size(); i++) {
            if (granted[i] < active.get(i).want()) {
                hungry++;
            }
        }
        if (hungry == 0) {
            return 0;
        }

        long share = pool / hungry;
        if (share == 0) {
            // 나머지만 남았다. 나눠 주면 앞쪽 쿠폰이 유리해진다.
            return 0;
        }

        long spent = 0;
        for (int i = 0; i < active.size(); i++) {
            long room = active.get(i).want() - granted[i];
            if (room <= 0) {
                continue;
            }
            long give = Math.min(room, share);
            granted[i] += give;
            spent += give;
        }
        return pool - spent;
    }
}

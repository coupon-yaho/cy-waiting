package com.kafkick.waiting.chaos;

import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.control.SnapshotCodec;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.allocation.QueueingHysteresis;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.QueueMode;
import com.kafkick.waiting.domain.coupon.RuntimeState;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * C20 — 배포 중에 옛 판과 새 판이 같은 스냅샷을 읽는다.
 *
 * <p>무엇을 왜 재는지는 {@code plan/08-resilience.md} 의 C20 절이 든다. 여기는
 * 그것을 어떻게 판정하는가만 든다.
 */
@Tag("chaos")
class MixedVersionSnapshotScenarioTest {

    private static final String 쿠폰 = "c20-coupon";

    private final SnapshotCodec 코덱 = SnapshotCodec.create();

    /**
     * 옛 판이 내던 모양. <b>쿠폰 값이 다섯 자리다</b> — 전역 배수가 붙기 전이다.
     *
     * <p>실제 옛 판을 못 부르므로 그 판이 냈던 필드 수로 손수 만든다. 새 판이
     * 이것을 못 읽으면 롤아웃 구간에 새 노드가 재료를 하나도 못 받는다.
     */
    private Map<String, String> 옛_판이_낸_것() {
        Map<String, String> hash = new LinkedHashMap<>();
        hash.put(쿠폰, "ADAPTIVE:QUEUEING:100:500:2000");
        hash.put("#credit", "1000");
        hash.put("#nodes", "4");
        hash.put("#publishedAt", "1800000000");
        return hash;
    }

    /** 새 판이 낼 모양. 코덱이 직접 만든다 — 손으로 적으면 형식이 갈린다. */
    private Map<String, String> 새_판이_낸_것(boolean 재고_미상) {
        CouponState 상태 = new CouponState(QueueMode.ADAPTIVE, RuntimeState.QUEUEING,
                100, 재고_미상 ? CouponState.STOCK_UNKNOWN : 500, 2000);
        GatewaySnapshot 스냅샷 = new GatewaySnapshot(Map.of(쿠폰, 상태),
                new SnapshotMeta(1000, 4, null, 1.0),
                Instant.ofEpochSecond(1_800_000_000L));
        return 코덱.encode(스냅샷, new CreditSmoother.Snapshot(0, false),
                new QueueingHysteresis.Snapshot(false, 0));
    }

    /** 옛 판이 이 해시를 읽으면 쿠폰을 받는가. 옛 디코더는 예약 자리를 건너뛴다. */
    private boolean 옛_판이_받는가(Map<String, String> hash) {
        String raw = hash.get(쿠폰);
        return raw != null && raw.split(":", 6).length >= 5;
    }

    @Test
    @DisplayName("C20_옛_판과_새_판이_서로의_스냅샷을_받아들인다")
    void C20_옛_판과_새_판이_서로의_스냅샷을_받아들인다() {
        GatewaySnapshot[] 새_판이_읽은_옛것 = new GatewaySnapshot[1];
        boolean[] 옛_판이_읽은_새것 = new boolean[2];
        GatewaySnapshot[] 미상_왕복 = new GatewaySnapshot[1];
        GatewaySnapshot[] 모자란_것 = new GatewaySnapshot[1];
        GatewaySnapshot[] 남는_것 = new GatewaySnapshot[1];

        ChaosScenario.named("C20 혼재 버전 스냅샷")
                .baseline(() -> 새_판이_읽은_옛것[0] = 코덱.decode(새_판이_낸_것(false)))
                .inject(() -> {
                    // 리더가 옛 판이다. 새 노드가 그 재료를 받아야 준비가 된다.
                    새_판이_읽은_옛것[0] = 코덱.decode(옛_판이_낸_것());
                    옛_판이_읽은_새것[0] = 옛_판이_받는가(새_판이_낸_것(false));
                })
                .duringFault(() -> {
                    // 재고 미상은 예약 자리로 간다. 옛 판은 그 자리를 건너뛴다.
                    옛_판이_읽은_새것[1] = 옛_판이_받는가(새_판이_낸_것(true));
                    미상_왕복[0] = 코덱.decode(새_판이_낸_것(true));
                    // 자리가 남으면 무시하고, 모자라면 안 받는다.
                    Map<String, String> 남음 = 옛_판이_낸_것();
                    남음.put(쿠폰, "ADAPTIVE:QUEUEING:100:500:2000:1.0:미래에_붙을_것");
                    남는_것[0] = 코덱.decode(남음);
                    Map<String, String> 모자람 = 옛_판이_낸_것();
                    모자람.put(쿠폰, "ADAPTIVE:QUEUEING:100:500");
                    모자란_것[0] = 코덱.decode(모자람);
                })
                .recover(() -> { })
                .afterRecovery(() -> { })
                .assertEntry(() -> RecoveryCriteria.violations(
                        // 전제 — 자기가 낸 것은 당연히 읽는다. 안 읽으면 아래가 뜻이 없다.
                        쿠폰을_받았다("자기 판", 새_판이_읽은_옛것[0]),
                        // 새 노드가 옛 리더의 재료를 받는다. 못 받으면 발행된
                        // 스냅샷을 하나도 못 받아 준비 상태가 안 되고 롤아웃이 멎는다.
                        쿠폰을_받았다("옛 판", 새_판이_읽은_옛것[0]),
                        // 옛 노드가 새 리더의 재료를 받는다. 못 받으면 이미 돌던
                        // 노드가 낡음으로 넘어가 전부 줄을 세운다.
                        옛_판도_받았다("재고 아는 판", 옛_판이_읽은_새것[0])))
                .assertDuring(() -> RecoveryCriteria.violations(
                        // **미상을 쿠폰 값에 안 싣는다.** 재고 자리에 음수를 넣으면
                        // 옛 생성자가 거부해 그 쿠폰이 통째로 빠지고, 양수를 넣으면
                        // 옛 노드가 재입고로 읽어 매진 방패를 푼다.
                        옛_판도_받았다("재고 미상 판", 옛_판이_읽은_새것[1]),
                        미상이_왕복한다(미상_왕복[0]),
                        // 관대함은 한 방향뿐이다.
                        남는_자리를_무시한다(남는_것[0]),
                        모자란_자리는_안_받는다(모자란_것[0])))
                .assertRecovery(RecoveryCriteria::violations)
                // **RC1~RC6 은 여기서 안 잰다.** 이 판은 스냅샷 부호화만 걷는다 —
                // 노드도 줄도 세우지 않는다. 롤아웃 중의 준비 상태와 낡음 전이는
                // 노드 둘짜리 하네스가 있어야 잰다.
                .run();
    }

    private Optional<String> 쿠폰을_받았다(String 누구, GatewaySnapshot 읽은_것) {
        return 읽은_것.coupons().containsKey(쿠폰) ? Optional.empty()
                : Optional.of("%s 이 낸 것에서 쿠폰이 빠졌다 — 그 노드는 재료를 못 받는다"
                        .formatted(누구));
    }

    private Optional<String> 옛_판도_받았다(String 무엇, boolean 받았나) {
        return 받았나 ? Optional.empty()
                : Optional.of("옛 판이 %s 을 못 읽는다 — 이미 돌던 노드가 낡음으로 넘어간다"
                        .formatted(무엇));
    }

    /** 미상이 예약 자리로 갔다가 그대로 돌아오는가. 접히면 거짓 매진이 된다. */
    private Optional<String> 미상이_왕복한다(GatewaySnapshot 읽은_것) {
        CouponState 상태 = 읽은_것.coupons().get(쿠폰);
        if (상태 == null) {
            return Optional.of("미상을 실은 판에서 쿠폰이 빠졌다");
        }
        return !상태.stockKnown() ? Optional.empty()
                : Optional.of("미상이 아는 것으로 접혔다 — 재고 자리의 0 이 거짓 매진이 된다");
    }

    private Optional<String> 남는_자리를_무시한다(GatewaySnapshot 읽은_것) {
        CouponState 상태 = 읽은_것.coupons().get(쿠폰);
        if (상태 == null) {
            return Optional.of("자리가 남았다고 쿠폰을 통째로 버렸다 (E-12)");
        }
        return 상태.remainingStock() == 500 ? Optional.empty()
                : Optional.of("남는 자리가 앞의 값을 밀었다 — 재고가 %d 다"
                        .formatted(상태.remainingStock()));
    }

    private Optional<String> 모자란_자리는_안_받는다(GatewaySnapshot 읽은_것) {
        return 읽은_것.coupons().containsKey(쿠폰)
                ? Optional.of("자리가 모자란 값을 받았다 — 밀린 값을 그대로 믿게 된다")
                : Optional.empty();
    }
}

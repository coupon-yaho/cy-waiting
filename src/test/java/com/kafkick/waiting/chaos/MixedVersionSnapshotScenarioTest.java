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
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * C20 — 배포 중에 옛 버전과 새 버전이 같은 스냅샷을 읽는다.
 *
 * <p>무엇을 왜 재는지는 {@code plan/08-resilience.md} 의 C20 절이 든다. 여기는
 * 그것을 어떻게 판정하는가만 든다.
 */
@Tag("chaos")
class MixedVersionSnapshotScenarioTest {

    private static final String 쿠폰 = "c20-coupon";

    private final SnapshotCodec 코덱 = SnapshotCodec.create();

    /**
     * 옛 버전이 내던 모양. <b>여섯째 자리에 쿠폰마다의 배수를 실었다</b>.
     *
     * <p>다섯 자리로 두었더니 그건 더 옛 형식이었다 — `SnapshotCodecTest` 의
     * `쿠폰에_모르는_필드가_붙어_와도_읽는다` 가 직전 버전의 모양을 여섯 자리로
     * 못 박아 두었다. 롤아웃에서 실제로 섞이는 버전을 안 쓰면 이 시나리오가
     * 겨냥한 구간을 안 밟는다.
     */
    private Map<String, String> 옛_버전이_낸_것() {
        Map<String, String> hash = new LinkedHashMap<>();
        hash.put(쿠폰, "ADAPTIVE:QUEUEING:100:500:2000:2.5");
        hash.put("#credit", "1000");
        hash.put("#nodes", "4");
        // **이름이 틀리면 프로덕션이 이 해시를 통째로 버린다.** 발행 시각을 못
        // 읽으면 EPOCH 이 되고, 갱신 루프가 "받아들일 수 없는 스냅샷" 으로 찍는다 —
        // 그러면 이 시나리오는 롤아웃이 멎는지를 정하는 조건을 안 거치고 초록이다.
        hash.put("#published", "1800000000");
        return hash;
    }

    /** 새 버전이 낼 모양. 코덱이 직접 만든다 — 손으로 적으면 형식이 갈린다. */
    private Map<String, String> 새_버전이_낸_것(boolean 재고_미상) {
        CouponState 상태 = new CouponState(QueueMode.ADAPTIVE, RuntimeState.QUEUEING,
                100, 재고_미상 ? CouponState.STOCK_UNKNOWN : 500, 2000);
        GatewaySnapshot 스냅샷 = new GatewaySnapshot(Map.of(쿠폰, 상태),
                new SnapshotMeta(1000, 4, null, 1.0),
                Instant.ofEpochSecond(1_800_000_000L));
        return 코덱.encode(스냅샷, new CreditSmoother.Snapshot(0, false),
                new QueueingHysteresis.Snapshot(false, 0));
    }

    /**
     * 옛 버전이 이 해시를 읽으면 쿠폰을 제대로 받는가.
     *
     * <p><b>자리 수만 세면 항등이다.</b> 새 버전의 부호화는 늘 여섯 조각이라 무엇을
     * 바꿔도 참이 된다 — 처음에 그렇게 두었더니 "미상 자리에 양수를 싣는다" 와
     * "여섯째 자리를 아예 안 싣는다" 가 둘 다 살아남았다. 옛 노드가 <b>그 자리에서
     * 읽는 값</b>으로 본다.
     */
    private boolean 옛_버전이_받는가(Map<String, String> hash) {
        String raw = hash.get(쿠폰);
        if (raw == null) {
            return false;
        }
        String[] parts = raw.split(":", 7);
        if (parts.length != 6) {
            return false;
        }
        try {
            // **옛 디코더가 하던 그대로 여섯 자리를 다 읽는다.** 자리 수와 두
            // 숫자만 보면 모드·상태가 뒤바뀌거나 크레딧이 깨져도 통과한다 —
            // 그 스냅샷에서 옛 노드는 쿠폰을 통째로 버리거나 엉뚱한 값으로 판정한다.
            QueueMode.valueOf(parts[0].toUpperCase(Locale.ROOT));
            RuntimeState.valueOf(parts[1].toUpperCase(Locale.ROOT));
            if (Long.parseLong(parts[2]) < 0) {
                return false;
            }
            // 옛 노드는 넷째 자리를 재고로 읽는다. 미상일 때 양수가 실리면
            // 재입고로 읽어 매진 방패를 푼다.
            if (Long.parseLong(parts[3]) < 0) {
                return false;
            }
            if (Long.parseLong(parts[4]) < 0) {
                return false;
            }
            // 여섯째 자리는 배수다. 안 실으면 옛 파드가 배수 없이 폴링해
            // 롤아웃 구간 내내 예산을 넘긴다.
            return Double.parseDouble(parts[5]) > 0;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /** 미상일 때 넷째 자리에 실린 값. 옛 노드가 재고로 읽는 그 자리다. */
    private long 옛_버전이_읽는_재고(Map<String, String> hash) {
        return Long.parseLong(hash.get(쿠폰).split(":", 7)[3]);
    }

    @Test
    @DisplayName("C20_옛_버전과_새_버전이_서로의_스냅샷을_받아들인다")
    void C20_옛_버전과_새_버전이_서로의_스냅샷을_받아들인다() {
        GatewaySnapshot[] 자기가_낸_것 = new GatewaySnapshot[1];
        GatewaySnapshot[] 새_버전이_읽은_옛것 = new GatewaySnapshot[1];
        boolean[] 옛_버전이_발행으로_읽히는가 = new boolean[1];
        boolean[] 자기_버전이_발행으로_읽히는가 = new boolean[1];
        long[] 미상_자리에_실린_재고 = new long[1];
        boolean[] 옛_버전이_읽은_새것 = new boolean[2];
        GatewaySnapshot[] 미상_왕복 = new GatewaySnapshot[1];
        GatewaySnapshot[] 모자란_것 = new GatewaySnapshot[1];
        GatewaySnapshot[] 남는_것 = new GatewaySnapshot[1];

        ChaosScenario.named("C20 혼재 버전 스냅샷")
                // **칸을 따로 둔다.** 같은 칸에 담으면 아래가 덮어써서 이 전제가
                // 아예 안 재진다 — 실제로 그렇게 두었더니 빈 해시로 바꿔도 초록이었다.
                .baseline(() -> {
                    자기가_낸_것[0] = 코덱.decode(새_버전이_낸_것(false));
                    // **자기가 낸 것도 발행으로 읽혀야 한다.** 발행 자리 이름이
                    // 바뀌면 쿠폰은 그대로 읽히면서 그 스냅샷이 통째로 버려진다 —
                    // 쿠폰만 보는 판정으로는 안 잡힌다.
                    자기_버전이_발행으로_읽히는가[0] = 코덱.isPublished(새_버전이_낸_것(false));
                })
                .inject(() -> {
                    // 리더가 옛 버전이다. 새 노드가 그 재료를 받아야 준비가 된다.
                    새_버전이_읽은_옛것[0] = 코덱.decode(옛_버전이_낸_것());
                    // **발행으로 읽히는가까지 본다.** 갱신 루프가 받아들이지 않으면
                    // 새 노드는 이 스냅샷을 통째로 버리고 준비 상태가 안 된다.
                    옛_버전이_발행으로_읽히는가[0] = 코덱.isPublished(옛_버전이_낸_것());
                    옛_버전이_읽은_새것[0] = 옛_버전이_받는가(새_버전이_낸_것(false));
                })
                .duringFault(() -> {
                    // 재고 미상은 예약 자리로 간다. 옛 버전은 그 자리를 건너뛴다.
                    옛_버전이_읽은_새것[1] = 옛_버전이_받는가(새_버전이_낸_것(true));
                    미상_자리에_실린_재고[0] = 옛_버전이_읽는_재고(새_버전이_낸_것(true));
                    미상_왕복[0] = 코덱.decode(새_버전이_낸_것(true));
                    // 자리가 남으면 무시하고, 모자라면 안 받는다.
                    Map<String, String> 남음 = 옛_버전이_낸_것();
                    남음.put(쿠폰, "ADAPTIVE:QUEUEING:100:500:2000:1.0:미래에_붙을_것");
                    남는_것[0] = 코덱.decode(남음);
                    Map<String, String> 모자람 = 옛_버전이_낸_것();
                    모자람.put(쿠폰, "ADAPTIVE:QUEUEING:100:500");
                    모자란_것[0] = 코덱.decode(모자람);
                })
                .recover(() -> { })
                .afterRecovery(() -> { })
                .assertEntry(() -> RecoveryCriteria.violations(
                        // 전제 — 자기가 낸 것은 당연히 읽는다. 안 읽으면 아래가 뜻이 없다.
                        쿠폰을_받았다("자기 버전", 자기가_낸_것[0]),
                        발행으로_읽힌다("자기 버전", 자기_버전이_발행으로_읽히는가[0]),
                        // 새 노드가 옛 리더의 재료를 받는다. 못 받으면 발행된
                        // 스냅샷을 하나도 못 받아 준비 상태가 안 되고 롤아웃이 멎는다.
                        쿠폰을_받았다("옛 버전", 새_버전이_읽은_옛것[0]),
                        // 옛 노드가 새 리더의 재료를 받는다. 못 받으면 이미 돌던
                        // 노드가 낡음으로 넘어가 전부 줄을 세운다.
                        옛_버전도_받았다("재고 아는 스냅샷", 옛_버전이_읽은_새것[0]),
                        발행으로_읽힌다("옛 버전", 옛_버전이_발행으로_읽히는가[0])))
                .assertDuring(() -> RecoveryCriteria.violations(
                        // **미상을 쿠폰 값에 안 싣는다.** 재고 자리에 음수를 넣으면
                        // 옛 생성자가 거부해 그 쿠폰이 통째로 빠지고, 양수를 넣으면
                        // 옛 노드가 재입고로 읽어 매진 방패를 푼다.
                        옛_버전도_받았다("재고 미상 스냅샷", 옛_버전이_읽은_새것[1]),
                        미상이_왕복한다(미상_왕복[0]),
                        // 옛 노드가 그 자리에서 읽는 값이 0 이어야 한다. 양수면
                        // 재입고로 읽어 매진 방패를 푼다.
                        미상은_재고_자리에_0_이다(미상_자리에_실린_재고[0]),
                        // 관대함은 한 방향뿐이다.
                        남는_자리를_무시한다(남는_것[0]),
                        모자란_자리는_안_받는다(모자란_것[0])))
                // **일부러 안 잰다.** 이 시나리오에는 걷을 장애가 없다 — 서로 다른 입력을
                // 넣어 볼 뿐이라 회복 구간이 성립하지 않는다. 빈 목록으로 두면
                // 빠뜨린 것과 구분이 안 된다.
                .assertRecovery(ChaosScenario.Verdict.none())
                // **RC1~RC6 은 여기서 안 잰다.** 이 시나리오는 스냅샷 부호화만 걷는다 —
                // 노드도 줄도 세우지 않는다. 롤아웃 중의 준비 상태와 낡음 전이는
                // 노드 둘짜리 하네스가 있어야 잰다.
                .run();
    }

    private Optional<String> 쿠폰을_받았다(String 누구, GatewaySnapshot 읽은_것) {
        return 읽은_것.coupons().containsKey(쿠폰) ? Optional.empty()
                : Optional.of("%s 이 낸 것에서 쿠폰이 빠졌다 — 그 노드는 재료를 못 받는다"
                        .formatted(누구));
    }

    private Optional<String> 옛_버전도_받았다(String 무엇, boolean 받았나) {
        return 받았나 ? Optional.empty()
                : Optional.of("옛 버전이 %s 을 못 읽는다 — 이미 돌던 노드가 낡음으로 넘어간다"
                        .formatted(무엇));
    }

    /** 미상이 예약 자리로 갔다가 그대로 돌아오는가. 접히면 거짓 매진이 된다. */
    private Optional<String> 미상이_왕복한다(GatewaySnapshot 읽은_것) {
        CouponState 상태 = 읽은_것.coupons().get(쿠폰);
        if (상태 == null) {
            return Optional.of("미상을 실은 스냅샷에서 쿠폰이 빠졌다");
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

    /**
     * <b>갱신 루프가 받아들이는 모양인가.</b> 발행 시각을 못 읽으면 EPOCH 이 되고,
     * 그 스냅샷은 "받아들일 수 없는 스냅샷" 으로 찍혀 통째로 버려진다 — 새 노드는
     * 준비 상태가 안 되고 롤아웃이 그 자리에서 멎는다.
     */
    private Optional<String> 발행으로_읽힌다(String 누구, boolean 읽히는가) {
        return 읽히는가 ? Optional.empty()
                : Optional.of("%s 이 발행으로 안 읽힌다 — 읽는 노드가 이 스냅샷을 통째로 버린다"
                        .formatted(누구));
    }

    /** 미상은 재고 자리에 0 이 나간다. 양수면 옛 노드가 재입고로 읽는다. */
    private Optional<String> 미상은_재고_자리에_0_이다(long 재고) {
        return 재고 == 0 ? Optional.empty()
                : Optional.of("미상인데 재고 자리에 %d 가 실렸다 — 옛 노드가 재입고로 읽는다"
                        .formatted(재고));
    }
}

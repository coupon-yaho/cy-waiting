package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.QueueMode;
import com.kafkick.waiting.domain.coupon.RuntimeState;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * 스냅샷 해시를 판정 재료로 옮긴다.
 *
 * <p><b>밖에서 쓰는 키다.</b> 스케줄러가 쓰고 게이트웨이가 읽는데 둘의 배포
 * 시점이 다르다 — 모르는 필드·깨진 값·빠진 값이 예외가 아니라 정상 입력이다.
 */
public final class SnapshotCodec {

    /** 전역값은 이 접두사를 단다. 쿠폰 ID 에는 못 들어간다 (RedisKeys 가 막는다). */
    private static final String RESERVED = "#";

    private static final String CREDIT = "#credit";
    private static final String NODES = "#nodes";
    private static final String PUBLISHED = "#published";

    /** {@code mode:runtime:credit:stock:waiting:pollScale} */
    private static final int FIELDS = 6;

    private SnapshotCodec() {
    }

    /** 상태가 없지만 인스턴스다 — 판이 늘면 여기 필드가 생긴다 (JS-13). */
    public static SnapshotCodec create() {
        return new SnapshotCodec();
    }

    public GatewaySnapshot decode(Map<String, String> hash) {
        Map<String, CouponState> coupons = new LinkedHashMap<>();
        hash.forEach((field, raw) -> {
            if (field.startsWith(RESERVED)) {
                return;   // 전역값이다. 쿠폰으로 세면 없는 쿠폰이 매진으로 보인다
            }
            CouponState state = 쿠폰으로(raw);
            if (state != null) {
                coupons.put(field, state);
            }
        });
        return new GatewaySnapshot(coupons, 전역값(hash), 발행시각(hash));
    }

    /**
     * 못 읽으면 {@code null} 이다 — 그 쿠폰만 빠진다.
     *
     * <p>여기서 던지면 뒷단 하나의 버그가 <b>게이트웨이 전체를 세운다.</b>
     * 불변식 위반도 같다 — 생성자가 거부하는 조합이 스냅샷에 실려 올 수 있다.
     */
    private CouponState 쿠폰으로(String raw) {
        String[] parts = raw.split(":", -1);
        if (parts.length != FIELDS) {
            return null;
        }
        try {
            return new CouponState(
                    QueueMode.valueOf(parts[0].toUpperCase(Locale.ROOT)),
                    RuntimeState.valueOf(parts[1].toUpperCase(Locale.ROOT)),
                    Long.parseLong(parts[2]),
                    Long.parseLong(parts[3]),
                    Long.parseLong(parts[4]),
                    Double.parseDouble(parts[5]));
        } catch (IllegalArgumentException e) {
            return null;   // 모르는 열거값·깨진 수·불변식 위반이 다 여기로 온다
        }
    }

    /** 모르면 0 이다. 모르는데 크게 잡으면 초과 배분이 된다. */
    private SnapshotMeta 전역값(Map<String, String> hash) {
        return new SnapshotMeta(정수(hash.get(CREDIT), 0), (int) 정수(hash.get(NODES), 1));
    }

    /** 발행 시각이 없으면 EPOCH — 어떤 임계로도 낡음이라 판정에 안 쓰인다. */
    private Instant 발행시각(Map<String, String> hash) {
        long at = 정수(hash.get(PUBLISHED), 0);
        return at > 0 ? Instant.ofEpochSecond(at) : Instant.EPOCH;
    }

    private long 정수(String raw, long 기본값) {
        if (raw == null) {
            return 기본값;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            return 기본값;
        }
    }
}

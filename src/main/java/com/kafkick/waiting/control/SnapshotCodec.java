package com.kafkick.waiting.control;

import com.kafkick.waiting.adapter.redis.RedisKeys;
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

    /**
     * 전역값은 이 접두사를 단다.
     *
     * <p>쿠폰 ID 에 들어가면 그 쿠폰이 전역값을 덮어쓴다 — {@code #credit} 이름의
     * 쿠폰 하나로 전 쿠폰의 몫이 0 이 된다. {@link RedisKeys} 가 막는다.
     */
    private static final String RESERVED = "#";

    private static final String CREDIT = "#credit";
    private static final String NODES = "#nodes";
    private static final String PUBLISHED = "#published";

    /** {@code mode:runtime:credit:stock:waiting:pollScale} */
    private static final int FIELDS = 6;

    /** {@link Instant#MAX} 를 넘으면 생성자가 던진다 — 넘기지 않고 걸러낸다. */
    private static final long MAX_EPOCH_SECOND = Instant.MAX.getEpochSecond();

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
        // **상한을 걸어 쪼갠다.** 뒤에서 길이를 보면 콜론 100만 개짜리 값이
        // 100만 원소를 먼저 만들고 버려진다 — 갱신 스레드가 OOM 으로 죽으면
        // "실패해도 옛 값을 유지한다" 는 설계가 통째로 무력해진다.
        // 남는 필드는 마지막 원소에 뭉쳐 길이가 하나 늘므로 판정은 같다.
        String[] parts = raw.split(":", FIELDS + 1);
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

    /**
     * 모르거나 말이 안 되면 보수적으로 잡는다 — 모르는데 크게 잡으면 초과 배분이다.
     *
     * <p><b>여기서 던지면 안 된다.</b> 던지면 그 필드가 고쳐질 때까지 갱신이
     * 영구히 멎고, 재시작한 노드는 빈 스냅샷에 갇혀 전 쿠폰이 매진으로 보인다.
     * 쿠폰 항목에 지킨 격리를 전역 항목에도 지킨다.
     */
    private SnapshotMeta 전역값(Map<String, String> hash) {
        long credit = Math.max(0, 정수(hash.get(CREDIT), 0));
        long nodes = 정수(hash.get(NODES), 1);
        // long→int 축소는 조용히 0 을 만든다. 그러면 전 노드가 "내가 유일하다"
        // 고 믿어 크레딧을 노드 수만큼 초과 배분한다.
        int 노드수 = (nodes >= 1 && nodes <= Integer.MAX_VALUE) ? (int) nodes : 1;
        return new SnapshotMeta(credit, 노드수);
    }

    /** 발행 시각이 없거나 말이 안 되면 EPOCH — 어떤 임계로도 낡음이다. */
    private Instant 발행시각(Map<String, String> hash) {
        long at = 정수(hash.get(PUBLISHED), 0);
        if (at <= 0 || at > MAX_EPOCH_SECOND) {
            return Instant.EPOCH;
        }
        return Instant.ofEpochSecond(at);
    }

    /**
     * 스케줄러가 발행한 것인가.
     *
     * <p>빈 해시는 장애가 아니라 <b>흔한 상태</b>다 — 데이터 없는 복제본 승격,
     * 키 만료, 리더 재선출 중 재작성. 그때 성공 응답을 그대로 받아들이면
     * 들고 있던 것이 지워지고 전 쿠폰이 매진으로 보인다.
     */
    public boolean 발행된것인가(Map<String, String> hash) {
        return !발행시각(hash).equals(Instant.EPOCH);
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

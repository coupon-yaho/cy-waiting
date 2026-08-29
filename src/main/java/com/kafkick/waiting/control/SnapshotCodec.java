package com.kafkick.waiting.control;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.QueueMode;
import com.kafkick.waiting.domain.coupon.RuntimeState;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.allocation.QueueingHysteresis;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.domain.coupon.Tunables;
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

    /**
     * 운영자가 배포 없이 고치는 값 (P-1).
     *
     * <p>리더가 매 틱 읽어 여기 실어 보냅니다. 스냅샷은 이미 매 틱 전 노드에
     * 닿으므로 설정 서버를 따로 붙이지 않습니다.
     */
    private static final String TUNABLES = "#tunables";

    /**
     * 전역 폴링 배수.
     *
     * <p>쿠폰 항목이 아니라 전역 항목이다 — 판 전체를 보고 나온 값 하나라,
     * 쿠폰별로 실으면 그 쿠폰이 빠질 때 배수도 같이 사라진다.
     */
    private static final String POLL_SCALE = "#pollScale";

    /**
     * 평활화 상태. <b>리더만 읽고 쓴다.</b>
     *
     * <p>판정 재료가 아니라 다음 리더에게 넘기는 장부다. 도메인 메타에 넣으면
     * 요청 경로가 안 쓰는 값을 들고 다니게 된다.
     */
    private static final String EWMA = "#ewma";
    private static final String EWMA_SEEDED = "#ewmaSeeded";

    /**
     * 히스테리시스 상태. <b>EWMA 만 실으면 이월이 반쪽이다.</b>
     *
     * <p>붙잡고 있던 대기열이 교체마다 한 틱 꺼졌다 켜지면 진동이 그대로 보인다.
     */
    private static final String QUEUEING = "#queueing";
    private static final String BELOW_EXIT = "#belowExitTicks";

    /**
     * 디코더가 요구하는 <b>최소</b> 필드 수 — {@code mode:runtime:credit:stock:waiting}.
     *
     * <p>발행은 여섯을 쓴다. 이것은 와이어 포맷이 아니라 관대함의 경계다.
     */
    private static final int MIN_FIELDS = 5;

    /** 재고 자리에 미상 대신 싣는 값. 양수라는 것 말고는 뜻이 없다. */
    private static final long NOT_SOLD_OUT = 1;

    /** {@link Instant#MAX} 를 넘으면 생성자가 던진다 — 넘기지 않고 걸러낸다. */
    private static final long MAX_EPOCH_SECOND = Instant.MAX.getEpochSecond();

    private SnapshotCodec() {
    }

    /** 상태가 없지만 인스턴스다 — 판이 늘면 여기 필드가 생긴다 (JS-13). */
    public static SnapshotCodec create() {
        return new SnapshotCodec();
    }

    /**
     * 이월 상태에 <b>기본값을 안 준다</b> — 편의 오버로드를 두면 이월을 지우는
     * 호출이 안 지우는 호출과 똑같이 생겨서 발행 경로가 늘 때 조용히 섞인다.
     * 리더마다 0 에서 시작하면 진동하기 가장 쉬운 회복 직후에 ETA 가 튄다 (F9).
     */
    public Map<String, String> encode(GatewaySnapshot snapshot, CreditSmoother.Snapshot smoothing,
            QueueingHysteresis.Snapshot hysteresis) {
        Map<String, String> hash = new LinkedHashMap<>();
        snapshot.coupons().forEach((couponId, state) -> {
            // 예약 접두사를 단 쿠폰 하나로 전 쿠폰의 몫이 0 이 된다.
            if (!couponId.startsWith(RESERVED)) {
                hash.put(couponId, encodeCoupon(state, snapshot.meta().pollScale()));
            }
        });
        hash.put(CREDIT, Long.toString(snapshot.meta().globalCredit()));
        hash.put(NODES, Integer.toString(snapshot.meta().gatewayCount()));
        hash.put(POLL_SCALE, Double.toString(snapshot.meta().pollScale()));
        hash.put(PUBLISHED, Long.toString(snapshot.publishedAt().getEpochSecond()));
        // **안 실린 것은 안 싣는다.** 기본값으로 채워 보내면 읽는 쪽이 그것을
        // "운영자가 정한 값" 으로 읽고, 각 노드의 기동 설정을 덮어쓴다.
        if (snapshot.meta().tunables() != null) {
            hash.put(TUNABLES, snapshot.meta().tunables().toJson());
        }
        hash.put(EWMA, Double.toString(smoothing.value()));
        hash.put(EWMA_SEEDED, smoothing.seeded() ? "1" : "0");
        hash.put(QUEUEING, hysteresis.queueing() ? "1" : "0");
        hash.put(BELOW_EXIT, Integer.toString(hysteresis.belowExitTicks()));
        return hash;
    }

    /**
     * 쿠폰 하나의 값. <b>여섯 번째 자리에 전역 배수를 싣는다.</b>
     *
     * <p>읽는 쪽은 이 자리를 안 보지만 아직 여섯을 기대하는 노드가 있고, 그
     * 노드는 이 자리를 <b>그 쿠폰의 배수</b>로 읽는다. 전역값을 그대로 실으면
     * 그 노드의 계산이 새 노드와 같아진다 — 읽는 자리가 달라도 답은 같다.
     */
    // 상수를 박으면 롤아웃 구간 내내 옛 파드 전부가 배수 없이 폴링한다. 파드
    // 대부분이 아직 옛것인 구간이 있으므로, 새 리더가 보호가 걸렸다고 보고하는
    // 동안 클러스터는 예산을 한참 넘긴 채로 돈다.
    //
    // 관대한 디코더가 전 노드에 깔린 것이 확인되면 이 자리를 지운다 (CY-736).
    private String encodeCoupon(CouponState state, double pollScale) {
        return "%s:%s:%d:%d:%d:%s".formatted(state.mode(), state.runtime(), state.credit(),
                wireStock(state), state.waiting(), pollScale);
    }

    /**
     * 재고 자리에 실을 수. <b>미상은 매진이 아니라는 뜻의 양수로 나간다.</b>
     *
     * <p>미상을 그대로 실으면 옛 노드의 생성자가 음수 재고를 거부해 그 항목이
     * 통째로 빠지고, 없는 쿠폰은 판정에서 매진으로 보인다 — 접힘을 없애려던
     * 변경이 롤아웃 구간에 같은 종결을 되살린다.
     *
     * <p>읽는 쪽은 이 수를 {@link CouponState#soldOut()} 으로만 쓰고 셈에 안
     * 넣는다. 미상인 것을 알아야 하는 곳은 리더의 정리 판단뿐이고, 그 경로는
     * 이 값을 안 거친다 (3.1).
     */
    private long wireStock(CouponState state) {
        return state.stockKnown() ? state.remainingStock() : NOT_SOLD_OUT;
    }

    /**
     * 이월받은 평활화 상태. <b>못 읽으면 안 받은 것으로 본다.</b>
     *
     * <p>여기서 던지면 리더가 바뀔 때마다 배분이 멎는다. 그리고 NaN 이나 음수를
     * 그대로 받으면 그 순간부터 평활화가 영영 죽는데, 리더가 바뀐 뒤에야 드러난다.
     */
    public CreditSmoother.Snapshot smoothing(Map<String, String> hash) {
        String raw = hash.get(EWMA);
        if (raw == null || !"1".equals(hash.get(EWMA_SEEDED))) {
            return CreditSmoother.Snapshot.empty();
        }
        try {
            return new CreditSmoother.Snapshot(Double.parseDouble(raw), true);
        } catch (IllegalArgumentException e) {
            return CreditSmoother.Snapshot.empty();
        }
    }

    /**
     * 유지 틱만 못 읽으면 <b>붙잡던 것은 지킨다</b> — 거기서 놓으면 대기열이
     * 꺼졌다 켜져, 막으려던 진동이 리더 교체마다 난다. 모순된 조합은 통째로
     * 버린다. 그대로 받으면 새 리더가 켜지자마자 끄는 판단을 하고, 여기서
     * 던지면 리더가 바뀔 때마다 배분이 멎는다.
     */
    public QueueingHysteresis.Snapshot hysteresis(Map<String, String> hash) {
        boolean queueing = "1".equals(hash.get(QUEUEING));
        String raw = hash.get(BELOW_EXIT);
        if (raw == null) {
            // **붙잡던 것은 유지한다.** 유지 틱만 못 읽었다고 놓아 버리면 그
            // 순간 대기열이 꺼졌다 켜진다 — 막으려던 진동이 그대로 난다.
            return new QueueingHysteresis.Snapshot(queueing, 0);
        }
        try {
            return new QueueingHysteresis.Snapshot(queueing, Integer.parseInt(raw));
        } catch (IllegalArgumentException e) {
            return new QueueingHysteresis.Snapshot(queueing, 0);
        }
    }

    public GatewaySnapshot decode(Map<String, String> hash) {
        Map<String, CouponState> coupons = new LinkedHashMap<>();
        hash.forEach((field, raw) -> {
            if (field.startsWith(RESERVED)) {
                return;   // 전역값이다. 쿠폰으로 세면 없는 쿠폰이 매진으로 보인다
            }
            CouponState state = toCouponState(raw);
            if (state != null) {
                coupons.put(field, state);
            }
        });
        return new GatewaySnapshot(coupons, toMeta(hash), publishedAtOf(hash));
    }

    /**
     * 못 읽으면 {@code null} 이다 — 그 쿠폰만 빠진다.
     *
     * <p>여기서 던지면 뒷단 하나의 버그가 <b>게이트웨이 전체를 세운다.</b>
     * 불변식 위반도 같다 — 생성자가 거부하는 조합이 스냅샷에 실려 올 수 있다.
     */
    private CouponState toCouponState(String raw) {
        // **상한을 걸어 쪼갠다.** 뒤에서 길이를 보면 콜론 100만 개짜리 값이
        // 100만 원소를 먼저 만들고 버려진다 — 갱신 스레드가 OOM 으로 죽으면
        // "실패해도 옛 값을 유지한다" 는 설계가 통째로 무력해진다.
        // 남는 필드는 마지막 원소에 뭉쳐 길이가 하나 늘므로 판정은 같다.
        String[] parts = raw.split(":", MIN_FIELDS + 1);
        // **모르는 필드는 무시한다** (E-12). 배포는 한 순간에 안 끝나므로,
        // 구·신 버전이 섞이는 구간에 형식이 갈리면 신버전이 옛 재료를 통째로
        // 버린다 — 그 노드는 발행된 스냅샷을 하나도 못 받아 준비가 안 되고
        // 롤아웃이 멈춘다. 이미 돌던 노드는 낡음으로 넘어가 전부 줄을 세운다.
        //
        // **관대함은 한 방향뿐이다.** 모자란 것은 받아 주지 않는다. 받으면
        // 자리가 밀린 값을 그대로 믿게 되고, 그건 판정을 바꾼다.
        if (parts.length < MIN_FIELDS) {
            return null;
        }
        try {
            return new CouponState(
                    QueueMode.valueOf(parts[0].toUpperCase(Locale.ROOT)),
                    RuntimeState.valueOf(parts[1].toUpperCase(Locale.ROOT)),
                    Long.parseLong(parts[2]),
                    Long.parseLong(parts[3]),
                    Long.parseLong(parts[4]));
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
    private SnapshotMeta toMeta(Map<String, String> hash) {
        long credit = Math.max(0, parseLongOr(hash.get(CREDIT), 0));
        long nodes = parseLongOr(hash.get(NODES), 1);
        // long→int 축소는 조용히 0 을 만든다. 그러면 전 노드가 "내가 유일하다"
        // 고 믿어 크레딧을 노드 수만큼 초과 배분한다.
        int nodeCount = (nodes >= 1 && nodes <= Integer.MAX_VALUE) ? (int) nodes : 1;
        // **안 실려 왔으면 null 이다.** 기본값으로 채우면 그 값이 기동 설정을
        // 덮어써서, 운영자가 아무것도 안 바꿨는데 값이 바뀐다. 실려 왔는데 못
        // 읽는 것은 다른 얘기라, 그때는 파서가 값별로 기본값으로 떨어뜨린다.
        // **배수는 못 읽으면 1.0 이다.** 크게 잡으면 예산이 멀쩡한데도 전원이
        // 뜸하게 묻고, 그만큼 차례가 온 사실을 늦게 안다.
        return new SnapshotMeta(credit, nodeCount, tunablesOf(hash),
                parseScaleOr(hash.get(POLL_SCALE)));
    }

    /** 못 읽거나 말이 안 되면 배수 없음이다. 여기서 던지면 갱신이 통째로 멎는다. */
    private double parseScaleOr(String raw) {
        if (raw == null) {
            return 1.0;
        }
        try {
            double parsed = Double.parseDouble(raw);
            return Double.isFinite(parsed) ? parsed : 1.0;
        } catch (NumberFormatException e) {
            return 1.0;
        }
    }

    /**
     * 실려 온 운영 값.
     *
     * <p><b>여기서 던지면 안 된다.</b> 값 하나가 전 노드의 디코드를 동시에
     * 멈추고, 그러면 재시작한 노드가 빈 스냅샷에 갇힌다 — 쿠폰 항목에 지킨
     * 격리를 여기도 지킨다. 안 실려 온 것은 {@code null} 이다.
     */
    private Tunables tunablesOf(Map<String, String> hash) {
        String raw = hash.get(TUNABLES);
        if (raw == null) {
            return null;
        }
        try {
            return Tunables.parse(raw);
        } catch (RuntimeException e) {
            return Tunables.defaults();
        }
    }

    /** 발행 시각이 없거나 말이 안 되면 EPOCH — 어떤 임계로도 낡음이다. */
    private Instant publishedAtOf(Map<String, String> hash) {
        long at = parseLongOr(hash.get(PUBLISHED), 0);
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
    public boolean isPublished(Map<String, String> hash) {
        return !publishedAtOf(hash).equals(Instant.EPOCH);
    }

    private long parseLongOr(String raw, long fallback) {
        if (raw == null) {
            return fallback;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}

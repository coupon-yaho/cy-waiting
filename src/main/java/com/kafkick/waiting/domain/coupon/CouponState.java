package com.kafkick.waiting.domain.coupon;

/**
 * 판정에 쓰는 쿠폰 하나의 상태. 스냅샷에서 읽어 온 값이다.
 *
 * <p><b>불변식을 문서가 아니라 생성자가 지킨다.</b> 픽스처가 존재할 수 없는 상태를
 * 만들 수 있으면 테스트가 버그를 증명하지 못한다. 도달 가능한 상태만 만들려면
 * {@code CouponStates} 팩토리를 쓴다.
 *
 * @param mode           운영자가 정한 대기열 정책
 * @param runtime        기계가 관측한 현재 상태
 * @param credit         이 쿠폰에 배분된 초당 통과 몫
 * @param remainingStock 남은 재고. <b>발급 계층이 소유</b>하고 게이트웨이는 읽기만 한다
 * @param waiting        줄 서 있는 사람 수
 * @param pollScale      폴링 간격 배수. 예산이 빠듯하면 커진다
 */
public record CouponState(
        QueueMode mode,
        RuntimeState runtime,
        long credit,
        long remainingStock,
        long waiting,
        double pollScale) {

    public CouponState {
        if (mode == null || runtime == null) {
            throw new IllegalArgumentException("mode 와 runtime 은 필수다");
        }
        if (credit < 0 || remainingStock < 0 || waiting < 0) {
            throw new IllegalArgumentException(
                    "음수가 될 수 없다: credit=%d, remainingStock=%d, waiting=%d"
                            .formatted(credit, remainingStock, waiting));
        }

        // I1 — IDLE 은 배분을 못 받았다는 뜻이다. 이 둘은 독립 값이 아니라
        // 같은 원인에서 나온다. 갈라지면 "한산한 쿠폰일수록 큐로 간다"는
        // 역전이 생기고, 그게 이전 구현의 핵심 버그였다.
        if (runtime == RuntimeState.IDLE && credit != 0) {
            throw new IllegalArgumentException(
                    "IDLE 이면 credit 이 0 이어야 한다: credit=%d".formatted(credit));
        }

        // I2 — 재고가 남았는데 종결됐다면 그건 종결이 아니다.
        if (runtime == RuntimeState.CLOSED && remainingStock != 0) {
            throw new IllegalArgumentException(
                    "CLOSED 면 remainingStock 이 0 이어야 한다: remainingStock=%d"
                            .formatted(remainingStock));
        }

        // I3 — DRAINING 은 "이번 틱에 남은 대기자를 다 뺄 수 있다"는 뜻이다.
        // 몫이 대기자보다 적으면 그건 아직 QUEUEING 이다.
        if (runtime == RuntimeState.DRAINING && credit < waiting) {
            throw new IllegalArgumentException(
                    "DRAINING 이면 credit >= waiting 이어야 한다: credit=%d, waiting=%d"
                            .formatted(credit, waiting));
        }

        // I4 — 줄이 비었는데 큐 상태라는 것은 유령이다. 판정 사다리 9번이
        // IDLE 쿠폰만 받는다는 논증이 이 불변식의 대우에 걸려 있다.
        if (waiting == 0
                && runtime != RuntimeState.IDLE
                && runtime != RuntimeState.CLOSED) {
            throw new IllegalArgumentException(
                    "waiting 이 0 이면 IDLE 또는 CLOSED 여야 한다: runtime=%s".formatted(runtime));
        }

        // I6 — 거부가 아니라 정규화다. 1 미만은 폴링을 더 자주 하라는 뜻이
        // 되는데 그건 예산을 늘리는 방향이라 의미가 없다.
        pollScale = Math.max(1.0, pollScale);
    }

    /**
     * 경합 쿠폰이 이 노드에서 쓸 수 있는 몫. 노드 번호를 모를 때 쓴다.
     *
     * <p>나머지를 버리므로 총합이 {@code credit} 을 넘지 않는다. 대신 나머지만큼
     * 덜 나간다 — 초과는 장애고 미달은 지연이다.
     */
    public long contendedCap(int gatewayCount) {
        return credit / Math.max(1, gatewayCount);
    }

    /**
     * 경합 쿠폰이 이 노드에서 쓸 수 있는 몫. 나머지를 노드 번호로 나눠 갖는다.
     *
     * <p>{@code credit} 이 노드 수보다 작으면 정수 나눗셈으로 전 노드가 0 이 된다.
     * 그렇다고 {@code max(1, …)} 로 올리면 노드 수만큼 나가 <b>초과 배분</b>이다 —
     * credit 10 에 노드 20 이면 20 이 나간다. 앞쪽 노드에만 1 을 준다.
     */
    public long contendedCap(int gatewayCount, int nodeIndex) {
        int n = Math.max(1, gatewayCount);
        long base = credit / n;
        long remainder = credit % n;
        return base + (nodeIndex < remainder ? 1 : 0);
    }

    /**
     * 한산한 쿠폰이 이 노드에서 쓸 수 있는 상한.
     *
     * <p><b>이 쿠폰의 credit 으로 재지 않는다.</b> IDLE 이면 credit 이 0 이라(I1)
     * 한산한 쿠폰일수록 반드시 큐로 가는 역전이 생긴다 — 이전 구현의 핵심 버그다.
     * 노드 몫의 전역 크레딧으로 잰다.
     */
    public long idleCap(SnapshotMeta meta, double idleCreditRatio) {
        long perNode = meta.globalCredit() / meta.effectiveGatewayCount();
        return (long) (perNode * idleCreditRatio);
    }

    /**
     * 지금 줄이 빠지는 데 걸리는 시간(초).
     *
     * <p>{@code credit} 이 0 이면 영원히 안 빠진다 — 예외가 아니라 무한이 맞다.
     * 한산한 쿠폰이 정확히 그 상태이므로(I1) 방어가 없으면 R1 경로가 터진다.
     */
    public double queueDepthSec() {
        if (waiting == 0) {
            return 0.0;
        }
        return credit == 0 ? Double.POSITIVE_INFINITY : (double) waiting / credit;
    }

    /**
     * 받아도 되는 줄의 최대 길이.
     *
     * <p>배수할 수 없는데(credit 0) 줄을 받으면 갇힌 사람만 늘어난다.
     */
    public long queueCapacity(long maxEtaSec) {
        return credit * maxEtaSec;
    }

    /** 아무도 줄을 서지 않았다. 배분을 못 받았으므로 credit 은 0 이다. */
    public static CouponState idle(long remainingStock) {
        return new CouponState(QueueMode.ADAPTIVE, RuntimeState.IDLE, 0, remainingStock, 0, 1.0);
    }

    /** 줄이 생겼다. 상한을 넘은 초과분이 큐로 들어가면서 이 상태가 된다. */
    public static CouponState queueing(long credit, long remainingStock, long waiting) {
        return new CouponState(
                QueueMode.ADAPTIVE, RuntimeState.QUEUEING, credit, remainingStock, waiting, 1.0);
    }

    /** 이번 틱에 남은 대기자를 다 빼줄 수 있다. 배분이 대기자를 따라잡으면 여기로 온다. */
    public static CouponState draining(long credit, long remainingStock, long waiting) {
        return new CouponState(
                QueueMode.ADAPTIVE, RuntimeState.DRAINING, credit, remainingStock, waiting, 1.0);
    }

    /** 재고가 소진됐는데 대기자가 남았다. 스케줄러가 이 전이를 만든다. */
    public static CouponState closed(long waiting) {
        return new CouponState(QueueMode.ADAPTIVE, RuntimeState.CLOSED, 0, 0, waiting, 1.0);
    }

    /** 운영자가 대기열을 껐다. 붐비든 말든 줄을 세우지 않는다. */
    public static CouponState off(long remainingStock) {
        return new CouponState(QueueMode.OFF, RuntimeState.IDLE, 0, remainingStock, 0, 1.0);
    }

    /**
     * 스냅샷에 없는 쿠폰. 판정이 {@code null} 을 다루지 않게 하려는 것이지
     * 통과시키려는 게 아니다 — 미지 쿠폰은 요청 경로에서 404 로 끊는다.
     */
    public static CouponState unknown() {
        return new CouponState(QueueMode.ADAPTIVE, RuntimeState.CLOSED, 0, 0, 0, 1.0);
    }
}

package com.kafkick.waiting.domain.coupon;

/**
 * 판정에 쓰는 쿠폰 하나의 상태. <b>불변식을 문서가 아니라 생성자가 지킨다</b> —
 * 픽스처가 존재할 수 없는 상태를 만들면 테스트가 버그를 증명하지 못한다.
 * 도달 가능한 상태만 만들려면 {@code CouponStates} 팩토리를 쓴다.
 *
 * @param mode           운영자가 정한 대기열 정책
 * @param runtime        기계가 관측한 현재 상태
 * @param credit         이 쿠폰에 배분된 초당 통과 몫
 * @param remainingStock 남은 재고. {@link #STOCK_UNKNOWN} 이면 셈에 안 넣고 {@link #soldOut()} 으로만 묻는다
 * @param waiting        줄 서 있는 사람 수
 */
public record CouponState(
        QueueMode mode,
        RuntimeState runtime,
        long credit,
        long remainingStock,
        long waiting) {

    /**
     * 재고를 <b>못 읽었다</b>는 뜻. 다 팔린 것(0)과 다른 값이라야 한다 — 같은 값이면
     * 재고 키를 잃은 쿠폰이 종결되고 큐까지 지워진다. 경계를 넘는 것은 이 값이
     * 아니라 {@link #stockKnown()} 이라, 수요 쪽 값과 같은 수일 필요는 없다.
     */
    public static final long STOCK_UNKNOWN = -1;

    public CouponState {
        if (mode == null || runtime == null) {
            throw new IllegalArgumentException("mode 와 runtime 은 필수다");
        }
        // 미상을 뜻하는 한 값 말고는 음수를 안 받는다. 열어 두면 오타가 값이 된다.
        if (credit < 0 || waiting < 0
                || (remainingStock < 0 && remainingStock != STOCK_UNKNOWN)) {
            throw new IllegalArgumentException(
                    "음수가 될 수 없다. 재고를 못 읽었으면 stockUnknown 을 쓴다: "
                            + "credit=%d, remainingStock=%d, waiting=%d"
                            .formatted(credit, remainingStock, waiting));
        }

        // I1 — IDLE 은 배분을 못 받았다는 뜻이다. 이 둘은 독립 값이 아니라
        // 같은 원인에서 나온다. 갈라지면 "한산한 쿠폰일수록 큐로 간다"는
        // 역전이 생기고, 그게 이전 구현의 핵심 버그였다.
        if (runtime == RuntimeState.IDLE && credit != 0) {
            throw new IllegalArgumentException(
                    "[I1] IDLE 이면 credit 이 0 이어야 한다: credit=%d".formatted(credit));
        }

        // I2 — 재고가 남았는데 종결됐다면 그건 종결이 아니다.
        if (runtime == RuntimeState.CLOSED && remainingStock != 0) {
            throw new IllegalArgumentException(
                    "[I2] CLOSED 면 remainingStock 이 0 이어야 한다: remainingStock=%d"
                            .formatted(remainingStock));
        }

        // I3 — DRAINING 은 "이번 틱에 남은 대기자를 다 뺄 수 있다"는 뜻이다.
        // 몫이 대기자보다 적으면 그건 아직 QUEUEING 이다.
        if (runtime == RuntimeState.DRAINING && credit < waiting) {
            throw new IllegalArgumentException(
                    "[I3] DRAINING 이면 credit >= waiting 이어야 한다: credit=%d, waiting=%d"
                            .formatted(credit, waiting));
        }

        // I1' — IDLE 인데 줄이 서 있으면 판정 8번(runtime != IDLE)이 통과시켜
        // 줄 선 사람을 추월한다. I4 의 대우로는 이 조합이 막히지 않는다.
        if (runtime == RuntimeState.IDLE && waiting != 0) {
            throw new IllegalArgumentException(
                    "[I1'] IDLE 이면 waiting 이 0 이어야 한다: waiting=%d".formatted(waiting));
        }

        // I4 — 줄이 비었는데 큐 상태라는 것은 유령이다. 판정 사다리 9번이
        // IDLE 쿠폰만 받는다는 논증이 이 불변식의 대우에 걸려 있다.
        if (waiting == 0
                && runtime != RuntimeState.IDLE
                && runtime != RuntimeState.CLOSED) {
            throw new IllegalArgumentException(
                    "[I4] waiting 이 0 이면 IDLE 또는 CLOSED 여야 한다: runtime=%s".formatted(runtime));
        }

        // I3' — 반대 방향도 막아 같은 (credit, waiting) 이 두 상태를 갖지 않게 한다.
        // 판정은 안 달라진다. 사다리가 runtime 을 != IDLE 로만 보기 때문이다.
        // **I4 뒤에 둔다.** 앞에 두면 줄이 빈 QUEUEING 이 원인을 잘못 말한다.
        if (runtime == RuntimeState.QUEUEING && credit >= waiting) {
            throw new IllegalArgumentException(
                    "[I3'] QUEUEING 이면 credit < waiting 이어야 한다: credit=%d, waiting=%d"
                            .formatted(credit, waiting));
        }
    }

    /**
     * 경합 쿠폰이 이 노드에서 쓸 수 있는 몫. 노드 번호를 모를 때 쓴다. 나머지를
     * 버려 총합이 {@code credit} 을 안 넘는다 — 초과는 장애고 미달은 지연이다.
     */
    public long contendedCap(int gatewayCount) {
        return credit / Math.max(1, gatewayCount);
    }

    /**
     * 경합 쿠폰이 이 노드에서 쓸 수 있는 몫. 나머지는 앞쪽 노드부터 하나씩 준다 —
     * {@code max(1, …)} 로 올리면 credit 10 에 노드 20 에서 20 이 나가 <b>초과 배분</b>이다.
     */
    public long contendedCap(int gatewayCount, int nodeIndex) {
        int n = Math.max(1, gatewayCount);
        long base = credit / n;
        long remainder = credit % n;
        return base + (nodeIndex < remainder ? 1 : 0);
    }

    /**
     * 한산한 쿠폰의 노드 상한. <b>이 쿠폰의 credit 으로는 못 잰다</b> — IDLE 이면
     * 0 이라 한산할수록 큐로 가는 역전이 생긴다. 노드 몫의 전역 크레딧으로 잰다.
     */
    public long idleCap(SnapshotMeta meta, double idleCreditRatio) {
        if (!Double.isFinite(idleCreditRatio) || idleCreditRatio < 0) {
            throw new IllegalArgumentException(
                    "idleCreditRatio 는 0 이상 유한값이어야 한다: %s".formatted(idleCreditRatio));
        }
        long perNode = meta.globalCredit() / meta.effectiveGatewayCount();
        long capped = (long) (perNode * idleCreditRatio);
        // 절삭으로 0 이 되면 아무도 안 몰리는 쿠폰이 전 노드에서 줄을 선다.
        // perNode >= 1 일 때만 1 을 얹으므로 총합은 globalCredit 을 안 넘는다.
        // 비율 0 은 예외다 — 절삭이 아니라 한산 통과를 끈다는 설정이다.
        return capped == 0 && perNode > 0 && idleCreditRatio > 0 ? 1 : capped;
    }

    /**
     * 지금 줄이 빠지는 데 걸리는 시간(초). {@code credit} 이 0 이면 예외가 아니라
     * 무한이 맞다 — 한산한 쿠폰이 정확히 그 상태라, 막으면 줄 없이 통과하는
     * 경로가 통째로 터진다.
     */
    public double queueDepthSec() {
        if (waiting == 0) {
            return 0.0;
        }
        return credit == 0 ? Double.POSITIVE_INFINITY : (double) waiting / credit;
    }

    /**
     * 받아도 되는 줄의 최대 길이. <b>사다리 6번이 보는 값이다.</b> 배수할 수
     * 없는데(credit 0) 줄을 받으면 갇힌 사람만 늘어난다. 등록 경로는 그때
     * {@code AdmissionDecider.queueCapacity} 의 폴백으로 갈아탄다.
     */
    public long queueCapacity(long maxEtaSec) {
        if (maxEtaSec <= 0) {
            return 0;
        }
        // 곱셈이 넘치면 음수가 되어 큐 상한이 사실상 0 이 된다 — 전원 거절이다.
        try {
            return Math.multiplyExact(credit, maxEtaSec);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    /** 아무도 줄을 서지 않았다. 배분을 못 받았으므로 credit 은 0 이다. */
    public static CouponState idle(long remainingStock) {
        return new CouponState(QueueMode.ADAPTIVE, RuntimeState.IDLE, 0, remainingStock, 0);
    }

    /** 줄이 생겼다. 상한을 넘은 초과분이 큐로 들어가면서 이 상태가 된다. */
    public static CouponState queueing(long credit, long remainingStock, long waiting) {
        return new CouponState(
                QueueMode.ADAPTIVE, RuntimeState.QUEUEING, credit, remainingStock, waiting);
    }

    /** 이번 틱에 남은 대기자를 다 빼줄 수 있다. 배분이 대기자를 따라잡으면 여기로 온다. */
    public static CouponState draining(long credit, long remainingStock, long waiting) {
        return new CouponState(
                QueueMode.ADAPTIVE, RuntimeState.DRAINING, credit, remainingStock, waiting);
    }

    /**
     * 매진인가. <b>발급 판정과 순번 조회가 같이 부른다</b> — 각자 재고를 해석하면 같은
     * 쿠폰에 정반대로 답하는 순간이 생긴다. 런타임 상태로는 못 읽는다: 큐를 정리해
     * 대기자가 0 이 된 매진 쿠폰은 IDLE 로 떨어진다.
     */
    public boolean soldOut() {
        return stockKnown() && remainingStock <= 0;
    }

    /** 재고를 읽었는가. <b>미상은 매진이 아니다</b> — 그 구분이 여기서 난다. */
    public boolean stockKnown() {
        return remainingStock != STOCK_UNKNOWN;
    }

    /** 재고가 소진됐는데 대기자가 남았다. 스케줄러가 이 전이를 만든다. */
    public static CouponState closed(long waiting) {
        return closed(QueueMode.ADAPTIVE, waiting);
    }

    /**
     * 매진된 쿠폰도 <b>운영자가 정한 모드를 그대로 싣는다.</b> 판정은 1번에서
     * 끝나 모드를 안 보지만, 모드를 읽는 소비자가 하나라도 늘면 그날 이 자리가
     * 거짓말을 한다 — 대기 응답이 이미 모드를 싣는다.
     */
    public static CouponState closed(QueueMode mode, long waiting) {
        return new CouponState(mode, RuntimeState.CLOSED, 0, 0, waiting);
    }

    /**
     * 줄이 남아 있는 쿠폰. <b>모드는 운영자가 정한 그대로 싣는다</b> — 바꿔 실으면 대기
     * 응답의 모드가 사실이 아니게 되고, 항상 대기로 둔 쿠폰이 적응형으로 돌아간다.
     * 런타임은 못 박지 않고 유도한다 ({@link #offWithQueue} 와 같은 이유).
     */
    public static CouponState withQueue(QueueMode mode, long credit, long remainingStock,
            long waiting) {
        if (waiting <= 0) {
            throw new IllegalArgumentException(
                    "withQueue 는 줄이 남아 있을 때만이다. 비었으면 noQueue 를 쓴다: waiting=%d"
                            .formatted(waiting));
        }
        // 재고가 없는데 줄이 남았으면 매진이다. 여기서 만들면 아무것도 못 받을 줄에
        // 사람을 계속 세운다. **미상은 안 걸린다** — 못 읽은 것을 매진으로 접으면 그
        // 줄이 종결되고 큐까지 지워진다. 초과 발급을 막는 상한은 뒷단이 지킨다.
        if (remainingStock != STOCK_UNKNOWN && remainingStock <= 0) {
            throw new IllegalArgumentException(
                    "재고가 없으면 매진이다. closed 를 쓴다: remainingStock=%d"
                            .formatted(remainingStock));
        }
        // 이번 틱에 다 뺄 수 있으면 배수 중, 아니면 아직 줄 서는 중이다.
        // **생성자의 DRAINING 경계와 같은 자리**를 쓴다 — 갈리면 이 팩토리가 생성자에
        // 막히는 조합을 만든다. 그래서 이 셈은 여기 한 곳에만 있다.
        RuntimeState runtime = credit >= waiting
                ? RuntimeState.DRAINING
                : RuntimeState.QUEUEING;
        return new CouponState(mode, runtime, credit, remainingStock, waiting);
    }

    /**
     * 재고를 못 읽은 쿠폰. 줄은 그대로 돌리고 <b>매진으로는 안 접는다</b> — 굶기지
     * 않으려면 몫을 안 깎아야 하고, 종결하지 않으려면 매진이 아니어야 한다. 줄이
     * 비면 몫도 0 이라야 유휴와 배분이 갈라지지 않는다.
     */
    public static CouponState stockUnknown(QueueMode mode, long credit, long waiting) {
        return waiting > 0
                ? withQueue(mode, credit, STOCK_UNKNOWN, waiting)
                : new CouponState(mode, RuntimeState.IDLE, credit, STOCK_UNKNOWN, 0);
    }

    /** 줄이 빈 쿠폰. 배분을 못 받았으므로 credit 은 0 이다. */
    public static CouponState noQueue(QueueMode mode, long remainingStock) {
        return new CouponState(mode, RuntimeState.IDLE, 0, remainingStock, 0);
    }

    /**
     * 운영자가 껐는데 <b>줄이 아직 남아 있다.</b> {@code mode} 와 {@code waiting} 은
     * 서로 독립이다. 런타임은 못 박지 않고 유도한다 — 못 박으면 다 뺄 수 있는 줄까지
     * {@code QUEUEING} 이 되어 생성자에 막힌다.
     */
    public static CouponState offWithQueue(long credit, long remainingStock, long waiting) {
        // **가드는 여기 남긴다.** 이름이 "줄이 있는 OFF" 이므로 비었을 때
        // 무엇을 쓰라고 그 자리에서 말해야 한다. 런타임 유도는 위임한다 —
        // DRAINING 경계를 두 곳에 적으면 갈린다.
        if (waiting <= 0) {
            throw new IllegalArgumentException(
                    "offWithQueue 는 줄이 남아 있을 때만이다. 비었으면 off 를 쓴다: waiting=%d"
                            .formatted(waiting));
        }
        return withQueue(QueueMode.OFF, credit, remainingStock, waiting);
    }

    /** 운영자가 무조건 줄을 세우기로 했다. 한산해도 대기열을 태운다. */
    public static CouponState always(long remainingStock) {
        return new CouponState(QueueMode.ALWAYS, RuntimeState.IDLE, 0, remainingStock, 0);
    }

    /** 운영자가 대기열을 껐다. 줄이 비어 있는 동안 줄을 세우지 않는다. */
    public static CouponState off(long remainingStock) {
        return new CouponState(QueueMode.OFF, RuntimeState.IDLE, 0, remainingStock, 0);
    }

    /**
     * 스냅샷에 없는 쿠폰. 판정이 {@code null} 을 다루지 않게 하려는 것이지
     * 통과시키려는 게 아니다 — 미지 쿠폰은 요청 경로에서 404 로 끊는다.
     */
    public static CouponState unknown() {
        return new CouponState(QueueMode.ADAPTIVE, RuntimeState.CLOSED, 0, 0, 0);
    }
}

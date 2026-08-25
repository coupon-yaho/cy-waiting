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

        // I3' — **반대 방향도 막는다.** 한쪽만 보면 같은 (credit, waiting) 이
        // 두 상태를 다 가질 수 있다. 그러면 상태가 사실을 안 말하고, 두 발행자가
        // 같은 사실을 다른 이름으로 적는다.
        //
        // **판정은 이걸로 달라지지 않는다.** 사다리는 runtime 을 `!= IDLE` 로만
        // 보므로 DRAINING 과 QUEUEING 이 같은 칸이다. 줄이 있으면 뒤에 세우는
        // 것이 맞고(불변식 4), 다 뺄 수 있다고 통과시키면 그게 추월이다.
        // 여기서 얻는 것은 **표현의 유일성**이지 판정의 변화가 아니다.
        //
        // **I4 뒤에 둔다.** 앞에 두면 줄이 빈 QUEUEING 이 여기서 먼저 걸려
        // "줄이 비었다" 대신 "다 뺄 수 있다" 고 답한다 — 원인을 잘못 말한다.
        if (runtime == RuntimeState.QUEUEING && credit >= waiting) {
            throw new IllegalArgumentException(
                    "[I3'] QUEUEING 이면 credit < waiting 이어야 한다: credit=%d, waiting=%d"
                            .formatted(credit, waiting));
        }

        // NaN 은 비교가 전부 false 라 Math.max 를 그냥 통과한다. 그대로 두면
        // 폴링 간격 계산이 조용히 NaN 이 되어 대기자가 폴링을 멈춘다.
        if (Double.isNaN(pollScale)) {
            throw new IllegalArgumentException("pollScale 이 NaN 이다");
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
        if (!Double.isFinite(idleCreditRatio) || idleCreditRatio < 0) {
            throw new IllegalArgumentException(
                    "idleCreditRatio 는 0 이상 유한값이어야 한다: %s".formatted(idleCreditRatio));
        }
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
     * 받아도 되는 줄의 최대 길이. <b>사다리 5번이 보는 값이다.</b> 배수할 수
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
        return closed(QueueMode.ADAPTIVE, waiting);
    }

    /**
     * 매진된 쿠폰도 <b>운영자가 정한 모드를 그대로 싣는다.</b> 판정은 1번에서
     * 끝나 모드를 안 보지만, 모드를 읽는 소비자가 하나라도 늘면 그날 이 자리가
     * 거짓말을 한다 — 대기 응답이 이미 모드를 싣는다.
     */
    public static CouponState closed(QueueMode mode, long waiting) {
        return new CouponState(mode, RuntimeState.CLOSED, 0, 0, waiting, 1.0);
    }

    /**
     * 줄이 남아 있는 쿠폰. <b>모드는 운영자가 정한 그대로 싣는다</b> — 줄이 있다고
     * 모드를 바꿔 실으면 대기 응답의 모드가 사실이 아니게 되고, 항상 대기로 둔
     * 쿠폰이 다음 틱에 적응형으로 돌아간다.
     *
     * <p>런타임은 못 박지 않고 유도한다 ({@link #offWithQueue} 와 같은 이유).
     */
    public static CouponState withQueue(QueueMode mode, long credit, long remainingStock,
            long waiting) {
        if (waiting <= 0) {
            throw new IllegalArgumentException(
                    "withQueue 는 줄이 남아 있을 때만이다. 비었으면 noQueue 를 쓴다: waiting=%d"
                            .formatted(waiting));
        }
        // 이번 틱에 다 뺄 수 있으면 배수 중, 아니면 아직 줄 서는 중이다.
        // **I3 의 경계와 같은 자리**를 쓴다 — 갈리면 이 팩토리가 생성자에
        // 막히는 조합을 만든다. 그래서 이 셈은 여기 한 곳에만 있다.
        RuntimeState runtime = credit >= waiting
                ? RuntimeState.DRAINING
                : RuntimeState.QUEUEING;
        return new CouponState(mode, runtime, credit, remainingStock, waiting, 1.0);
    }

    /** 줄이 빈 쿠폰. 배분을 못 받았으므로 credit 은 0 이다 (I1). */
    public static CouponState noQueue(QueueMode mode, long remainingStock) {
        return new CouponState(mode, RuntimeState.IDLE, 0, remainingStock, 0, 1.0);
    }

    /**
     * 운영자가 껐는데 <b>줄이 아직 남아 있다.</b> {@code mode} 와 {@code waiting}
     * 은 서로 독립이다.
     *
     * <p>런타임은 <b>못 박지 않고 유도한다.</b> 못 박으면 다 뺄 수 있는 줄까지
     * {@code QUEUEING} 이 되어 I3' 에 막힌다 (계획서 2절 3.7).
     */
    public static CouponState offWithQueue(long credit, long remainingStock, long waiting) {
        // **가드는 여기 남긴다.** 이름이 "줄이 있는 OFF" 이므로 비었을 때
        // 무엇을 쓰라고 그 자리에서 말해야 한다. 런타임 유도는 위임한다 —
        // I3 의 경계를 두 곳에 적으면 갈린다.
        if (waiting <= 0) {
            throw new IllegalArgumentException(
                    "offWithQueue 는 줄이 남아 있을 때만이다. 비었으면 off 를 쓴다: waiting=%d"
                            .formatted(waiting));
        }
        return withQueue(QueueMode.OFF, credit, remainingStock, waiting);
    }

    /** 운영자가 무조건 줄을 세우기로 했다. 한산해도 대기열을 태운다. */
    public static CouponState always(long remainingStock) {
        return new CouponState(QueueMode.ALWAYS, RuntimeState.IDLE, 0, remainingStock, 0, 1.0);
    }

    /** 운영자가 대기열을 껐다. 줄이 비어 있는 동안 줄을 세우지 않는다. */
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

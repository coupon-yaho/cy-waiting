package com.kafkick.waiting.domain.coupon;

/**
 * 배포 없이 되돌릴 수 있는 값들 (P-1).
 *
 * <p><b>한 벌로 움직입니다.</b> 필드별로 갈아 끼우면 낡은 타임아웃과 새 격벽 상한
 * 같은 조합이 한순간 존재하고, 그 조합은 아무도 검증한 적이 없습니다.
 *
 * @param idleCreditRatio 노드 몫 중 한산 통과에 쓰는 비율. 1 미만이어야 합니다
 * @param inFlightSeconds 한 건이 뒷단에 걸려 있을 수 있는 시간(초)
 */
public record Tunables(double idleCreditRatio, long inFlightSeconds) {

    /**
     * 한산 몫의 하한.
     *
     * <p><b>0 은 못 받습니다.</b> 그러면 부하가 없는 쿠폰의 요청이 전부 큐 등록으로
     * 가고, 그게 요청 경로에서 레디스를 치는 유일한 예외 경로입니다 — 값 하나로
     * 피크 전량이 그리로 들어가고, 되돌리려면 그 레디스에 써야 합니다.
     */
    public static final double MIN_IDLE_RATIO = 0.1;

    /**
     * 한산 몫의 상한.
     *
     * <p><b>1 에 가까우면 안 됩니다.</b> 한산 통과와 토큰 통과가 같은 노드 예산을
     * 쓰므로, 한산이 거의 다 긁으면 차례가 온 사람이 밀립니다. 토큰 수명이 지나면
     * 줄 맨 뒤로 다시 서고, 그건 순번 역행입니다 (불변식 3).
     */
    public static final double MAX_IDLE_RATIO = 0.9;

    /**
     * 걸림 시간의 하한.
     *
     * <p><b>서킷의 느림 임계(1.5초)보다 커야 합니다.</b> 작으면 느려진 뒷단의
     * 요청이 서킷에 집계되기 전에 격벽이 먼저 끊고, 그러면 서킷이 영영 안 열려
     * 회복 경로 자체가 사라집니다.
     */
    public static final long MIN_INFLIGHT_SECONDS = 2;

    /**
     * 걸림 시간의 상한.
     *
     * <p><b>자리를 놓게 하는 시한과 같은 값입니다.</b> 그보다 길게 잡으면 존재할
     * 수 없는 동시 건수를 상한으로 삼는 셈입니다 — 자리는 시한에서 강제로
     * 반납되므로 그 인원은 절대 안 모입니다.
     */
    public static final long MAX_INFLIGHT_SECONDS = 15;

    /**
     * <b>여기서도 막습니다.</b> {@code parse} 만 거르면 직접 만드는 경로로 NaN 이나
     * 1 이상의 비율이 들어오고, 그 값은 상한 계산을 통째로 뒤집습니다.
     */
    public Tunables {
        if (!Double.isFinite(idleCreditRatio)
                || idleCreditRatio < MIN_IDLE_RATIO || idleCreditRatio > MAX_IDLE_RATIO) {
            throw new IllegalArgumentException("idleCreditRatio 는 %s 이상 %s 이하여야 한다: %s"
                    .formatted(MIN_IDLE_RATIO, MAX_IDLE_RATIO, idleCreditRatio));
        }
        if (inFlightSeconds < MIN_INFLIGHT_SECONDS || inFlightSeconds > MAX_INFLIGHT_SECONDS) {
            throw new IllegalArgumentException("inFlightSeconds 는 %d 이상 %d 이하여야 한다: %d"
                    .formatted(MIN_INFLIGHT_SECONDS, MAX_INFLIGHT_SECONDS, inFlightSeconds));
        }
    }

    /** 값을 안 적었을 때 도는 값. 키가 없어도 게이트웨이는 돌아야 합니다. */
    public static Tunables defaults() {
        return new Tunables(0.7, 3);
    }

    /**
     * 운영자가 적은 값을 읽습니다.
     *
     * <p><b>깨져도 멈추지 않습니다.</b> 장애 중에 손으로 넣는 값이라 오타가 나는데,
     * 그때 기동이 막히면 되돌릴 수단 자체가 사라집니다. 한 값이 틀렸다고 나머지까지
     * 버리지도 않습니다 — 오타 하나가 방금 고친 다른 값도 되돌립니다.
     */
    public static Tunables parse(String json) {
        Tunables base = defaults();
        // 빈 값도 읽기로 넘긴다. 여기서 한 번 더 거르면 그 갈래를 부를 길이
        // 없어져, 읽기 쪽 방어가 도달 불가능한 채로 남는다 (TS-3).
        if (json == null) {
            return base;
        }
        TunableValues read = TunableValues.create();
        return new Tunables(
                read.ratio(json, "idleCreditRatio", base.idleCreditRatio()),
                read.seconds(json, "inFlightSeconds", base.inFlightSeconds()));
    }

    /**
     * 스냅샷에 실어 보낼 모양.
     *
     * <p><b>읽는 쪽과 같은 형식이어야 합니다.</b> 갈리면 리더가 실은 값과 노드가
     * 읽는 값이 달라지고, 그 차이는 값을 바꿔 본 뒤에야 드러납니다.
     */
    public String toJson() {
        return "{\"idleCreditRatio\":" + idleCreditRatio
                + ",\"inFlightSeconds\":" + inFlightSeconds + "}";
    }
}

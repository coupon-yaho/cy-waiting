package com.kafkick.waiting.domain.coupon;

/**
 * 배포 없이 되돌릴 수 있는 값들. <b>한 벌로 움직인다</b> — 필드별로 갈아 끼우면
 * 낡은 타임아웃과 새 격벽 상한 같은, 아무도 검증한 적 없는 조합이 한순간 존재한다.
 *
 * @param idleCreditRatio 노드 몫 중 한산 통과에 쓰는 비율. 1 미만이어야 한다
 * @param inFlightSeconds 한 건이 뒷단에 걸려 있을 수 있는 시간(초)
 */
public record Tunables(double idleCreditRatio, long inFlightSeconds) {

    /**
     * 한산 몫의 하한. <b>0 은 못 받는다</b> — 피크 전량이 큐 등록으로 가고, 그것이
     * 요청 경로에서 레디스를 치는 유일한 예외 경로다. 되돌리려면 그 레디스에
     * 써야 하니 값 하나로 스스로 못 빠져나오는 상태가 된다.
     */
    public static final double MIN_IDLE_RATIO = 0.1;

    /**
     * 한산 몫의 상한. 둘이 같은 노드 예산을 쓰므로 한산이 거의 다 긁으면 차례가 온
     * 사람이 밀린다. 밀린 사람은 토큰 수명이 지나 줄 맨 뒤로 다시 서고, 그건
     * 순번 역행이다.
     */
    public static final double MAX_IDLE_RATIO = 0.9;

    /**
     * 걸림 시간의 하한. <b>서킷의 느림 임계보다 커야 한다</b> — 작으면 느려진 뒷단이
     * 서킷에 집계되기 전에 격벽이 먼저 끊어, 서킷이 영영 안 열리고 회복 경로가 사라진다.
     */
    public static final long MIN_INFLIGHT_SECONDS = 2;

    /**
     * 걸림 시간의 상한. <b>자리를 놓게 하는 시한과 같은 값이다</b> — 그보다 길면 자리가
     * 시한에서 강제로 반납되므로, 존재할 수 없는 동시 건수를 상한으로 삼는 셈이다.
     */
    public static final long MAX_INFLIGHT_SECONDS = 15;

    /**
     * <b>여기서도 막는다.</b> {@code parse} 만 거르면 직접 만드는 경로로 NaN 이나
     * 1 이상의 비율이 들어오고, 그 값은 상한 계산을 통째로 뒤집는다.
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

    /** 값을 안 적었을 때 도는 값. 키가 없어도 게이트웨이는 돌아야 한다. */
    public static Tunables defaults() {
        return new Tunables(0.7, 3);
    }

    /**
     * 운영자가 적은 값을 읽는다. <b>깨져도 멈추지 않는다</b> — 장애 중에 손으로
     * 넣는 값이라 오타가 나는데, 그때 기동이 막히면 되돌릴 수단이 사라진다. 한 값이
     * 틀렸다고 나머지를 버리지도 않는다: 오타 하나가 방금 고친 값도 되돌린다.
     */
    public static Tunables parse(String json) {
        Tunables base = defaults();
        // 빈 값도 읽기로 넘긴다. 여기서 한 번 더 거르면 그 갈래를 부를 길이
        // 없어져, 읽기 쪽 방어가 도달 불가능한 채로 남는다.
        if (json == null) {
            return base;
        }
        TunableValues read = TunableValues.create();
        return new Tunables(
                read.ratio(json, "idleCreditRatio", base.idleCreditRatio()),
                read.seconds(json, "inFlightSeconds", base.inFlightSeconds()));
    }

    /**
     * 스냅샷에 실어 보낼 모양. <b>읽는 쪽과 같은 형식이어야 한다</b> — 갈리면 리더가
     * 실은 값과 노드가 읽는 값이 달라지고, 그 차이는 값을 바꿔 본 뒤에야 드러난다.
     */
    public String toJson() {
        return "{\"idleCreditRatio\":" + idleCreditRatio
                + ",\"inFlightSeconds\":" + inFlightSeconds + "}";
    }
}

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
     * <b>여기서도 막습니다.</b> {@code parse} 만 거르면 직접 만드는 경로로 NaN 이나
     * 1 이상의 비율이 들어오고, 그 값은 상한 계산을 통째로 뒤집습니다.
     */
    public Tunables {
        if (!Double.isFinite(idleCreditRatio) || idleCreditRatio < 0 || idleCreditRatio >= 1) {
            throw new IllegalArgumentException(
                    "idleCreditRatio 는 0 이상 1 미만이어야 한다: " + idleCreditRatio);
        }
        if (inFlightSeconds < 1) {
            throw new IllegalArgumentException(
                    "inFlightSeconds 는 1 이상이어야 한다: " + inFlightSeconds);
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

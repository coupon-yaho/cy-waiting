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
        if (json == null || json.isBlank()) {
            return base;
        }
        TunableValues read = TunableValues.create();
        return new Tunables(
                read.ratio(json, "idleCreditRatio", base.idleCreditRatio()),
                read.seconds(json, "inFlightSeconds", base.inFlightSeconds()));
    }
}

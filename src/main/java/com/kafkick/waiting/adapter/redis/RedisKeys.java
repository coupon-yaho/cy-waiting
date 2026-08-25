package com.kafkick.waiting.adapter.redis;

/**
 * 레디스 키를 한 곳에서만 만든다 (RD-3).
 *
 * <p>키가 두 곳에서 만들어지면 <b>샤딩을 도입할 때 한쪽만 고쳐진다.</b> 그때는
 * 진행 중인 큐가 통째로 유실되고, 되돌릴 방법이 없다.
 */
public final class RedisKeys {

    /** 리더가 발행하는 판정 재료. 각 노드가 주기적으로 받아 간다. */
    public static final String SNAPSHOT = "gw:snapshot";

    /** 살아 있는 게이트웨이 목록. 배분의 분모가 여기서 나온다. */
    public static final String INSTANCES = "gw:instances";

    /** 배분을 도는 노드 하나를 정하는 락. */
    public static final String LEADER = "scheduler:leader";

    /** 운영자가 배포 없이 고치는 값 (P-1). 밖에서 쓰는 키다. */
    public static final String TUNABLES = "gw:tunables";

    /** 배분 대상 쿠폰. 여기 없는 쿠폰은 스케줄러가 보지 않는다. */
    public static final String ACTIVE_COUPONS = "coupons:active";

    /** 쿠폰별 정책 JSON. 밖에서 쓰는 키라 파싱 실패를 전제한다 (E-12). */
    public static final String COUPON_POLICY = "coupon:policy";

    /**
     * 뒷단 인스턴스의 가용량 자기보고.
     *
     * <p><b>판을 키에 담는다.</b> 측정 방식을 바꿀 때 옛 보고가 섞이면 배분이
     * 두 기준을 합산한다 — 그건 어느 쪽도 아닌 값이다 (4.4.7).
     */
    public static final String CAPACITY = "capacity:coupon-svc:v1";

    /**
     * 이탈 기록의 종류 접두사.
     *
     * <p><b>writer 가 둘, reader 가 셋이 된다.</b> 접두사를 각자 문자열로 들면
     * 새로 붙는 쪽이 입장한 사람을 이탈자로 읽는다 — 그건 값 하나가 아니라
     * 사람의 상태가 뒤집히는 일이다 (RD-3 과 같은 이유로 한 곳에 둔다).
     */
    public static final String GRACE_DEPARTED = "d:";

    /** 입장 표시. {@link #GRACE_DEPARTED} 참조. */
    public static final String GRACE_ADMITTED = "a:";

    /** 해시태그를 깨뜨리는 문자. 클라이언트 입력이 키에 들어가는 경로를 막는다. */
    // '#' 은 스냅샷 해시의 전역값 접두사다. 쿠폰 ID 에 들어가면 그 쿠폰이
    // 전역값을 덮어써 — '#credit' 이름의 쿠폰 하나로 전 쿠폰의 몫이 0 이 된다.
    private static final String FORBIDDEN = "{}:#";

    private RedisKeys() {
    }

    /** 대기열 ZSET. score 는 Redis {@code TIME} 의 마이크로초다 (A-9). */
    public static String queue(String couponId, int shards, int shard) {
        return "queue:{" + tag(couponId, shards, shard) + "}";
    }

    /** 시계 역행 방어용 바닥값. ZSET 이 비어도 남아 있어야 한다. */
    public static String maxScore(String couponId, int shards, int shard) {
        return "maxscore:{" + tag(couponId, shards, shard) + "}";
    }

    /** 입장 임계. <b>개수가 아니라 score 값</b>이다 (D-8). */
    public static String admitted(String couponId, int shards, int shard) {
        return "admitted:{" + tag(couponId, shards, shard) + "}";
    }

    /**
     * 이탈자 기록. 재방문자를 식별하되 자리는 보관하지 않는다 (D-11).
     *
     * <p><b>값의 형식은 {@code <종류>:<초>} 다</b> — {@code d:} 이탈, {@code a:} 입장.
     * 있는지만 봐서는 둘을 못 가른다 ({@link #GRACE_DEPARTED}·{@link #GRACE_ADMITTED}).
     */
    public static String grace(String couponId, int shards, int shard) {
        return "grace:{" + tag(couponId, shards, shard) + "}";
    }

    /**
     * 생존 신호. 폴링이 곧 하트비트다.
     *
     * <p><b>사람마다 키를 만들지 않는다.</b> 그러면 청소 스크립트가 KEYS 에
     * 선언되지 않은 키를 만지게 되고 클러스터가 거부한다 (RD-1). 쿠폰당 ZSET
     * 하나에 <b>만료 시각을 score 로</b> 담는다 — 개별 TTL 은 잃지만 청소가
     * 어차피 만료를 보므로 잃는 것이 없다.
     */
    public static String alive(String couponId, int shards, int shard) {
        return "alive:{" + tag(couponId, shards, shard) + "}";
    }

    /**
     * 남은 재고. <b>발급 계층이 소유하고 샤드와 무관하다.</b>
     *
     * <p>샤딩하면 슬롯이 갈리므로 <b>Lua 에서 만지지 않는다</b> — 별도로 읽는다.
     */
    public static String stock(String couponId) {
        return "stock:{" + validated(couponId, "couponId") + "}";
    }

    /** 키에서 해시태그를 꺼낸다. 같은 슬롯에 모이는지 확인하는 데 쓴다. */
    public static String hashTagOf(String key) {
        int open = key.indexOf('{');
        int close = key.indexOf('}', open + 1);
        if (open < 0 || close < 0) {
            throw new IllegalArgumentException("해시태그가 없는 키다: " + key);
        }
        return key.substring(open + 1, close);
    }

    /**
     * 해시태그 본문.
     *
     * <p><b>샤드가 하나면 접미사를 붙이지 않는다.</b> 붙였다 떼는 순간 콜드 쿠폰
     * 전체의 키가 갈리므로, 운영 중 샤딩 도입이 불가능해진다 (3.1절).
     */
    // RULE-EXCEPTION(JS-13): JS-14 가 RedisKeys 를 유틸리티 클래스로 명시한다.
    // 인스턴스가 없어 인스턴스 메서드로 둘 수 없다 (AIJ-0014).
    private static String tag(String couponId, int shards, int shard) {
        String id = validated(couponId, "couponId");
        if (shards < 1) {
            throw new IllegalArgumentException("shards 는 1 이상이어야 한다: " + shards);
        }
        if (shard < 0 || shard >= shards) {
            // 범위를 넘으면 아무도 안 보는 키가 생기고 그 큐는 영영 안 빠진다.
            throw new IllegalArgumentException(
                    "shard 는 [0, %d) 안이어야 한다: %d".formatted(shards, shard));
        }
        return shards == 1 ? id : id + ":" + shard;
    }

    /** 클라이언트 입력이 키 이름에 들어가는 경로는 전부 의심한다 (PK-R5). */
    // RULE-EXCEPTION(JS-13): JS-14 가 RedisKeys 를 유틸리티 클래스로 명시한다.
    // 인스턴스가 없어 인스턴스 메서드로 둘 수 없다 (AIJ-0014).
    private static String validated(String value, String what) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(what + " 는 필수다");
        }
        for (int i = 0; i < FORBIDDEN.length(); i++) {
            if (value.indexOf(FORBIDDEN.charAt(i)) >= 0) {
                throw new IllegalArgumentException(
                        "%s 에 '%c' 가 들어갈 수 없다 — 슬롯이 갈린다: %s"
                                .formatted(what, FORBIDDEN.charAt(i), value));
            }
        }
        return value;
    }
}

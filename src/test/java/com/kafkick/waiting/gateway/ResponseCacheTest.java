package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.MutableClock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 아주 짧게 들고 있는 조회 응답.
 *
 * <p><b>코얼레싱만으로는 부족합니다.</b> 그건 동시에 도착한 것만 모으고, 1ms 어긋난
 * 요청은 각각 나갑니다. 짧은 수명을 얹어야 연속 도착까지 흡수됩니다.
 */
class ResponseCacheTest {

    private static final Instant 지금 = Instant.parse("2026-08-27T00:00:00Z");

    private final MutableClock 시계 = MutableClock.at(지금);

    /** 항목 하나의 무게. 본문 두 바이트에 헤더 몫이 붙는다. */
    private static final long 한_항목 = 2 + 512;

    /** 딱 셋만 들어가는 예산. 넷째가 못 들어가는 것을 값으로 잰다. */
    private static final long 예산 = 3 * 한_항목;

    private final ResponseCache cache = ResponseCache.ofBytes(시계, 예산);

    private static ResponseCache.Entry 응답(String body) {
        return new ResponseCache.Entry(200,
                Map.of("Content-Type", List.of("application/json")), body.getBytes());
    }

    @Test
    @DisplayName("수명_안에는_같은_것을_돌려준다")
    void 수명_안에는_같은_것을_돌려준다() {
        cache.put("k", 응답("첫 응답"), Duration.ofMillis(200));
        시계.앞으로(Duration.ofMillis(199));

        assertThat(cache.get("k")).isPresent()
                .get()
                .extracting(e -> new String(e.body()))
                .isEqualTo("첫 응답");
    }

    /**
     * <b>수명이 지나면 없는 것입니다.</b> 지난 값을 돌려주면 재고가 0 이 된 뒤에도
     * 남아 있다고 답하고, 그 사람은 매진된 쿠폰을 받으러 갑니다.
     */
    @Test
    @DisplayName("수명이_지나면_안_돌려준다")
    void 수명이_지나면_안_돌려준다() {
        cache.put("k", 응답("첫 응답"), Duration.ofMillis(200));
        시계.앞으로(Duration.ofMillis(200));

        assertThat(cache.get("k")).isEmpty();
    }

    /**
     * <b>키는 밖에서 오는 값입니다.</b> 경로와 쿼리로 만드므로 가짓수에 상한이
     * 없습니다 — 안 막으면 맵 하나가 메모리를 밀어냅니다.
     */
    @Test
    @DisplayName("상한을_넘으면_새_키를_안_받는다")
    void 상한을_넘으면_새_키를_안_받는다() {
        for (int i = 0; i < 3; i++) {
            cache.put("k" + i, 응답("v" + i), Duration.ofMillis(200));
        }
        assertThat(cache.isFull()).as("예산이 찼는지 먼저 잰다").isTrue();

        cache.put("넘친다", 응답("v"), Duration.ofMillis(200));

        assertThat(cache.get("넘친다")).as("상한 밖의 키").isEmpty();
        assertThat(cache.get("k0")).as("이미 담긴 키는 그대로다").isPresent();
    }

    /**
     * <b>같은 키를 덮어쓰는 것은 새 키가 아닙니다.</b> 상한으로 막으면 가장 자주
     * 쓰는 키가 영영 갱신 안 되고, 그때 캐시는 낡은 값을 계속 냅니다.
     */
    @Test
    @DisplayName("상한이_차도_이미_담긴_키는_갱신된다")
    void 상한이_차도_이미_담긴_키는_갱신된다() {
        for (int i = 0; i < 3; i++) {
            cache.put("k" + i, 응답("옛 값"), Duration.ofMillis(200));
        }

        cache.put("k0", 응답("새 값"), Duration.ofMillis(200));

        assertThat(cache.get("k0")).get()
                .extracting(e -> new String(e.body()))
                .isEqualTo("새 값");
    }

    /**
     * <b>지난 것이 자리를 차지하면 안 됩니다.</b> 안 지우면 수명이 다 된 키들이
     * 상한을 채우고, 그때부터 새 키가 하나도 안 들어갑니다.
     */
    @Test
    @DisplayName("지난_것은_자리를_비운다")
    void 지난_것은_자리를_비운다() {
        for (int i = 0; i < 3; i++) {
            cache.put("k" + i, 응답("v"), Duration.ofMillis(200));
        }
        시계.앞으로(Duration.ofMillis(200));

        cache.put("새것", 응답("v"), Duration.ofMillis(200));

        assertThat(cache.get("새것")).isPresent();
        // 지난 셋이 실제로 빠졌는지까지 본다. 안 빠졌으면 다음 것이 못 들어간다.
        assertThat(cache.size()).as("담고 있는 키 수").isEqualTo(1);
    }

    /**
     * <b>키 수로만 막으면 유계가 아닙니다.</b> 키 1만 × 본문 256KiB 는 2.4GiB 라,
     * 메모리를 지키겠다고 만든 상한이 그대로 OOM 의 근거가 됩니다.
     */
    @Test
    @DisplayName("예산보다_큰_응답은_아예_안_담는다")
    void 예산보다_큰_응답은_아예_안_담는다() {
        cache.put("큰것", 응답("가".repeat((int) 예산)), Duration.ofMillis(200));

        assertThat(cache.get("큰것")).isEmpty();
        assertThat(cache.bytes()).as("들고 있는 바이트").isZero();
    }

    /**
     * <b>준 쪽이 나중에 고치면 안 됩니다.</b> 그러면 그다음 요청들이 바뀐 것을
     * 받습니다 — 담아 둔 응답이 조용히 다른 것이 됩니다.
     */
    @Test
    @DisplayName("담은_뒤_원본을_고쳐도_안_바뀐다")
    void 담은_뒤_원본을_고쳐도_안_바뀐다() {
        byte[] 원본 = "첫 응답".getBytes();
        cache.put("k", new ResponseCache.Entry(200,
                Map.of("X", List.of("하나")), 원본), Duration.ofMillis(200));

        원본[0] = 0;
        byte[] 받은_것 = cache.get("k").orElseThrow().body();
        받은_것[0] = 0;

        assertThat(cache.get("k").orElseThrow().body())
                .isEqualTo("첫 응답".getBytes());
    }

    @Test
    @DisplayName("없는_키는_비어_있다")
    void 없는_키는_비어_있다() {
        assertThat(cache.get("없다")).isEmpty();
    }

    @Test
    @DisplayName("상한이_1_미만이면_만들지_못한다")
    void 상한이_1_미만이면_만들지_못한다() {
        assertThatThrownBy(() -> ResponseCache.ofBytes(시계, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 0 이하 수명은 곧바로 지난 것이라, 담는 순간 없는 것과 같아야 한다. */
    @Test
    @DisplayName("수명이_0_이하면_안_담는다")
    void 수명이_0_이하면_안_담는다() {
        cache.put("k", 응답("v"), Duration.ZERO);

        assertThat(cache.get("k")).isEmpty();
    }
}

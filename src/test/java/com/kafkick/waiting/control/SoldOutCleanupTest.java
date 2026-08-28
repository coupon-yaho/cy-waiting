package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.domain.coupon.CouponState;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import com.kafkick.waiting.domain.coupon.CouponStates;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 매진된 쿠폰의 큐를 <b>유예 뒤에</b> 지운다 (7.3).
 *
 * <p>정리 판단만 합니다. 실제로 지우는 것은 어댑터이고, 여기는 <b>어느 쿠폰을
 * 지금 지워도 되는가</b>만 답합니다 — 그래야 레디스 없이 판단을 잽니다.
 */
class SoldOutCleanupTest {

    private static final String COUPON = "c1";
    private static final int 유예_틱 = 3;

    private final MeterRegistry meters = new SimpleMeterRegistry();

    private SoldOutCleanup 정리() {
        return SoldOutCleanup.of(유예_틱, meters);
    }

    /** <b>줄이 빈 매진 쿠폰.</b> 대기자가 남았으면 안 지운다 (7.3.3). */
    private Map<String, CouponState> 매진() {
        return Map.of(COUPON, CouponStates.closed(0));
    }

    private Map<String, CouponState> 매진인데_줄이_남음() {
        return Map.of(COUPON, CouponStates.closed(100));
    }

    private Map<String, CouponState> 재고있음() {
        return Map.of(COUPON, CouponStates.queueing(10, 1_000, 100));
    }

    /**
     * <b>바로 안 지웁니다.</b> 마지막 폴링이 아직 오는 중입니다 — 그 사람이
     * <code>NOT_QUEUED</code> 를 보면 매진이 아니라 "줄에 없다" 로 읽힙니다.
     */
    @Test
    @DisplayName("처음_매진을_본_틱에는_안_지운다")
    void 처음_매진을_본_틱에는_안_지운다() {
        assertThat(정리().due(매진())).isEmpty();
    }

    /** 유예가 다 차야 지웁니다. 경계에서 아직 살아 있고 그 다음에 지웁니다. */
    @Test
    @DisplayName("유예_틱을_다_채우면_지운다")
    void 유예_틱을_다_채우면_지운다() {
        SoldOutCleanup 정리 = 정리();

        for (int i = 0; i < 유예_틱; i++) {
            assertThat(정리.due(매진())).as("%d 번째 틱".formatted(i + 1)).isEmpty();
        }

        assertThat(정리.due(매진())).containsExactly(COUPON);
    }

    /**
     * <b>한 번 지운 것을 또 지우지 않습니다.</b> 매 틱 같은 명령을 다시 내면
     * 틱당 명령 수가 쿠폰 수만큼 늘고, 그게 곧 배분이 밀리는 이유가 됩니다.
     */
    @Test
    @DisplayName("지운_것이_확인되면_다시_안_지운다")
    void 지운_것이_확인되면_다시_안_지운다() {
        SoldOutCleanup 정리 = 정리();
        for (int i = 0; i <= 유예_틱; i++) {
            정리.due(매진());
        }

        정리.dropped(List.of(COUPON));

        assertThat(정리.due(매진())).isEmpty();
    }

    /**
     * <b>못 지웠으면 다음 틱에 다시 옵니다</b> (7.3.4).
     *
     * <p>시도 전에 표시하면 부분 실패가 "다시 온다" 가 아니라 영구 누수가
     * 됩니다 — 활성 목록이 한 방향으로만 자랍니다.
     */
    @Test
    @DisplayName("못_지웠으면_다음_틱에_다시_온다")
    void 못_지웠으면_다음_틱에_다시_온다() {
        SoldOutCleanup 정리 = 정리();
        for (int i = 0; i <= 유예_틱; i++) {
            정리.due(매진());
        }

        정리.failed(List.of(COUPON));

        assertThat(정리.due(매진())).containsExactly(COUPON);
    }

    /**
     * <b>줄이 남았을 때가 지울 때입니다.</b>
     *
     * <p>"줄이 빈 뒤에 지운다" 로 쓰면 영영 안 돕니다 — 매진 쿠폰은 크레딧이
     * 0 이라 아무도 입장으로 안 빠지고, 폴링은 게이트웨이가 종결하므로 큐에서
     * 빼는 스크립트가 안 돌고, 스위퍼는 매진 중 멈춥니다. <code>waiting</code>
     * 을 줄이는 주체가 하나도 없습니다.
     */
    @Test
    @DisplayName("줄이_남아_있어도_유예_뒤에_지운다")
    void 줄이_남아_있어도_유예_뒤에_지운다() {
        SoldOutCleanup 정리 = 정리();

        for (int i = 0; i < 유예_틱; i++) {
            assertThat(정리.due(매진인데_줄이_남음())).as("%d 번째 틱".formatted(i + 1)).isEmpty();
        }

        assertThat(정리.due(매진인데_줄이_남음())).containsExactly(COUPON);
    }

    /**
     * <b>리더가 되면 셈을 처음부터 줍니다.</b>
     *
     * <p>비리더 구간에 얼어 있던 셈을 이어 쓰면 유예가 설정값이 아니라 "내가
     * 리더였던 틱 수" 가 됩니다 — 그 둘은 장애 중에 완전히 갈립니다.
     */
    @Test
    @DisplayName("리더가_되면_셈을_처음부터_준다")
    void 리더가_되면_셈을_처음부터_준다() {
        SoldOutCleanup 정리 = 정리();
        for (int i = 0; i < 유예_틱; i++) {
            정리.due(매진());
        }

        정리.leadershipAcquired();

        for (int i = 0; i < 유예_틱; i++) {
            assertThat(정리.due(매진())).as("%d 번째 틱".formatted(i + 1)).isEmpty();
        }
        assertThat(정리.due(매진())).containsExactly(COUPON);
    }

    /**
     * <b>취소가 0 이면 안전 장치가 죽어 있다는 뜻입니다.</b> 쿠폰 ID 를 라벨로
     * 못 쓰므로(LG-4) 그것을 알 방법이 이 계수뿐입니다.
     */
    @Test
    @DisplayName("취소와_삭제를_따로_센다")
    void 취소와_삭제를_따로_센다() {
        SoldOutCleanup 정리 = 정리();
        정리.due(매진());

        정리.due(재고있음());

        assertThat(meters.get("waiting.soldout.cleanup").tag("outcome", "cancelled")
                .counter().count()).as("취소").isEqualTo(1);
        assertThat(meters.find("waiting.soldout.cleanup").tag("outcome", "dropped")
                .counter().count()).as("삭제").isZero();
    }

    /**
     * <b>유예 중 재고가 돌아오면 삭제를 취소합니다</b> (7.3.2b).
     *
     * <p>지워 버리면 줄 선 사람이 순번을 잃고, 다시 서면 그동안 온 사람들 뒤로
     * 갑니다 — 순번 역행입니다.
     */
    @Test
    @DisplayName("유예_중_재고가_돌아오면_취소한다")
    void 유예_중_재고가_돌아오면_취소한다() {
        SoldOutCleanup 정리 = 정리();
        정리.due(매진());
        정리.due(매진());

        정리.due(재고있음());

        // 세었던 것이 지워졌으므로 다시 처음부터 채워야 한다.
        for (int i = 0; i < 유예_틱; i++) {
            assertThat(정리.due(매진())).as("%d 번째 틱".formatted(i + 1)).isEmpty();
        }
        assertThat(정리.due(매진())).containsExactly(COUPON);
    }

    /** 재고가 있는 쿠폰은 셈 자체를 시작하지 않습니다. */
    @Test
    @DisplayName("재고가_있으면_세지_않는다")
    void 재고가_있으면_세지_않는다() {
        SoldOutCleanup 정리 = 정리();

        for (int i = 0; i < 유예_틱 * 2; i++) {
            assertThat(정리.due(재고있음())).isEmpty();
        }
    }

    /**
     * <b>스냅샷에서 사라진 쿠폰의 셈은 버립니다.</b> 안 버리면 활성 목록을
     * 드나드는 쿠폰이 옛 셈을 이어받아, 유예를 다 안 채우고 지워집니다.
     */
    @Test
    @DisplayName("재료에서_사라진_쿠폰의_셈은_버린다")
    void 재료에서_사라진_쿠폰의_셈은_버린다() {
        SoldOutCleanup 정리 = 정리();
        정리.due(매진());
        정리.due(매진());

        정리.due(Map.of());

        for (int i = 0; i < 유예_틱; i++) {
            assertThat(정리.due(매진())).as("%d 번째 틱".formatted(i + 1)).isEmpty();
        }
        assertThat(정리.due(매진())).containsExactly(COUPON);
    }

    /** 쿠폰마다 따로 셉니다. 한 쿠폰의 셈이 다른 쿠폰을 지우면 안 됩니다. */
    @Test
    @DisplayName("쿠폰마다_따로_센다")
    void 쿠폰마다_따로_센다() {
        SoldOutCleanup 정리 = 정리();
        for (int i = 0; i < 유예_틱; i++) {
            정리.due(매진());
        }

        List<String> 지울_것 = 정리.due(Map.of(
                COUPON, CouponStates.closed(0), "c2", CouponStates.closed(0)));

        assertThat(지울_것).containsExactly(COUPON);
    }

    /** 유예가 0 이면 유예가 아닙니다. 값으로 끄면 그 사실이 설정에 안 드러납니다. */
    @Test
    @DisplayName("유예가_없는_정리는_만들_수_없다")
    void 유예가_없는_정리는_만들_수_없다() {
        assertThatThrownBy(() -> SoldOutCleanup.of(0, meters))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("유예");
    }
}

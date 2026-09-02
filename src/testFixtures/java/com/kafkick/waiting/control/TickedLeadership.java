package com.kafkick.waiting.control;

import java.time.Duration;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;

/**
 * 시각을 주입한 리더십. <b>시험이 실시간을 안 태우게 한다</b> (TS-4).
 *
 * <p>리스 경계는 밀리초 단위라 실시계로 재면 여유가 백 밀리초대다 — 클래스 로딩
 * 한 번에 그 여유가 사라져 시험이 <b>불변식 위반 문구로</b> 빨개진다. 그 빨강은
 * 다음 사람이 제품을 의심하게 만든다.
 */
// 팩토리가 패키지 프라이빗이라 같은 패키지에 얇은 창을 낸다. 프로덕션에 시험용
// 구멍을 내지 않으려는 것이 원래 의도이고, 픽스처는 그 의도를 안 깬다.
public final class TickedLeadership {

    private TickedLeadership() {
    }

    /** @param ticker 나노초를 돌려준다. 시험이 원하는 만큼만 앞으로 감는다 */
    public static Leadership of(String ownerId, Duration lease, Duration attemptTimeout,
            Supplier<Mono<LeaderLock>> acquire, Supplier<Mono<Void>> release,
            LongSupplier ticker) {
        return Leadership.of(ownerId, lease, attemptTimeout, acquire, release, ticker);
    }
}

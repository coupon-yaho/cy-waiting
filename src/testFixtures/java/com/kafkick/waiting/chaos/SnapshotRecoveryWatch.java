package com.kafkick.waiting.chaos;

import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.control.SnapshotHolder;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;

/**
 * 레디스가 끊겼다 돌아올 때 판정 재료를 지켜본다. <b>시나리오마다 따로 쓰지 않는다</b> — 같은 관측을 두 벌
 * 두면 한쪽만 고쳐 "첫 스냅샷 5초" 가 시나리오마다 다른 것을 재게 된다.
 */
public final class SnapshotRecoveryWatch {

    private static final Duration POLL = Duration.ofMillis(50);

    private final SnapshotHolder holder;

    private SnapshotRecoveryWatch(SnapshotHolder holder) {
        this.holder = Objects.requireNonNull(holder, "holder 는 필수다");
    }

    /** <b>시계를 안 받는다</b> — 홀더와 다른 시계로 받아온 시각을 되짚으면 둘의 차이만큼 판정이 어긋난다. */
    public static SnapshotRecoveryWatch of(SnapshotHolder holder) {
        return new SnapshotRecoveryWatch(holder);
    }

    /** 회복을 재기 시작한 순간. 단조 시각과 그때 들고 있던 스냅샷을 함께 든다. */
    public record Mark(long nanos, GatewaySnapshot held) {
    }

    /** 지금을 표시한다. 장애를 걷은 직후에 부른다. */
    public Mark 표시한다() {
        return new Mark(System.nanoTime(), holder.current());
    }

    /** 받아오기가 멎었다고 볼 만큼 나이가 찰 때까지 기다린다. 멎은 뒤의 나이를 돌려준다. */
    public Duration 받아오기가_멎을_때까지(Duration 멎은_나이, Duration 한계) {
        Awaitility.await().atMost(한계).until(() -> holder.fetchAge().compareTo(멎은_나이) > 0);
        return holder.fetchAge();
    }

    /** 받아오기가 멎은 채 {@code 동안} 을 버틴다. 중간에 다시 받으면 던진다. */
    public void 멎은_채로_둔다(Duration 동안, Duration 멎은_나이) {
        Awaitility.await().during(동안).atMost(동안.plusSeconds(2))
                .until(() -> holder.fetchAge().compareTo(멎은_나이) > 0);
    }

    /** {@code 동안} 루프가 돈 뒤 나이를 촘촘히 보고 가장 긴 값을 돌려준다. */
    public Duration 가장_긴_틱_나이(Duration 동안) {
        Duration[] 가장_긴 = {Duration.ZERO};
        // 조건이 늘 참이라 during 동안 표본만 모은다.
        Awaitility.await().during(동안).atMost(동안.plusSeconds(2)).pollInterval(POLL)
                .until(() -> {
                    Duration 나이 = holder.tickAge();
                    if (나이.compareTo(가장_긴[0]) > 0) {
                        가장_긴[0] = 나이;
                    }
                    return true;
                });
        return 가장_긴[0];
    }

    /** 지금 들고 있는 발행 시각. 회복 뒤 새 발행인지 가르는 기준이다. */
    public Instant 들고_있는_발행() {
        return holder.view().snapshot().publishedAt();
    }

    /**
     * 표시 뒤로 스냅샷을 다시 받기까지. 한계 안에 못 받으면 null. <b>받은 것을 객체가 바뀐 것으로 가른다</b> —
     * 성공한 받아오기만 새로 풀어 갈아 끼우고, 실패한 회차는 들고 있던 것을 그대로 둔다.
     */
    public Duration 다시_받기까지(Mark 표시, Duration 한계) {
        try {
            Awaitility.await().pollInterval(POLL).atMost(한계)
                    .until(() -> holder.current() != 표시.held() && holder.view().snapshot().isPublished());
            return 지난_시간(표시);
        } catch (ConditionTimeoutException e) {
            return null;
        }
    }

    /**
     * 표시 뒤로 <b>새 발행</b>을 받아 낡음이 풀리기까지. 한계 안에 안 풀리면 null. 옛 해시를 다시 받는 것만 보면
     * 리더가 한 번도 발행 못 해도 참이라 따로 잰다.
     */
    public Duration 새_발행으로_낡음이_풀리기까지(Mark 표시, Instant 앞_발행, Duration 한계) {
        try {
            Awaitility.await().pollInterval(POLL).atMost(한계)
                    .until(() -> 들고_있는_발행().isAfter(앞_발행) && !holder.isDataStale());
            return 지난_시간(표시);
        } catch (ConditionTimeoutException e) {
            return null;
        }
    }

    private static Duration 지난_시간(Mark 표시) {
        return Duration.ofNanos(System.nanoTime() - 표시.nanos());
    }
}

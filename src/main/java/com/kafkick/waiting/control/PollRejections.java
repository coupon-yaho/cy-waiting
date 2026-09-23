package com.kafkick.waiting.control;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 이 노드가 조회를 상한으로 거절했다는 표시. 거절당한 사람은 생존 신호를 못 갱신하므로 클러스터가 그동안 청소를
 * 멈춰야 한다. <b>하트비트가 실어 보낸 만큼만 내리고</b>, 마지막 거절 뒤에도 성공한 하트비트 몇 번 동안 더 싣는다.
 */
public final class PollRejections {

    private final AtomicLong count = new AtomicLong();

    /** 마지막 거절을 실은 뒤 더 실을 성공 횟수. 한 번만 실으면 리더의 하트비트가 그 틈을 비켜 갈 수 있다. */
    private final AtomicInteger holding = new AtomicInteger();

    private final int holdBeats;

    private PollRejections(int holdBeats) {
        if (holdBeats < 0) {
            throw new IllegalArgumentException("holdBeats 는 0 이상이어야 한다: " + holdBeats);
        }
        this.holdBeats = holdBeats;
    }

    /** 마지막 거절 뒤로 더 싣지 않는다. 시험과 조각 배선용이다. */
    public static PollRejections create() {
        return create(0);
    }

    /** @param holdBeats 마지막 거절을 실은 뒤 더 실을 성공한 하트비트 수 */
    public static PollRejections create(int holdBeats) {
        return new PollRejections(holdBeats);
    }

    /** 요청 경로에서 부른다. 레디스를 안 친다. */
    public void rejected() {
        count.incrementAndGet();
    }

    /** 하트비트가 실을 값. 0 이면 지난 하트비트 뒤로 거절이 없었다. */
    public long mark() {
        return count.get();
    }

    /** 이번 하트비트가 거절 중이라고 실을지. */
    public boolean sending(long mark) {
        return mark > 0 || holding.get() > 0;
    }

    /** 그 값을 실은 하트비트가 성공했다. 그 뒤에 난 거절은 남긴다. */
    public void settled(long mark) {
        if (mark > 0) {
            if (count.compareAndSet(mark, 0)) {
                holding.set(holdBeats);
            }
            return;
        }
        holding.updateAndGet(left -> Math.max(0, left - 1));
    }
}

package com.kafkick.waiting.control;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 이 노드가 조회를 상한으로 거절했다는 표시. 거절당한 사람은 생존 신호를 못 갱신하므로 클러스터가 그동안 청소를
 * 멈춰야 한다. <b>하트비트가 실어 보낸 만큼만 내린다</b> — 싣는 사이 새로 난 거절까지 지우면 클러스터가 모른다.
 */
public final class PollRejections {

    private final AtomicLong count = new AtomicLong();

    private PollRejections() {
    }

    public static PollRejections create() {
        return new PollRejections();
    }

    /** 요청 경로에서 부른다. 레디스를 안 친다. */
    public void rejected() {
        count.incrementAndGet();
    }

    /** 하트비트가 실을 값. 0 이면 지난 하트비트 뒤로 거절이 없었다. */
    public long mark() {
        return count.get();
    }

    /** 그 값을 실은 하트비트가 성공했다. 그 뒤에 난 거절은 남긴다. */
    public void settled(long mark) {
        count.compareAndSet(mark, 0);
    }
}

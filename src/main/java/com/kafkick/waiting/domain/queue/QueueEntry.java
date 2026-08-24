package com.kafkick.waiting.domain.queue;

import java.util.Objects;

/**
 * 줄에서의 자리.
 *
 * <p>{@code score} 는 순번이고 {@code rank} 는 내 앞의 인원이다. 둘은 다르다 —
 * 순번은 벽시계라 안 변하고, 앞의 인원은 앞사람이 빠지면 줄어든다.
 *
 * @param rank 내 앞의 인원. 줄에 없으면 {@code -1}
 * @param score 이 사람의 순번(마이크로초). 줄에 없으면 {@code -1}
 * @param alreadyQueued 이미 서 있던 사람인가. 새로고침 연타를 가른다
 * @param clockWentBack 바닥값이 적용됐는가. 참이면 시계가 뒤로 갔다는 뜻이다
 */
public record QueueEntry(QueueState state, long rank, long score,
        boolean alreadyQueued, boolean clockWentBack) {

    public QueueEntry {
        Objects.requireNonNull(state, "state 는 필수다");
    }

    /** 줄에 자리가 있는가. 거절은 상한에 걸린 것뿐이다. */
    public boolean accepted() {
        return state != QueueState.REJECTED;
    }

    public boolean admitted() {
        return state == QueueState.ADMITTED;
    }
}

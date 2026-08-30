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
 * @param rejoined <b>등록 결과에만 있는 사실이다.</b> 조회는 항상 거짓을 싣는다 —
 *                 자리를 비웠다는 것은 다시 설 때 한 번만 알려 줄 수 있다
 */
public record QueueEntry(QueueState state, long rank, long score,
        boolean alreadyQueued, boolean clockWentBack, boolean rejoined) {

    /** 줄에 없다는 뜻. 0번째와 구분하려면 음수여야 한다. */
    public static final long NONE = -1;

    public QueueEntry {
        Objects.requireNonNull(state, "state 는 필수다");
        // **상태마다 가질 수 있는 값이 다르다.** 아무 조합이나 만들어지면 그것을
        // 전제로 통과하는 시험이 생기고, 운영이 못 만드는 상태를 재게 된다.
        boolean ok = switch (state) {
            // 줄에 없다. 자리를 들고 있으면 안 된다.
            case NOT_QUEUED, REJECTED -> rank == NONE && score == NONE;
            // 줄에 있다. 앞의 인원도 순번도 있다.
            case WAITING -> rank >= 0 && score >= 0;
            // 차례가 왔다. 큐에서 빠졌으므로 앞에 아무도 없고, 유예 기록으로
            // 되읽은 경우에는 순번을 모른다.
            case ADMITTED -> rank == 0 && (score >= 0 || score == NONE);
        };
        if (!ok) {
            throw new IllegalArgumentException(
                    "%s 가 가질 수 없는 값이다: rank=%d score=%d".formatted(state, rank, score));
        }
        // **재방문은 새로 선 사람에게만 있다.** 이미 줄에 있던 사람은 스크립트가
        // 먼저 돌아가 기록을 안 보고, 조회는 그 사실을 아예 안 싣는다.
        //
        // **던지지 않고 낮춘다.** 등록 결과를 만들다 던지면 부르는 쪽이 그것을
        // 삼켜 fail-open 으로 흘리고, 그러면 줄에 5만 명이 서 있어도 신규가
        // 뒷단 직행이 된다 — 보고용 값 하나 때문에 줄이 통째로 열린다.
        rejoined = rejoined && state == QueueState.WAITING && !alreadyQueued;
    }

    /** 줄에 없다. 아직 안 섰거나 이탈로 지워졌다. */
    public static QueueEntry notQueued() {
        return new QueueEntry(QueueState.NOT_QUEUED, NONE, NONE, false, false, false);
    }

    /** 줄이 꽉 차 못 섰다. */
    public static QueueEntry rejected() {
        return new QueueEntry(QueueState.REJECTED, NONE, NONE, false, false, false);
    }

    /** 줄에 자리가 있는가. 거절은 상한에 걸린 것뿐이다. */
    public boolean accepted() {
        return state != QueueState.REJECTED;
    }

    public boolean admitted() {
        return state == QueueState.ADMITTED;
    }
}

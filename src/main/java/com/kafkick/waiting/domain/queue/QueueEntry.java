package com.kafkick.waiting.domain.queue;

import java.util.Objects;

/**
 * 줄에서의 자리. 순번은 벽시계라 안 변하고, 앞의 인원은 앞사람이 빠지면 줄어든다.
 *
 * @param rank 내 앞의 인원. 줄에 없으면 {@code -1}
 * @param score 이 사람의 순번(마이크로초). 줄에 없으면 {@code -1}
 * @param alreadyQueued 이미 서 있던 사람인가. 새로고침 연타를 가른다
 * @param clockWentBack 바닥값이 적용됐는가. 참이면 시계가 뒤로 갔다는 뜻이다
 * @param rejoined <b>등록 결과에만 있는 사실이다.</b> 조회는 항상 거짓을 싣는다 —
 *                 자리를 비웠다는 것은 다시 설 때 한 번만 알려 줄 수 있다
 */
public record QueueEntry(QueueState state, long rank, long score,
        boolean alreadyQueued, boolean clockWentBack, boolean rejoined, long total) {

    /** 총원을 모르는 자리. 등록 결과와 유예로 되읽은 입장이 여기다. */
    public static final long UNKNOWN_TOTAL = -1;

    /** 줄에 없다는 뜻. 0번째와 구분하려면 음수여야 한다. */
    public static final long NONE = -1;

    public QueueEntry {
        Objects.requireNonNull(state, "state 는 필수다");
        // **상태마다 가질 수 있는 값이 다르다.** 아무 조합이나 만들어지면 그것을
        // 전제로 통과하는 시험이 생기고, 운영이 못 만드는 상태를 재게 된다.
        boolean ok = switch (state) {
            // 줄에 없다. 자리를 들고 있으면 안 된다.
            case NOT_QUEUED, REJECTED -> rank == NONE && score == NONE
                    && total == UNKNOWN_TOTAL;
            // **총원은 나를 포함해 센다.** 그래서 앞 인원보다 반드시 크다 — 아니면 두 값을
            // 다른 기준으로 읽은 것이고, 그대로 두면 "앞에 100명인데 총 80명" 이 나간다.
            case WAITING -> rank >= 0 && score >= 0
                    && (total == UNKNOWN_TOTAL || total > rank);
            // 차례가 왔다. 큐에서 빠졌으므로 앞에 아무도 없고, 유예 기록으로
            // 되읽은 경우에는 순번을 모른다. 줄을 벗어났으니 총원도 안 든다.
            case ADMITTED -> rank == 0 && (score >= 0 || score == NONE)
                    && total == UNKNOWN_TOTAL;
        };
        if (!ok) {
            throw new IllegalArgumentException(
                    "%s 가 가질 수 없는 값이다: rank=%d score=%d total=%d"
                            .formatted(state, rank, score, total));
        }
        // **재방문은 새로 선 사람에게만 있다.** 던지지 않고 낮추는 것은, 등록 결과를
        // 만들다 던지면 부르는 쪽이 삼켜 fail-open 으로 흘리기 때문이다 — 보고용 값
        // 하나 때문에 줄에 5만 명이 서 있어도 신규가 뒷단 직행이 된다.
        rejoined = rejoined && state == QueueState.WAITING && !alreadyQueued;
    }

    /** 총원을 안 싣는 자리. 등록 결과는 왕복을 늘리지 않으려고 세지 않는다. */
    public QueueEntry(QueueState state, long rank, long score,
            boolean alreadyQueued, boolean clockWentBack, boolean rejoined) {
        this(state, rank, score, alreadyQueued, clockWentBack, rejoined, UNKNOWN_TOTAL);
    }

    /** 줄에 없다. 아직 안 섰거나 이탈로 지워졌다. */
    public static QueueEntry notQueued() {
        return new QueueEntry(QueueState.NOT_QUEUED, NONE, NONE, false, false, false);
    }

    /** 줄이 꽉 차 못 섰다. */
    public static QueueEntry rejected() {
        return new QueueEntry(QueueState.REJECTED, NONE, NONE, false, false, false);
    }

    /**
     * 내 뒤에 선 사람 수. <b>총원과 앞 인원을 같은 기준으로 센 값에서만 뽑는다</b> — 따로 읽으면
     * "앞에 100명인데 총 80명" 이 나온다 (CY-827). 모르면 {@link #UNKNOWN_TOTAL}.
     *
     * <p>상태는 안 본다 — 총원을 드는 것은 {@code WAITING} 뿐이라고 생성자가 이미 막았다.
     */
    public long behind() {
        if (total == UNKNOWN_TOTAL) {
            return UNKNOWN_TOTAL;
        }
        return total - rank - 1;
    }

    /** 줄에 자리가 있는가. 거절은 상한에 걸린 것뿐이다. */
    public boolean accepted() {
        return state != QueueState.REJECTED;
    }

    public boolean admitted() {
        return state == QueueState.ADMITTED;
    }
}

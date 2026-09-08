package com.kafkick.waiting.gateway;

import com.kafkick.waiting.domain.admission.AdmissionDecision;
import com.kafkick.waiting.domain.queue.EtaPolicy;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import java.util.Objects;
import java.util.function.DoubleSupplier;

/**
 * 거절의 봉투. 판정값을 응답 코드와 다시 올 시각으로 옮긴다.
 *
 * <p><b>필터 밖으로 꺼내 둔다.</b> 필터에 정적 헬퍼로 두면 시험이 클래스 이름으로만
 * 부를 수 있고, 폴링 정책 같은 협력자가 붙을 때 호출부를 전부 고쳐야 한다.
 */
public final class Rejection {

    private final PollIntervalPolicy poll;

    private Rejection(PollIntervalPolicy poll) {
        this.poll = Objects.requireNonNull(poll, "poll 은 필수다");
    }

    public static Rejection of(PollIntervalPolicy poll) {
        return new Rejection(poll);
    }

    /**
     * 거절의 응답 코드. <b>전부 열거한다</b> — 빠짐없이 적어야 새 판정값이 생겼을 때
     * 컴파일이 깨진다. {@code default} 로 두면 새 사유가 조용히 매진으로 나간다.
     */
    public ApiError.Code code(AdmissionDecision decision) {
        return switch (decision) {
            case REJECT_SOLD_OUT -> ApiError.Code.SOLD_OUT;
            case REJECT_QUEUE_FULL -> ApiError.Code.QUEUE_FULL;
            case REJECT_OVERLOAD -> ApiError.Code.TEMPORARILY_UNAVAILABLE;
            // 차례가 온 사람을 큐 뒤로 안 돌린다. 되돌리면 허가가 "아마도" 가 된다.
            case RETRY_TOKEN -> ApiError.Code.RETRY_TOKEN;
            case PASS_TOKEN, PASS_BYPASS, PASS_FAIL_OPEN, PASS_UNDER_CAP,
                 ENQUEUE_STALE, ENQUEUE_ALWAYS, ENQUEUE_BACKLOG,
                 ENQUEUE_RATE_COUPON, ENQUEUE_RATE_GLOBAL, ENQUEUE_KEY_SATURATED,
                 ENQUEUE_CIRCUIT_OPEN ->
                    throw new IllegalArgumentException("거절이 아니다: " + decision);
        };
    }

    /**
     * 다시 와도 되는 때. 같은 값을 주면 다 같이 돌아오므로 흔들어서 흩는다.
     *
     * <p>배수를 인자로 받는다. 안 받는 갈래를 남기면 거절 갈래가 그쪽을 쓰고,
     * 과부하일수록 거절 비중이 커져 예산이 절반만 걸린다.
     */
    public int retryAfterSec(AdmissionDecision decision, DoubleSupplier random,
            double pollScale) {
        return switch (decision) {
            // 차례가 온 사람은 배수에서 뺀다. 멀리 보내면 수명 있는 입장 토큰이
            // 죽어 줄 맨 뒤에 새 순번으로 다시 서고, 그것이 곧 순번 역행이자
            // 추월이다. 밴드가 1초면 흔들림이 0 이라 통째로 같이 돌아온다.
            //
            // 그래서 차단된 토큰 보유자가 쌓였다가 매초 같은 순간에 함께 돌아오고,
            // 서킷이 닫히려는 순간을 되밀 수 있다.
            case RETRY_TOKEN -> (int) poll.intervalSec(0, random, PollIntervalPolicy.NO_SCALE);
            case REJECT_QUEUE_FULL, REJECT_OVERLOAD ->
                    (int) poll.intervalSec(EtaPolicy.UNKNOWN, random, pollScale);
            // 매진은 안 싣는다. 다시 와도 소용없는데 시각을 주면 재시도를 부른다.
            case REJECT_SOLD_OUT -> ApiError.NO_RETRY;
            case PASS_TOKEN, PASS_BYPASS, PASS_FAIL_OPEN, PASS_UNDER_CAP,
                 ENQUEUE_STALE, ENQUEUE_ALWAYS, ENQUEUE_BACKLOG,
                 ENQUEUE_RATE_COUPON, ENQUEUE_RATE_GLOBAL, ENQUEUE_KEY_SATURATED,
                 ENQUEUE_CIRCUIT_OPEN ->
                    throw new IllegalArgumentException("거절이 아니다: " + decision);
        };
    }
}

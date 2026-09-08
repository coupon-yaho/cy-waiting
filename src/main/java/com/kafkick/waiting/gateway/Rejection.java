package com.kafkick.waiting.gateway;

import com.kafkick.waiting.domain.admission.AdmissionDecision;
import com.kafkick.waiting.domain.queue.EtaPolicy;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import java.util.function.DoubleSupplier;

/**
 * 거절의 응답 값. 판정값을 응답 코드와 다시 올 시각으로 옮긴다.
 *
 * <p><b>거절의 다시 올 시각은 여기 하나가 낸다.</b> 갈래를 손으로 펴 둔 자리가 셋이었고,
 * 하나가 바뀌면 나머지가 조용히 갈렸다. 줄에 선 사람의 다음 폴링은 아직 밖에 있다.
 */
final class Rejection {

    private static final PollIntervalPolicy STANDARD = PollIntervalPolicy.standard();

    private final PollIntervalPolicy poll;

    private Rejection(PollIntervalPolicy poll) {
        this.poll = poll;
    }

    /**
     * <b>만드는 길이 이것뿐이다.</b> 정책을 인자로 받는 문을 열어 두면 시험이 제 것을
     * 들고, 운영의 흔들림을 0 으로 바꿔도 그 시험이 초록으로 남는다.
     */
    static Rejection standard() {
        return new Rejection(STANDARD);
    }

    /**
     * 거절의 응답 코드. <b>전부 열거한다</b> — 빠짐없이 적어야 새 판정값이 생겼을 때
     * 컴파일이 깨진다. {@code default} 로 두면 새 사유가 조용히 매진으로 나간다.
     */
    ApiError.Code code(AdmissionDecision decision) {
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
    int retryAfterSec(AdmissionDecision decision, DoubleSupplier random,
            double pollScale) {
        return switch (decision) {
            // 차례가 온 사람은 배수에서 뺀다. 멀리 보내면 수명 있는 입장 토큰이
            // 죽어 줄 맨 뒤에 새 순번으로 다시 서고, 그것이 곧 순번 역행이자
            // 추월이다.
            //
            // **여전히 같은 초에 몰린다.** 이 밴드는 넷 중 셋이 1초로 접히고,
            // 다시 올 시각이 정수라 서브초 위상은 안 흩어진다 — 초당 유입이 0.8배로
            // 줄 뿐이라 서킷의 프로브 자리를 먹는 그림은 남는다 (CY-898).
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

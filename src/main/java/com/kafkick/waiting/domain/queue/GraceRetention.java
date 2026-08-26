package com.kafkick.waiting.domain.queue;

/**
 * 이탈 기록과 입장 표시를 들고 있는 기간.
 *
 * <p><b>입장 토큰보다 길어야 한다.</b> 표시가 먼저 사라지면 아직 유효한 토큰을
 * 든 사람이 폴링에서 종료를 받는다 — 다시 서면 그 사이 온 사람들 뒤로 간다.
 */
public final class GraceRetention {

    /**
     * 보관 기간(초).
     *
     * <p>재방문자 식별에 필요한 시간과 토큰 수명 중 <b>큰 쪽</b>이다. 값을 손으로
     * 적으면 토큰 수명을 늘릴 때 이쪽이 안 따라온다.
     */
    public static final long SECONDS = Math.max(300, EntryToken.TTL_SEC * 2);

    private GraceRetention() {
    }
}

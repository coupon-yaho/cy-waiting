package com.kafkick.waiting.gateway;

/**
 * 실패를 지표 라벨로 쓸 수 있는 몇 갈래로 좁힌다 (LG-4). <b>밖에서 온 클래스명을
 * 그대로 안 쓴다</b> — 라이브러리가 클래스 하나 바꿀 때마다 시계열이 늘고, 익명
 * 클래스면 라벨이 빈 문자열이 된다.
 */
final class FailureCause {

    /** 실패가 아닌 판정의 자리. 태그 키 집합을 늘 같게 두려고 채운다. */
    static final String NONE = "none";

    /** 우리 코드가 계약 위반으로 던진 것. 응답 형식이나 상태가 어긋났다. */
    static final String BAD_STATE = "bad-state";

    /** 그 밖의 전부. 지금은 레디스가 느리거나 끊긴 경우가 대부분이다. */
    static final String IO = "io";

    private FailureCause() {
    }

    /**
     * <b>레디스가 느린 것과 끊긴 것은 아직 못 가른다.</b> 레티스가 두 경우를
     * 자기 예외 계층으로 올리고 어댑터가 그것을 번역하지 않아서다. 가르려면
     * 어댑터 경계에서 우리 예외로 바꿔야 한다 (CY-602).
     */
    static String of(Throwable e) {
        return e instanceof IllegalArgumentException || e instanceof IllegalStateException
                ? BAD_STATE
                : IO;
    }
}

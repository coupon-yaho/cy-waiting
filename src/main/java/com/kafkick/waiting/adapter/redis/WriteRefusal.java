package com.kafkick.waiting.adapter.redis;

/**
 * 레디스가 쓰기를 거부했는가 (CY-970).
 *
 * <p>거부는 메모리를 줄여야 풀리고 단절은 연결이 돌아오면 풀린다. 접으면 지표로 못 가른다.
 */
final class WriteRefusal {

    /** 레디스가 상한에서 내는 문장. 앞뒤에 스크립트 해시가 붙어 오기도 해 부분 일치로 본다. */
    private static final String OOM = "OOM command not allowed";

    /** 원인이 도는 예외에서 안 멎게 둔다. 깊이가 이보다 깊은 감싸기는 이 저장소에 없다. */
    private static final int MAX_DEPTH = 10;

    private WriteRefusal() {
    }

    /** 스크립트 실행은 원인을 감싸서 오므로 겉만 보지 않는다. */
    static boolean refused(Throwable cause) {
        Throwable at = cause;
        for (int depth = 0; at != null && depth < MAX_DEPTH; depth++) {
            String message = at.getMessage();
            if (message != null && message.contains(OOM)) {
                return true;
            }
            at = at.getCause() == at ? null : at.getCause();
        }
        return false;
    }
}

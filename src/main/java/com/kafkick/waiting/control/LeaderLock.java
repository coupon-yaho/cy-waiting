package com.kafkick.waiting.control;

/**
 * 락을 물었을 때 돌아온 <b>사실</b>.
 *
 * <p>참·거짓 하나로 접으면 "내가 못 잡았다" 와 "누가 잡고 있다" 가 같은 값이 된다.
 * 스플릿 브레인을 사후에 조사할 때 필요한 것이 정확히 뒤엣것이라, 소유자를 버리면
 * <b>"그럼 누가 리더였나" 에 답할 수 없다.</b>
 *
 * @param acquired  참이면 내가 리더다 — 새로 잡았거나 연장했다
 * @param owner     지금 락을 쥔 노드
 * @param ttlMillis 남은 리스
 */
public record LeaderLock(boolean acquired, String owner, long ttlMillis) {

    public LeaderLock {
        if (owner == null) {
            throw new IllegalArgumentException("owner 는 필수다 — 못 잡았어도 누가 쥐었는지는 사실이다");
        }
    }

    public static LeaderLock mine(String owner, long ttlMillis) {
        return new LeaderLock(true, owner, ttlMillis);
    }

    public static LeaderLock heldBy(String owner, long ttlMillis) {
        return new LeaderLock(false, owner, ttlMillis);
    }
}

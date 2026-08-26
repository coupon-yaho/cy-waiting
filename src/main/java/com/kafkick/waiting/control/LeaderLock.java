package com.kafkick.waiting.control;

/**
 * 락을 물었을 때 돌아온 <b>사실</b>.
 *
 * <p>참·거짓으로 접으면 "내가 못 잡았다" 와 "누가 잡고 있다" 가 같은 값이 된다.
 * 스플릿 브레인을 사후에 조사할 때 필요한 것이 뒤엣것이다.
 *
 * @param acquired  참이면 내가 리더다 — 새로 잡았거나 연장했다
 * @param owner     지금 락을 쥔 노드. <b>그 사이 풀렸으면 빈 문자열</b>이다
 * @param ttlMillis 남은 리스. 키가 없으면 음수다
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

    /** 로그에 쓸 소유자. 빈 값을 그대로 찍으면 문장에 구멍이 생겨 원인을 오해한다. */
    public String describeOwner() {
        return owner.isBlank() ? "(그 사이 풀렸다)" : owner;
    }
}

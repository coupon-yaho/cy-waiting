package com.kafkick.waiting.chaos;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 사용자별 순번과 자리를 따라간다 (RC2·RC5).
 *
 * <p>응답을 사람 단위로 모은다. 전체 평균만 보면 한 사람이 뒤로 밀린 것이 안
 * 잡힌다 — 밀린 사람과 당겨진 사람이 서로 상쇄되기 때문이다.
 */
public final class RankTracker {

    /** 한 사람이 받은 순번과 그때의 자리. 순서가 곧 시간이다. */
    private record Seen(long rank, double score) {
    }

    private final Map<String, List<Seen>> byMember = new LinkedHashMap<>();

    /**
     * 응답 하나를 기록한다.
     *
     * @param rank  그때 알려 준 순번
     * @param score 그때의 줄 자리. 바뀌면 걷혔다가 다시 선 것이다
     */
    public void saw(String memberId, long rank, double score) {
        byMember.computeIfAbsent(memberId, id -> new ArrayList<>()).add(new Seen(rank, score));
    }

    public Set<String> members() {
        return Set.copyOf(byMember.keySet());
    }

    public List<Long> ranksOf(String memberId) {
        return byMember.getOrDefault(memberId, List.of()).stream().map(Seen::rank).toList();
    }

    /**
     * RC2 — 뒤로 밀린 사람들.
     *
     * <p><b>아무도 안 봤으면 통과가 아니다.</b> 관측이 없는 것을 빈 목록으로
     * 돌려주면 시나리오가 사람을 한 명도 안 세우고도 초록이 된다.
     */
    public List<String> regressions() {
        if (byMember.isEmpty()) {
            return List.of("RC2 관측이 없다 — 순번을 한 번도 안 봤다");
        }
        List<String> found = new ArrayList<>();
        byMember.forEach((id, seen) -> RecoveryCriteria
                .rankRegressed(seen.stream().map(Seen::rank).toList())
                .ifPresent(why -> found.add("%s (%s)".formatted(why, id))));
        return List.copyOf(found);
    }

    /** RC5 — 자리를 잃은 사람들. score 가 바뀌면 걷혔다가 새 순번으로 다시 선 것이다. */
    public List<String> seatChanges() {
        List<String> found = new ArrayList<>();
        byMember.forEach((id, seen) -> {
            for (int i = 1; i < seen.size(); i++) {
                if (seen.get(i).score() != seen.get(i - 1).score()) {
                    found.add("RC5 자리를 잃었다 — %s 가 %s 에서 %s 로 옮겨졌다"
                            .formatted(id, seen.get(i - 1).score(), seen.get(i).score()));
                    return;
                }
            }
        });
        return List.copyOf(found);
    }
}

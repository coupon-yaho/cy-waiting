package com.kafkick.waiting.chaos;

import java.util.List;
import java.util.Optional;

/**
 * 노드를 두드린 결과의 판정.
 *
 * <p><b>판정식이 시나리오마다 따로 살면 두 게이트가 다른 기준으로 초록이 된다.</b> 비대칭 소실과
 * 하트비트 흔들림은 같은 것을 재는데, 허용 상한 식이 한쪽만 바뀌어도 아무도 못 알아챈다.
 */
public final class NodeIssueProbe {

    private NodeIssueProbe() {
    }

    /**
     * 그 창에서 뒷단에 닿아도 되는 수. <b>초당 크레딧을 잰 창 길이로 편다</b> — 여러 초짜리 창을 초당
     * 값과 견주면 정상도 초과로 읽힌다. 초 경계가 한 칸 걸치므로 한 초를 더 준다.
     */
    public static long 허용(long 크레딧, long 걸린_나노) {
        long 초 = Math.max(1, (걸린_나노 + 999_999_999L) / 1_000_000_000L);
        return 크레딧 * (초 + 1);
    }

    /** 전원이 5xx 는 아닌가. 끊긴 노드도 판정은 내야 한다 — 전부 막히면 그 구간이 통째로 안 보인다. */
    public static Optional<String> 전면_차단이_아니다(String 이름, List<Integer> 상태) {
        if (상태.isEmpty()) {
            return Optional.of("%s — 보낸 것이 없다".formatted(이름));
        }
        long 답한_것 = 상태.stream().filter(status -> status < 500).count();
        return 답한_것 > 0 ? Optional.empty()
                : Optional.of("%s — 전원이 5xx 다 (보낸 %d)".formatted(이름, 상태.size()));
    }

    /** 5xx 가 섞였는가. 끊긴 노드도 판정은 내야 한다 — 못 내면 그 구간이 통째로 안 보인다. */
    public static Optional<String> 멎지_않았다(String 이름, List<Integer> 상태) {
        if (상태.isEmpty()) {
            return Optional.of("%s — 보낸 것이 없다".formatted(이름));
        }
        long 멎은_것 = 상태.stream().filter(status -> status >= 500).count();
        return 멎은_것 == 0 ? Optional.empty()
                : Optional.of("%s — %d 건이 5xx 다 (보낸 %d)".formatted(이름, 멎은_것, 상태.size()));
    }
}

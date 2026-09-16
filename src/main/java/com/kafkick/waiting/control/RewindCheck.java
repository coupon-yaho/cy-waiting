package com.kafkick.waiting.control;

import java.util.List;

/**
 * 되감기 점검 결과 (CY-856).
 *
 * @param rewound  이 노드가 쓴 임계보다 뒤로 간 쿠폰들
 * @param measured 견줄 기준이 있어 실제로 본 쿠폰 수. 0 이면 못 잰 것이지 깨끗한 것이 아니다
 */
public record RewindCheck(List<String> rewound, int measured) {

    /** 기준이 하나도 없다. 승계 직후의 새 리더가 이 자리다. */
    public static final RewindCheck NONE = new RewindCheck(List.of(), 0);
}

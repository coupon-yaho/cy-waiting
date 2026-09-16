package com.kafkick.waiting.control;

import java.util.List;

/**
 * 되감기 점검 결과 (CY-856). <b>본 수를 같이 든다</b> — 0 은 깨끗한 것이 아니라 못 잰 것이고, 둘을 한 값으로 접으면
 * 기준이 없는 새 리더가 "이상 없음" 으로 읽힌다.
 *
 * @param rewound  이 노드가 쓴 임계보다 뒤로 간 쿠폰들
 * @param measured 견줄 기준이 있어 실제로 본 쿠폰 수
 */
public record RewindCheck(List<String> rewound, int measured) {

    /** 기준이 하나도 없다. 승계 직후의 새 리더가 이 자리다. */
    public static final RewindCheck NONE = new RewindCheck(List.of(), 0);

    /** 본 것보다 많이 감길 수 없다. 그 모순을 담으면 되감김이 "못 쟀다" 로 삼켜진다. */
    public RewindCheck {
        if (measured < 0) {
            throw new IllegalArgumentException("본 쿠폰 수는 음수일 수 없다: " + measured);
        }
        rewound = List.copyOf(rewound);
        if (rewound.size() > measured) {
            throw new IllegalArgumentException(
                    "감긴 쿠폰이 본 쿠폰보다 많다: %d > %d".formatted(rewound.size(), measured));
        }
    }

    public static RewindCheck seen(List<String> rewound, int measured) {
        return new RewindCheck(rewound, measured);
    }
}

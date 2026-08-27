package com.kafkick.waiting.domain.coupon;

/**
 * 운영자가 적은 값을 읽습니다.
 *
 * <p>읽기를 밖에 둡니다. 압축 생성자와 정적 팩토리에서 부를 수 있는 것은 정적뿐이라,
 * 레코드 안에 두면 그 자리에서만 쓰이는 정적 메서드가 생깁니다 (JS-13).
 */
final class TunableValues {

    private TunableValues() {
    }

    /** 상태가 없지만 인스턴스입니다 — 읽을 값이 늘면 여기 필드가 생깁니다 (JS-13). */
    static TunableValues create() {
        return new TunableValues();
    }

    /**
     * <b>1 이상이면 안 됩니다.</b> 한산 통과가 노드 예산 전체를 쓰면 토큰을 든
     * 사람이 밀리고, 0 이면 그 경로가 통째로 막힙니다.
     */
    double ratio(String json, String key, double fallback) {
        Double value = number(json, key);
        return value != null && value > 0 && value < 1 ? value : fallback;
    }

    /** 0 이하면 격벽이 아무도 안 들여보냅니다. */
    long seconds(String json, String key, long fallback) {
        Double value = number(json, key);
        return value != null && value >= 1 ? (long) (double) value : fallback;
    }

    /**
     * 값 하나를 꺼냅니다.
     *
     * <p><b>도메인은 라이브러리를 안 씁니다</b> (DS-1). 읽는 것이 수 몇 개뿐이라
     * 파서를 들이는 것보다 여기서 끝내는 편이 의존을 안 늘립니다.
     */
    private Double number(String json, String key) {
        // **키 자리인지 확인한다.** 원문에서 찾기만 하면 `{"note":"inFlightSeconds"}`
        // 처럼 값 안에 든 문자열도 키로 읽고, 그 뒤의 아무 수나 설정으로 들인다.
        String quoted = '"' + key + '"';
        int at = -1;
        for (int from = json.indexOf(quoted); from >= 0; from = json.indexOf(quoted, from + 1)) {
            int before = from - 1;
            while (before >= 0 && Character.isWhitespace(json.charAt(before))) {
                before--;
            }
            // 키는 `{` 나 `,` 뒤에 온다. 값 자리에 오면 `:` 뒤다.
            if (before >= 0 && (json.charAt(before) == '{' || json.charAt(before) == ',')) {
                at = from;
                break;
            }
        }
        if (at < 0) {
            return null;
        }
        int colon = json.indexOf(':', at + quoted.length());
        if (colon < 0) {
            return null;
        }
        int start = colon + 1;
        while (start < json.length() && Character.isWhitespace(json.charAt(start))) {
            start++;
        }
        int end = start;
        while (end < json.length() && "-+.0123456789eE".indexOf(json.charAt(end)) >= 0) {
            end++;
        }
        // **뒤에 뭐가 붙었으면 그 값은 못 믿는다.** 앞부분만 읽으면 `8oops` 가
        // 8 로 들어가고, 운영자는 자기가 적은 값이 통과한 줄 안다.
        int after = end;
        while (after < json.length() && Character.isWhitespace(json.charAt(after))) {
            after++;
        }
        if (after < json.length() && json.charAt(after) != ',' && json.charAt(after) != '}') {
            return null;
        }
        try {
            return Double.valueOf(json.substring(start, end));
        } catch (NumberFormatException e) {
            // **그 값만 버립니다.** 여기서 던지면 오타 하나가 방금 고친 다른 값도
            // 되돌립니다.
            return null;
        }
    }
}

package com.kafkick.waiting.domain.coupon;

/**
 * 운영자가 적은 값을 읽습니다.
 *
 * <p>레코드 안에 두면 그 자리에서만 쓰이는 정적 메서드가 생깁니다 (JS-13).
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
     * 사람이 밀립니다. 0 은 그 경로를 끄는 값이라 그대로 받습니다.
     */
    double ratio(String json, String key, double fallback) {
        Double value = number(json, key);
        return value != null && value >= 0 && value < 1 ? value : fallback;
    }

    /**
     * <b>long 안의 정수만 받습니다.</b> {@code 1e309} 는 무한대가 되고, 좁히면
     * {@code Long.MAX_VALUE} 라는 멀쩡해 보이는 값으로 저장됩니다.
     */
    long seconds(String json, String key, long fallback) {
        Double value = number(json, key);
        // 2^63 이상은 좁히면 Long.MAX_VALUE 로 눌려, 터무니없는 값이 멀쩡한
        // 값으로 저장된다. 무한대도 같은 길로 들어온다.
        return value != null && value >= 1 && value < 0x1p63 ? (long) (double) value : fallback;
    }

    /**
     * 값 하나를 꺼냅니다.
     *
     * <p><b>도메인은 라이브러리를 안 씁니다</b> (DS-1). 읽는 것이 수 몇 개뿐입니다.
     */
    private Double number(String json, String key) {
        // **최상위 객체의 멤버만 키로 본다.** 앞뒤 문자만 보면 중첩 객체 안의
        // 같은 이름이나 배열 원소도 키로 통과한다 — 운영자가 붙여 넣는 문자열에
        // 블록 하나가 섞이면 조용히 그쪽이 이긴다.
        int at = skipSpace(json, 0);
        if (at >= json.length() || json.charAt(at) != '{') {
            return null;
        }
        at++;
        while (true) {
            at = skipSpace(json, at);
            if (at >= json.length() || json.charAt(at) != '"') {
                return null;
            }
            int nameEnd = stringEnd(json, at);
            if (nameEnd < 0) {
                return null;
            }
            String name = json.substring(at + 1, nameEnd);
            at = skipSpace(json, nameEnd + 1);
            if (at >= json.length() || json.charAt(at) != ':') {
                return null;
            }
            at = skipSpace(json, at + 1);
            int valueEnd = valueEnd(json, at);
            if (valueEnd < 0) {
                return null;
            }
            if (name.equals(key)) {
                return asNumber(json.substring(at, valueEnd));
            }
            at = skipSpace(json, valueEnd);
            // 다음 멤버가 없으면 이 키는 없는 것이다.
            if (at >= json.length() || json.charAt(at) != ',') {
                return null;
            }
            at++;
        }
    }

    /** 여는 따옴표에서 닫는 따옴표까지. 이스케이프된 따옴표는 안 센다. */
    private int stringEnd(String json, int open) {
        for (int i = open + 1; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '\\') {
                i++;
            } else if (c == '"') {
                return i;
            }
        }
        return -1;
    }

    /** 값이 끝나는 자리. 중첩된 객체와 배열은 통째로 건너뛴다. */
    private int valueEnd(String json, int start) {
        if (start >= json.length()) {
            return -1;
        }
        char first = json.charAt(start);
        if (first == '"') {
            int end = stringEnd(json, start);
            return end < 0 ? -1 : end + 1;
        }
        if (first == '{' || first == '[') {
            return blockEnd(json, start);
        }
        int i = start;
        while (i < json.length() && json.charAt(i) != ',' && json.charAt(i) != '}') {
            i++;
        }
        return i == start ? -1 : i;
    }

    /** 여는 괄호에 맞는 닫는 괄호 다음 자리. 문자열 안의 괄호는 안 센다. */
    private int blockEnd(String json, int open) {
        int depth = 0;
        for (int i = open; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '"') {
                int end = stringEnd(json, i);
                if (end < 0) {
                    return -1;
                }
                i = end;
            } else if (c == '{' || c == '[') {
                depth++;
            } else if (c == '}' || c == ']') {
                depth--;
                if (depth == 0) {
                    return i + 1;
                }
            }
        }
        return -1;
    }

    private int skipSpace(String json, int from) {
        int i = from;
        while (i < json.length() && Character.isWhitespace(json.charAt(i))) {
            i++;
        }
        return i;
    }

    /** 수로 읽습니다. <b>뒤에 뭐가 붙었으면 그 값은 못 믿습니다</b> — {@code 8oops}. */
    private Double asNumber(String raw) {
        // 빈 값은 여기 안 온다 — 값 자리가 비면 앞에서 이미 -1 로 끊긴다.
        String text = raw.strip();
        for (int i = 0; i < text.length(); i++) {
            if ("-+.0123456789eE".indexOf(text.charAt(i)) < 0) {
                return null;
            }
        }
        try {
            return Double.valueOf(text);
        } catch (NumberFormatException e) {
            // **그 값만 버립니다.** 여기서 던지면 오타 하나가 방금 고친 다른 값도
            // 되돌립니다.
            return null;
        }
    }
}

package com.kafkick.waiting.adapter.redis;

import java.nio.charset.StandardCharsets;

/**
 * 큐 샤드를 정한다. <b>반드시 sticky 여야 한다</b> (E-7).
 *
 * <p>같은 사람이 틱마다 다른 큐에 서면 순위가 앞뒤로 튀고 불변식 3(순번 역행 0)이
 * 깨진다. 그래서 {@code String.hashCode()} 를 쓰지 않는다 — 판이 바뀌면 값이
 * 달라질 수 있고, 그 순간 전원이 다른 샤드로 옮겨 간다.
 */
public final class ShardHash {

    /** CRC16-CCITT (XMODEM). 레디스 클러스터가 슬롯을 정할 때 쓰는 것과 같다. */
    private static final int POLYNOMIAL = 0x1021;

    private ShardHash() {
    }

    /** 이 사람이 설 큐. 같은 입력은 언제 어디서나 같은 값을 낸다. */
    public static int shardOf(String memberId, int shards) {
        if (shards < 1) {
            throw new IllegalArgumentException("shards 는 1 이상이어야 한다: " + shards);
        }
        if (memberId == null || memberId.isBlank()) {
            throw new IllegalArgumentException("memberId 는 필수다");
        }
        return shards == 1 ? 0 : crc16(memberId) % shards;
    }

    /** 바이트 단위로 돈다 — 문자열 인코딩이 갈리면 값도 갈린다. */
    public static int crc16(String value) {
        if (value == null) {
            throw new IllegalArgumentException("memberId 는 필수다");
        }
        if (value.isEmpty()) {
            // 빈 문자열의 CRC 는 0 이다. 유효성은 호출부가 본다.
            return 0;
        }

        int crc = 0;
        for (byte b : value.getBytes(StandardCharsets.UTF_8)) {
            crc ^= (b & 0xFF) << 8;
            for (int bit = 0; bit < 8; bit++) {
                crc = (crc & 0x8000) != 0 ? (crc << 1) ^ POLYNOMIAL : crc << 1;
                crc &= 0xFFFF;
            }
        }
        return crc;
    }
}

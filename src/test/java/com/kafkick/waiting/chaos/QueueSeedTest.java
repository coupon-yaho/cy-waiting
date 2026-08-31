package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 하네스 자기검증. <b>줄을 못 세우는 픽스처는 전 시나리오를 통과시킨다</b> —
 * 실제로 그랬다. 점수를 100 부터 넣었더니 스위퍼가 그 줄을 아예 안 봤다.
 */
@Tag("chaos")
class QueueSeedTest {

    private static final String COUPON = "seed-check";

    private static final int 줄_선_사람 = 3;

    /** 심어 둔 줄의 생존 신호 수명. */
    private static final Duration 수명 = Duration.ofMinutes(5);

    private static RedisFaults faults;

    private static StatefulRedisConnection<String, String> 연결;

    @BeforeAll
    static void 띄운다() {
        faults = RedisFaults.시작한다();
        연결 = faults.연결한다();
    }

    @AfterAll
    static void 내린다() {
        if (연결 != null) {
            연결.close();
        }
        if (faults != null) {
            faults.close();
        }
    }

    @Test
    @DisplayName("등록_스크립트가_이미_선_사람으로_알아본다")
    void 등록_스크립트가_이미_선_사람으로_알아본다() {
        Map<String, Double> 자리 = QueueSeed.줄을_세운다(연결, COUPON, 줄_선_사람, 수명);

        List<Object> 결과 = 다시_등록한다("q0");

        assertThat(결과.get(2)).as("이미 줄에 있었다고 알아본다").isEqualTo(1L);
        assertThat(Double.parseDouble(String.valueOf(결과.get(0))))
                .as("자리를 그대로 돌려준다").isEqualTo(자리.get("q0"));
    }

    @Test
    @DisplayName("점수가_배분_임계와_같은_자에_있다")
    void 점수가_배분_임계와_같은_자에_있다() {
        Map<String, Double> 자리 = QueueSeed.줄을_세운다(연결, COUPON + "-scale", 줄_선_사람, 수명);

        // 레디스 시계의 마이크로초다. 초 단위나 작은 정수로 넣으면 첫 배분이
        // 임계를 그 위로 올리는 순간 심어 둔 줄이 통째로 스위프 창 밖으로 나간다.
        // **서버 시계로 잰다** — 시험 장비 시계로 재면 둘이 어긋난 만큼
        // 여유를 넓혀야 하고, 그러면 이 판정이 자릿수만 보게 된다.
        long 지금_마이크로 = 서버_마이크로();
        assertThat(자리.get("q0")).isBetween(지금_마이크로 - 60_000_000.0,
                지금_마이크로 + 60_000_000.0);
    }

    @Test
    @DisplayName("생존_신호가_만료_시각으로_들어간다")
    void 생존_신호가_만료_시각으로_들어간다() {
        String coupon = COUPON + "-alive";
        QueueSeed.줄을_세운다(연결, coupon, 줄_선_사람, 수명);

        // 초 단위다. 스위퍼가 지금 시각과 비교하므로 마이크로초로 넣으면
        // 영영 살아 있는 것으로 읽혀 이탈자 청소가 아무도 안 걷는다.
        long 지금_초 = 서버_마이크로() / 1_000_000L;
        assertThat(연결.sync().zrangeWithScores(RedisKeys.alive(coupon, 1, 0), 0, -1))
                .as("세 명이 다 생존 신호를 갖고, 만료는 지금부터 수명 안쪽이다")
                .hasSize(줄_선_사람)
                .allSatisfy(항목 -> assertThat((long) 항목.getScore())
                        .isBetween(지금_초, 지금_초 + 수명.toSeconds() + 5));
    }

    private List<Object> 다시_등록한다(String member) {
        String script = 스크립트를_읽는다();
        String[] keys = {
                RedisKeys.queue(COUPON, 1, 0), RedisKeys.maxScore(COUPON, 1, 0),
                RedisKeys.alive(COUPON, 1, 0), RedisKeys.admitted(COUPON, 1, 0),
                RedisKeys.grace(COUPON, 1, 0),
        };
        String[] argv = {member, "300", "300", "-1",
                String.valueOf(서버_마이크로() / 1_000_000L), "600"};
        return 연결.sync().eval(script, ScriptOutputType.MULTI, keys, argv);
    }

    /** 레디스 시계. 픽스처가 이 시계로 점수를 매기므로 검증도 같은 시계로 한다. */
    private long 서버_마이크로() {
        List<String> time = 연결.sync().time();
        return Long.parseLong(time.get(0)) * 1_000_000L + Long.parseLong(time.get(1));
    }

    private String 스크립트를_읽는다() {
        try (var stream = getClass().getResourceAsStream("/redis/enqueue.lua")) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("등록 스크립트를 못 읽었다", e);
        }
    }
}

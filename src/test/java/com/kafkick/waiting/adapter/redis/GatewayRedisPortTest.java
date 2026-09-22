package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.control.GatewayHeartbeatLoop;
import com.kafkick.waiting.domain.admission.CircuitState;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.core.publisher.Mono;

/**
 * 하트비트 어댑터.
 *
 * <p>스크립트는 {@code GatewayHeartbeatTest} 가 잰다. 여기서 재는 것은 <b>자바가
 * 그 반환을 분모로 옳게 읽는가</b> 다 — 스크립트가 맞아도 파싱이 틀리면 분모가
 * 조용히 어긋나고, 그건 어느 쪽 시험도 안 잡는다.
 */
@Tag("integration")
@SpringBootTest
class GatewayRedisPortTest extends RedisContainerSupport {

    private static final Duration WAIT = Duration.ofSeconds(5);

    private static final long REAP_AFTER_SEC = 30;

    @Autowired
    private ReactiveStringRedisTemplate redis;

    private GatewayRedisPort port;

    /**
     * <b>살아 있는 하트비트 루프를 멈춘다.</b>
     *
     * <p>이 컨텍스트에는 매 틱 자기를 등록하는 루프가 있고, 그 노드가 이
     * 시험이 세는 수에 섞인다 — 지운 뒤 단언 사이에 한 번만 찍혀도 깨진다.
     * 운영 키를 쓰는 시험이라 자리를 가를 수 없으니 원을 멈춘다.
     */
    @Autowired
    private GatewayHeartbeatLoop 하트비트;

    @BeforeEach
    void 준비() {
        하트비트.stop();
        port = GatewayRedisPort.of(redis);
        redis.delete(RedisKeys.INSTANCES).block(WAIT);
    }

    /** 표를 인정하는 신선도. 분모의 임계보다 짧다. */
    private static final long VOTE_FRESH_SEC = 5;

    /** 한 노드가 평시 서킷으로 찍는다. 분모만 본다. */
    private Mono<Integer> 노드(String id) {
        return 찍는다(id, CircuitState.CLOSED).map(GatewayRedisPort.Presence::alive);
    }

    private Mono<GatewayRedisPort.Presence> 찍는다(String id, CircuitState circuit) {
        return 찍는다(id, circuit, 0);
    }

    private Mono<GatewayRedisPort.Presence> 찍는다(String id, CircuitState circuit, long passed) {
        return port.beat(id, REAP_AFTER_SEC, VOTE_FRESH_SEC, circuit, passed);
    }

    @Test
    @DisplayName("자기를_세고_남도_센다")
    void 자기를_세고_남도_센다() {
        assertThat(노드("gw-a").block(WAIT)).isEqualTo(1);

        assertThat(노드("gw-b").block(WAIT)).isEqualTo(2);
        // 같은 노드가 다시 찍어도 늘지 않는다. 늘면 배포마다 분모가 부푼다.
        assertThat(노드("gw-a").block(WAIT)).isEqualTo(2);
    }

    @Test
    @DisplayName("나간_노드는_즉시_빠진다")
    void 나간_노드는_즉시_빠진다() {
        노드("gw-a").block(WAIT);
        노드("gw-b").block(WAIT);

        port.leave("gw-b").block(WAIT);

        // 임계를 안 기다린다. 기다리면 배포마다 그 시간 동안 전 노드가 몫을 덜 쓴다.
        assertThat(노드("gw-a").block(WAIT)).isEqualTo(1);
    }

    /**
     * <b>표를 갈래별로 세어 돌려준다</b> (CY-791).
     *
     * <p>분모와 표가 <b>같은 왕복</b>에서 나와야 한다. 나눠 읽으면 그 사이에
     * 노드가 드나들어 서로 다른 회차의 값이 섞인다.
     */
    // 열린 것과 반쯤 열린 것을 합치지 않는다. 합치면 전 노드가 동시에 반쯤 열린
    // 순간이 과반으로 접혀 배분이 0 이 되고, 그러면 서킷이 영영 안 닫힌다.
    @Test
    @DisplayName("표를_갈래별로_세어_돌려준다")
    void 표를_갈래별로_세어_돌려준다() {
        찍는다("gw-a", CircuitState.OPEN).block(WAIT);
        찍는다("gw-b", CircuitState.CLOSED).block(WAIT);

        GatewayRedisPort.Presence seen = 찍는다("gw-c", CircuitState.HALF_OPEN).block(WAIT);

        assertThat(seen).isEqualTo(GatewayRedisPort.Presence.withoutEjection(3, 1, 1, 3, 0, 3));
    }

    /**
     * <b>값은 옛 형식 그대로 둔다</b> — 표는 별도 field 에 실린다.
     *
     * <p>값에 붙이면 롤아웃 중 옛 노드의 {@code tonumber} 가 nil 을 내고 새
     * 노드를 죽은 것으로 판정해 지운다. 옛 리더가 보는 분모가 줄어 남은 노드가
     * 각자 큰 몫을 쓰고, 그건 초과 발급 방향이다.
     */
    @Test
    @DisplayName("생존_값은_초만_담는다")
    void 생존_값은_초만_담는다() {
        찍는다("gw-a", CircuitState.OPEN).block(WAIT);

        String stored = redis.<String, String>opsForHash()
                .get(RedisKeys.INSTANCES, "gw-a").block(WAIT);

        assertThat(stored).containsOnlyDigits();
    }

    /** 나간 노드는 표도 같이 빠진다. 안 그러면 해시가 배포 이력만큼 자란다. */
    @Test
    @DisplayName("나간_노드의_표도_같이_빠진다")
    void 나간_노드의_표도_같이_빠진다() {
        찍는다("gw-a", CircuitState.OPEN).block(WAIT);
        찍는다("gw-b", CircuitState.CLOSED).block(WAIT);

        port.leave("gw-a").block(WAIT);

        GatewayRedisPort.Presence seen = 찍는다("gw-b", CircuitState.CLOSED).block(WAIT);
        assertThat(seen).isEqualTo(GatewayRedisPort.Presence.withoutEjection(1, 0, 0, 1, 0, 1));
    }

    /**
     * <b>칸 수가 다르면 터뜨린다</b> (RD-11).
     *
     * <p>모자란 칸을 0 으로 메우면 표가 영영 0 이고 클러스터는 항상 닫힌 것으로
     * 보인다 — 기능이 조용히 꺼진 채 돌다가 다음 장애 때에야 드러난다. 롤백
     * 구간(새 코드 + 옛 스크립트)이 실제로 그 자리다.
     */
    @Test
    @DisplayName("칸_수가_다른_응답은_거절한다")
    void 칸_수가_다른_응답은_거절한다() {
        // 롤백 구간에서 옛 스크립트가 돌려주는 모양이다.
        assertThatThrownBy(() -> port.presence(List.of(3L, 1_700_000_000L, 1L)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("8 칸");
    }

    /** 칸 수가 맞으면 자리대로 읽는다. 순서를 바꾸면 여기가 빨개진다. */
    @Test
    @DisplayName("칸_수가_맞으면_자리대로_읽는다")
    void 칸_수가_맞으면_자리대로_읽는다() {
        GatewayRedisPort.Presence seen =
                port.presence(List.of(9L, 1_700_000_000L, 3L, 2L, 7L, 55L, 6L, 0L));

        assertThat(seen).isEqualTo(GatewayRedisPort.Presence.withoutEjection(9, 3, 2, 7, 55, 6));
    }

    /** 회복 봉우리를 정상과 견주려면 전 노드의 도착 합을 알아야 한다 (RC4). */
    @Test
    @DisplayName("노드별_통과_수를_합산해_돌려준다")
    void 노드별_통과_수를_합산해_돌려준다() {
        찍는다("gw-a", CircuitState.CLOSED, 30).block(WAIT);

        GatewayRedisPort.Presence seen = 찍는다("gw-b", CircuitState.CLOSED, 25).block(WAIT);

        assertThat(seen.passed()).isEqualTo(55);
    }

    /**
     * <b>스크립트가 못 내는 조합을 픽스처가 만들면 안 된다</b> (DS-2). 표를 낸
     * 수가 산 수보다 많은 상태로 배선을 재면 없는 클러스터를 짚는 시험이 된다.
     */
    @Test
    @DisplayName("산_수보다_많은_표는_못_만든다")
    void 산_수보다_많은_표는_못_만든다() {
        assertThatThrownBy(() -> GatewayRedisPort.Presence.withoutEjection(1, 0, 0, 2, 0, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 통과 수를 실은 수도 산 수를 못 넘는다. 넘으면 "모름" 판정이 영영 안 선다. */
    @Test
    @DisplayName("산_수보다_많이_실을_수_없다")
    void 산_수보다_많이_실을_수_없다() {
        assertThatThrownBy(() -> GatewayRedisPort.Presence.withoutEjection(1, 0, 0, 1, 30, 2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>거절이 심장을 멈추면 안 된다.</b> 스크립트는 범위를 벗어난 값에 오류를
     * 내고, 그러면 그 노드는 생존 표시조차 못 써 임계 뒤 분모에서 빠진다.
     */
    @Test
    @DisplayName("상한을_넘는_통과_수는_묶어_보낸다")
    void 상한을_넘는_통과_수는_묶어_보낸다() {
        GatewayRedisPort.Presence seen =
                찍는다("gw-a", CircuitState.CLOSED, 9_000_000_000L).block(WAIT);

        // 스크립트가 안 거절했다는 것과 묶인 값이 실렸다는 것을 같이 본다.
        assertThat(seen.passed()).isEqualTo(1_000_000_000);
        assertThat(seen.passReported()).isEqualTo(1);
    }

    /** 안 쟀으면 안 싣는다. 0 을 실으면 안 잰 노드가 0 을 잰 노드로 세어진다. */
    @Test
    @DisplayName("안_쟀으면_아무것도_안_싣는다")
    void 안_쟀으면_아무것도_안_싣는다() {
        assertThat(찍는다("gw-a", CircuitState.CLOSED, -1).block(WAIT).passReported())
                .as("안 잰 노드는 합에 기여하지 않는다").isZero();

        GatewayRedisPort.Presence 잰_영 = 찍는다("gw-a", CircuitState.CLOSED, 0).block(WAIT);

        assertThat(잰_영.passReported()).as("0 을 잰 것은 실린다").isEqualTo(1);
        assertThat(잰_영.passed()).isZero();
    }

    @Test
    @DisplayName("배제_표를_인스턴스별로_읽는다")
    void 배제_표를_인스턴스별로_읽는다() {
        GatewayRedisPort.Presence seen = port.presence(
                List.of(9L, 1_700_000_000L, 3L, 2L, 7L, 55L, 6L, 4L, "x", 3L, "y", 1L));

        assertThat(seen).isEqualTo(
                new GatewayRedisPort.Presence(9, 3, 2, 7, 55, 6, 4, Map.of("x", 3, "y", 1)));
    }

    @Test
    @DisplayName("꼬리가_홀수면_거절한다")
    void 꼬리가_홀수면_거절한다() {
        assertThatThrownBy(() -> port.presence(
                List.of(9L, 1_700_000_000L, 0L, 0L, 9L, 0L, 0L, 4L, "x")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("짝");
    }

    @Test
    @DisplayName("산_수보다_많은_배제_표는_못_만든다")
    void 산_수보다_많은_배제_표는_못_만든다() {
        assertThatThrownBy(() -> new GatewayRedisPort.Presence(1, 0, 0, 1, 0, 1, 2, Map.of()))
                .as("실은 수가 산 수를 넘는다").isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new GatewayRedisPort.Presence(3, 0, 0, 3, 0, 3, 2, Map.of("x", 3)))
                .as("표가 실은 수를 넘는다").isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new GatewayRedisPort.Presence(3, 0, 0, 3, 0, 3, 2, Map.of("x", 0)))
                .as("표가 0 인 인스턴스는 안 온다").isInstanceOf(IllegalArgumentException.class);
    }

    /** 이름순으로 보내야 상한에서 잘리는 것이 결정적이다. */
    @Test
    @DisplayName("배제_목록은_정렬해_쉼표로_감싸_보낸다")
    void 배제_목록은_정렬해_쉼표로_감싸_보낸다() {
        assertThat(port.ejectArg(List.of("b", "a", "b"))).isEqualTo(",a,b,");
        assertThat(port.ejectArg(List.of())).as("뺀 대가 없다는 보고").isEqualTo(",");
        assertThat(port.ejectArg(null)).as("안 실은 것").isEmpty();
    }

    /** 거절이 심장을 멈추면 안 된다. 스크립트가 거절할 값은 보내기 전에 걸러 낸다. */
    @Test
    @DisplayName("상한을_넘는_배제_목록은_잘라_보낸다")
    void 상한을_넘는_배제_목록은_잘라_보낸다() {
        List<String> ids = new ArrayList<>();
        for (int i = 39; i >= 0; i--) {
            ids.add(String.format("i%02d", i));
        }

        String sent = port.ejectArg(ids);

        assertThat(sent.split(",", -1)).hasSize(34);
        assertThat(sent).startsWith(",i00,i01,").endsWith(",i31,");
    }

    /** 자르면 다른 이름이 된다. 버린다. 스크립트가 거절할 이름을 보내면 그 노드의 생존 표시까지 막힌다. */
    @Test
    @DisplayName("스크립트가_거절할_이름은_빼고_보낸다")
    void 스크립트가_거절할_이름은_빼고_보낸다() {
        List<String> ids = new ArrayList<>(List.of("a,b", "x".repeat(65), "", "ok", "가", "a\u001bb"));
        ids.add(null);
        ids.add("y".repeat(64));

        assertThat(port.ejectArg(ids)).isEqualTo(",ok," + "y".repeat(64) + ",");
    }

    @Test
    @DisplayName("상한까지의_꼬리는_받고_넘으면_거절한다")
    void 상한까지의_꼬리는_받고_넘으면_거절한다() {
        assertThat(port.presence(꼬리(64)).ejectVotes()).hasSize(64);
        assertThatThrownBy(() -> port.presence(꼬리(65)))
                .isInstanceOf(IllegalStateException.class);
    }

    /** 머리 칸은 수여야 한다. 문자열을 받아 주면 스크립트가 머리를 바꿔도 조용히 통과한다. */
    @Test
    @DisplayName("머리_칸이_수가_아니면_거절한다")
    void 머리_칸이_수가_아니면_거절한다() {
        assertThatThrownBy(() -> port.presence(
                List.of("9", 1_700_000_000L, 0L, 0L, 9L, 0L, 0L, 0L)))
                .isInstanceOf(IllegalStateException.class);
    }

    /** 산 64 대가 저마다 다른 한 대씩 뺀 회차. */
    private List<Object> 꼬리(int pairs) {
        List<Object> raw = new ArrayList<>(
                List.of((long) pairs, 1_700_000_000L, 0L, 0L, (long) pairs, 0L, 0L, (long) pairs));
        for (int i = 0; i < pairs; i++) {
            raw.add("i" + i);
            raw.add(1L);
        }
        return raw;
    }

    @Test
    @DisplayName("배제_표를_실어_세어_받는다")
    void 배제_표를_실어_세어_받는다() {
        port.beat("gw-a", REAP_AFTER_SEC, VOTE_FRESH_SEC, CircuitState.CLOSED, 0, List.of("x", "y"))
                .block(WAIT);

        GatewayRedisPort.Presence seen = port.beat("gw-b", REAP_AFTER_SEC, VOTE_FRESH_SEC,
                CircuitState.CLOSED, 0, List.of("x")).block(WAIT);

        assertThat(seen.ejectReported()).isEqualTo(2);
        assertThat(seen.ejectVotes()).isEqualTo(Map.of("x", 2, "y", 1));
    }

    @Test
    @DisplayName("배제를_안_실으면_표가_없다")
    void 배제를_안_실으면_표가_없다() {
        port.beat("gw-a", REAP_AFTER_SEC, VOTE_FRESH_SEC, CircuitState.CLOSED, 0, List.of("x"))
                .block(WAIT);

        GatewayRedisPort.Presence seen = 찍는다("gw-a", CircuitState.CLOSED).block(WAIT);

        assertThat(seen.ejectReported()).isZero();
        assertThat(seen.ejectVotes()).isEmpty();
    }

    /** 포트가 보낸 이름을 스크립트가 받아야 한다. 두 문자 집합이 갈리면 하트비트가 실패한다. */
    @Test
    @DisplayName("구두점이_든_이름이_포트에서_스크립트까지_간다")
    void 구두점이_든_이름이_포트에서_스크립트까지_간다() {
        assertThat(port.ejectArg(List.of("a-b.c:1_2"))).isEqualTo(",a-b.c:1_2,");

        GatewayRedisPort.Presence seen = port.beat("gw-a", REAP_AFTER_SEC, VOTE_FRESH_SEC,
                CircuitState.CLOSED, 0, List.of("a-b.c:1_2", "10.0.1.7:8080")).block(WAIT);

        assertThat(seen.ejectVotes()).isEqualTo(Map.of("a-b.c:1_2", 1, "10.0.1.7:8080", 1));
    }
}

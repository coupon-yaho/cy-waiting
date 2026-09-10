package com.kafkick.waiting.chaos;

import com.kafkick.waiting.adapter.redis.AllocationRedisPort;
import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.QueueSweeper;
import com.kafkick.waiting.domain.allocation.Grant;
import io.lettuce.core.RedisURI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * C13a — 임기가 되감기고 울타리 표만 남는다.
 *
 * <p>무엇을 왜 재는지는 {@code plan/08-resilience.md} 의 C13a 절이 든다. 여기는
 * 그것을 어떻게 판정하는가만 든다.
 */
@Tag("chaos")
class TermRewindScenarioTest {

    private static final String COUPON = "c13a-rewind";

    private static final int SHARDS = 1;

    private static final int SHARD = 0;

    private static final Duration 기다림 = Duration.ofSeconds(10);

    /**
     * 운영 키를 안 쓴다. 다른 시험이 켜 놓은 스케줄러가 같은 락을 잡으러 오면
     * 임기 단언이 흔들린다. 획득 배선은 {@code LeaderElectionTest} 와 겹친다.
     */
    private static final String LEADER = "test:scheduler:leader";

    private static final String GEN = "{test:scheduler:leader}:gen";

    private static final String LEASE = "8000";

    /** 쿠폰별 표의 수명 하한. 운영값은 리스에 따라 이보다 길 수 있다. */
    private static final Duration 표_수명_하한 = Duration.ofHours(1);

    /**
     * 스냅샷 표의 수명 하한. 이 시나리오의 유일한 시한이다 — 한 회차가 이보다
     * 오래 걸리면 유령의 발행이 통과해 판정이 뒤집힌다.
     */
    private static final Duration 발행표_수명_하한 = Duration.ofSeconds(10);

    private static RedisFaults faults;

    private static LettuceConnectionFactory factory;

    private static ReactiveStringRedisTemplate redis;

    private static RedisScript<List> 획득;

    private AllocationRedisPort port;

    private long 옛_임기;
    private long 새_임기;
    private long 되감긴_세는값;
    private long 진입_입장표;
    private long 진입_삭제표;
    private long 진입_발행표;
    private QueueSweeper.SweepResult 유령_청소;
    private boolean 표없이_두드린_뒤_줄있음;
    private long 유실_입장표;
    private long 유실_세는값;
    private long 회복_입장표;
    private boolean 유령_적용_막힘;
    private boolean 유령_발행_막힘;
    private List<String> 유령_삭제;
    private List<String> 유령_후보;
    private List<String> 새_리더_후보;
    private long 유지_입장표;
    private long 유지_삭제표;
    private Map<String, String> 유지_스냅샷;
    private long 유지_임계;
    private long 유령_전_임계;
    private boolean 유령_뒤_줄있음;
    private List<String> 회복_지운쿠폰;
    private boolean 회복_줄있음;

    private long 옛_버전_번호;
    private long 롤백_입장표;
    private long 롤백_삭제표;
    private boolean 롤백_적용_통과;
    private boolean 롤백_삭제_막힘;
    private boolean 롤백_발행_막힘;
    private Duration 롤백_삭제표_수명;
    private Duration 롤백_발행표_수명;

    @BeforeAll
    static void 띄운다() {
        faults = RedisFaults.시작한다();
        RedisURI uri = RedisURI.create(faults.주소());
        factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(uri.getHost(), uri.getPort()),
                LettuceClientConfiguration.builder().commandTimeout(기다림).build());
        factory.afterPropertiesSet();
        redis = new ReactiveStringRedisTemplate(factory);
        획득 = RedisScript.of(new ClassPathResource("redis/leader_acquire.lua"), List.class);
    }

    @AfterAll
    static void 내린다() {
        factory.destroy();
        faults.close();
    }

    @BeforeEach
    void 비운다() {
        redis.delete(LEADER, GEN, RedisKeys.SNAPSHOT, RedisKeys.SNAPSHOT_FENCE,
                RedisKeys.queue(COUPON, SHARDS, SHARD),
                RedisKeys.alive(COUPON, SHARDS, SHARD),
                RedisKeys.admitted(COUPON, SHARDS, SHARD),
                RedisKeys.stock(COUPON),
                RedisKeys.applyFence(COUPON, SHARDS, SHARD),
                RedisKeys.dropFence(COUPON, SHARDS, SHARD)).block(기다림);
        // 회차마다 새로 만든다. 막힌 건수를 세는 값이 포트에 있어 공유하면 누적된다.
        port = AllocationRedisPort.of(redis, SHARDS);
    }

    /**
     * <b>유령 리더가 되감김에서 나온다.</b>
     *
     * <p>리더 슬롯만 복제본으로 넘어가면 락과 세는 값은 되감기고 쿠폰 슬롯의 표는
     * 남는다. 번호가 오르는지는 리더 시험이 재고, 여기는 그 번호가 문으로 서는지를 잰다.
     */
    @Test
    @DisplayName("C13a_임기가_되감겨도_유령_리더의_문이_닫힌다")
    void C13a_임기가_되감겨도_유령_리더의_문이_닫힌다() {
        ChaosScenario.named("C13a 임기 되감김")
                .baseline(() -> {
                    매진된_줄을_세운다();
                    옛_임기 = 임기("node-old");
                    port.sealFences(List.of(COUPON), 옛_임기).block(기다림);
                    port.publish(Map.of("v", "1"), 옛_임기).block(기다림);
                    진입_입장표 = 표(RedisKeys.applyFence(COUPON, SHARDS, SHARD));
                    진입_삭제표 = 표(RedisKeys.dropFence(COUPON, SHARDS, SHARD));
                    진입_발행표 = 표(RedisKeys.SNAPSHOT_FENCE);
                })
                .inject(() -> {
                    되감는다(옛_임기);
                    되감긴_세는값 = 표(GEN);
                    새_임기 = 임기("node-new");
                })
                .duringFault(() -> {
                    port.apply(new Grant(COUPON, 1), 새_임기).block(기다림);
                    port.publish(Map.of("v", "2"), 새_임기).block(기다림);
                    새_리더_후보 = port.claimSoldOutQueues(List.of(COUPON), 새_임기).block(기다림);
                    유령_전_임계 = 표(RedisKeys.admitted(COUPON, SHARDS, SHARD));
                    // **두 번 두드린다.** 유령은 매 틱 다시 오므로, 거절이 표를 자기
                    // 번호로 되감아 놓으면 둘째 회차가 통과한다.
                    유령이_두드린다();
                    유령이_두드린다();
                    유지_입장표 = 표(RedisKeys.applyFence(COUPON, SHARDS, SHARD));
                    유지_삭제표 = 표(RedisKeys.dropFence(COUPON, SHARDS, SHARD));
                    유지_스냅샷 = port.load().block(기다림);
                    유지_임계 = 표(RedisKeys.admitted(COUPON, SHARDS, SHARD));
                    유령_뒤_줄있음 = 줄이_있는가();
                })
                // 유령이 멎는 것 말고 걷을 장애가 없다. 되감김은 이미 지난 사건이다.
                .recover(() -> { })
                .afterRecovery(() -> {
                    회복_지운쿠폰 = port.dropSoldOutQueues(List.of(COUPON), 새_임기).block(기다림);
                    회복_줄있음 = 줄이_있는가();
                })
                .assertEntry(() -> RecoveryCriteria.violations(
                        표가_남았다("입장", 옛_임기, 진입_입장표),
                        표가_남았다("삭제", 옛_임기, 진입_삭제표),
                        표가_남았다("발행", 옛_임기, 진입_발행표),
                        같다("세는 값이 안 되감겼다", 옛_임기 - 1, 되감긴_세는값),
                        넘는다("입장", 새_임기, 진입_입장표),
                        넘는다("삭제", 새_임기, 진입_삭제표),
                        넘는다("발행", 새_임기, 진입_발행표)))
                .assertDuring(() -> RecoveryCriteria.violations(
                        막혔다("유령의 적용", 유령_적용_막힘),
                        막혔다("유령의 발행", 유령_발행_막힘),
                        막혔다("유령의 삭제", 유령_삭제.isEmpty()),
                        막혔다("유령의 후보 표시", 유령_후보.isEmpty()),
                        통과했다("새 리더의 후보 표시", List.of(COUPON).equals(새_리더_후보)),
                        // **거절이 표를 안 되감는다.** 되감기면 유령의 다음 회차가
                        // 통과한다 — 막혔다는 값과 지표만 보면 안 갈린다.
                        같다("입장 표가 되감겼다", 새_임기, 유지_입장표),
                        같다("삭제 표가 되감겼다", 새_임기, 유지_삭제표),
                        같다("막힌 적용이 세어졌다", 2, (long) port.applyFenced()),
                        같다("막힌 발행이 세어졌다", 2, (long) port.publishFenced()),
                        // 후보 표시의 거절은 아무 값도 안 올린다. 그 사실을 못 박는다.
                        같다("막힌 삭제가 세어졌다", 2, port.dropFenced()),
                        유령이_안_덮었다(유지_스냅샷),
                        같다("유령의 적용이 임계를 올렸다", 유령_전_임계, 유지_임계),
                        살아있다("유지 구간의 줄", 유령_뒤_줄있음)))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        지웠다(회복_지운쿠폰),
                        살아있다("회복 뒤의 줄", !회복_줄있음)))
                .run();
    }

    /**
     * <b>되돌리는 방향은 문마다 다르다.</b>
     *
     * <p>승계 잠금이 입장 표를 덮어쓰므로 적용은 그 자리에서 풀리고, 올리기만 하는
     * 삭제 표와 잠금이 없는 발행 표는 각자의 수명만큼 남는다.
     */
    @Test
    @DisplayName("C13a_롤백하면_적용은_풀리고_삭제와_발행이_남는다")
    void C13a_롤백하면_적용은_풀리고_삭제와_발행이_남는다() {
        ChaosScenario.named("C13a 옛 버전으로 되돌리기")
                .baseline(() -> {
                    매진된_줄을_세운다();
                    새_임기 = 임기("node-new");
                    port.sealFences(List.of(COUPON), 새_임기).block(기다림);
                    port.publish(Map.of("v", "1"), 새_임기).block(기다림);
                })
                .inject(() -> {
                    // 옛 버전 노드는 세는 값 없이 서버 마이크로초를 임기로 썼다.
                    // 프로덕션 순서 그대로 승계 잠금부터 돈다.
                    옛_버전_번호 = 옛_버전이_매기는_번호();
                    port.sealFences(List.of(COUPON), 옛_버전_번호).block(기다림);
                    롤백_입장표 = 표(RedisKeys.applyFence(COUPON, SHARDS, SHARD));
                    롤백_삭제표 = 표(RedisKeys.dropFence(COUPON, SHARDS, SHARD));
                })
                .duringFault(() -> {
                    롤백_적용_통과 = 통과하는가(() ->
                            port.apply(new Grant(COUPON, 1), 옛_버전_번호).block(기다림));
                    롤백_발행_막힘 = !통과하는가(() ->
                            port.publish(Map.of("v", "2"), 옛_버전_번호).block(기다림));
                    롤백_삭제_막힘 = port.dropSoldOutQueues(List.of(COUPON), 옛_버전_번호)
                            .block(기다림).isEmpty();
                    롤백_삭제표_수명 = 수명(RedisKeys.dropFence(COUPON, SHARDS, SHARD));
                    롤백_발행표_수명 = 수명(RedisKeys.SNAPSHOT_FENCE);
                })
                .recover(() -> { })
                .assertEntry(() -> RecoveryCriteria.violations(
                        // 승계 잠금은 덮어쓴다 — 락을 쥔 것이 권위다.
                        같다("입장 표가 안 내려갔다", 옛_버전_번호, 롤백_입장표),
                        같다("삭제 표가 내려갔다", 새_임기, 롤백_삭제표)))
                .assertDuring(() -> RecoveryCriteria.violations(
                        통과했다("옛 버전의 적용", 롤백_적용_통과),
                        막혔다("옛 버전의 삭제", 롤백_삭제_막힘),
                        막혔다("옛 버전의 발행", 롤백_발행_막힘),
                        // 막히는 길이가 곧 롤백 절차가 감당할 시간이다.
                        수명이_남았다("삭제 표", 표_수명_하한, 롤백_삭제표_수명),
                        수명이_남았다("발행 표", 발행표_수명_하한, 롤백_발행표_수명)))
                // 표가 만료될 때까지 기다리지 않는다. 유지 구간이 그 길이를 이미 쟀다.
                .assertRecovery(ChaosScenario.Verdict.none())
                .run();
    }

    @SuppressWarnings("unchecked")
    /**
     * <b>쿠폰 슬롯만 승격하면 그 슬롯의 표가 사라진다.</b> 리더 슬롯은 그대로라
     * 승계가 안 일어나고, 그래서 아무도 문을 다시 안 잠근다.
     */
    @Test
    @DisplayName("C13b_쿠폰_슬롯만_승격하면_아무도_문을_다시_안_잠근다")
    void C13b_쿠폰_슬롯만_승격하면_아무도_문을_다시_안_잠근다() {
        ChaosScenario.named("C13b 울타리 표 유실")
                .baseline(() -> {
                    매진된_줄을_세운다();
                    새_임기 = 임기("node-new");
                    port.sealFences(List.of(COUPON), 새_임기).block(기다림);
                    port.publish(Map.of("v", "1"), 새_임기).block(기다림);
                    옛_임기 = 새_임기 - 1;
                    진입_입장표 = 표(RedisKeys.applyFence(COUPON, SHARDS, SHARD));
                    진입_삭제표 = 표(RedisKeys.dropFence(COUPON, SHARDS, SHARD));
                })
                .inject(() -> {
                    // 쿠폰 슬롯만 옛 복제본으로 넘어간 모양. 리더 슬롯은 그대로다.
                    redis.delete(RedisKeys.applyFence(COUPON, SHARDS, SHARD),
                            RedisKeys.dropFence(COUPON, SHARDS, SHARD)).block(기다림);
                    유실_입장표 = 표(RedisKeys.applyFence(COUPON, SHARDS, SHARD));
                    유실_세는값 = 표(GEN);
                })
                .duringFault(() -> {
                    유령이_두드린다();
                    유지_입장표 = 표(RedisKeys.applyFence(COUPON, SHARDS, SHARD));
                    유지_삭제표 = 표(RedisKeys.dropFence(COUPON, SHARDS, SHARD));
                    유령_청소 = port.sweep(List.of(COUPON), 1_800_000_000L, 100, 300, 100,
                            옛_임기).block(기다림);
                    // **두 번 두드린다.** 첫 회차의 거절이 표를 유령 번호로 세우므로,
                    // 둘째 회차에는 그 표가 자기 번호와 같아 삭제가 통과한다.
                    유령이_두드린다();
                    표없이_두드린_뒤_줄있음 = 줄이_있는가();
                })
                // **다시 잠그는 것은 승계뿐이다.** 쿠폰 슬롯만 넘어가면 승계가 안 도므로,
                // 회복을 손으로 부르는 것이 곧 "무엇이 잠그는가" 의 답이다.
                .recover(() -> port.sealFences(List.of(COUPON), 새_임기).block(기다림))
                .afterRecovery(() -> {
                    유령이_두드린다();
                    회복_입장표 = 표(RedisKeys.applyFence(COUPON, SHARDS, SHARD));
                })
                .assertEntry(() -> RecoveryCriteria.violations(
                        같다("표가 안 섰다", 새_임기, 진입_입장표),
                        같다("삭제 표가 안 섰다", 새_임기, 진입_삭제표),
                        같다("표가 안 사라졌다", 0, 유실_입장표),
                        // 리더 슬롯은 멀쩡하다 — 승계가 안 도는 것이 이 시나리오의 전제다.
                        같다("세는 값까지 사라졌다", 새_임기, 유실_세는값)))
                .assertDuring(() -> RecoveryCriteria.violations(
                        // **표가 없으면 어떤 임기든 통과한다.** 이것이 이 시나리오가
                        // 드러내려는 구멍이고, 지금은 그것을 사실로 못 박는다.
                        통과했다("표가 없는 동안 유령의 적용", !유령_적용_막힘),
                        // 유령이 먼저 두드리면 표가 자기 옛 번호로 선다.
                        같다("표가 유령 번호로 안 섰다", 옛_임기, 유지_입장표),
                        같다("삭제 표가 유령 번호로 안 섰다", 옛_임기, 유지_삭제표),
                        // 청소의 울타리도 비교 대상이 없어 그대로 통과한다.
                        같다("유령의 청소가 막혔다", 0, 유령_청소.fenced()),
                        // **되돌릴 수 없는 쪽까지 열린다.** 첫 회차가 세운 표를 딛고
                        // 둘째 회차의 삭제가 줄을 지운다 — 되살리는 코드가 없다.
                        살아있다("둘째 회차 뒤의 줄", !표없이_두드린_뒤_줄있음)))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        같다("승계 잠금이 표를 안 올렸다", 새_임기, 회복_입장표),
                        막혔다("잠근 뒤 유령의 적용", 유령_적용_막힘)))
                .run();
    }

    private List<Object> 잡는다(String owner) {
        return (List<Object>) redis.execute(획득, List.of(LEADER, GEN),
                List.of(owner, LEASE)).blockFirst(기다림);
    }

    /** 스크립트 반환의 자리 셋은 {@code LeaderElectionTest} 도 안다 — 형식이 두 곳이다. */
    private long 임기(String owner) {
        List<Object> r = 잡는다(owner);
        if (Long.parseLong(String.valueOf(r.get(0))) != 1) {
            throw new IllegalStateException("리더를 못 잡았다: " + r.get(1));
        }
        return Long.parseLong(String.valueOf(r.get(3)));
    }

    /**
     * 복제본 승격 흉내. 리더 슬롯만 되감고 쿠폰 슬롯의 표는 그대로 둔다.
     *
     * <p>세는 값을 <b>직전 번호</b>로 두는 것은 시계 바닥이 없는 세상에서 승격이
     * 실제로 남기는 값이라서다. 바닥이 있으면 그 값이 안 나오는데, 바닥이 서 있는지를
     * 재려면 바닥이 없을 때의 값을 넣어 봐야 한다.
     */
    private void 되감는다(long 임기) {
        redis.delete(LEADER).block(기다림);
        redis.opsForValue().set(GEN, Long.toString(임기 - 1)).block(기다림);
    }

    private void 유령이_두드린다() {
        유령_적용_막힘 = !통과하는가(() -> port.apply(new Grant(COUPON, 1), 옛_임기).block(기다림));
        유령_발행_막힘 = !통과하는가(() ->
                port.publish(Map.of("v", "유령"), 옛_임기).block(기다림));
        유령_삭제 = port.dropSoldOutQueues(List.of(COUPON), 옛_임기).block(기다림);
        유령_후보 = port.claimSoldOutQueues(List.of(COUPON), 옛_임기).block(기다림);
    }

    private boolean 통과하는가(Runnable 호출) {
        try {
            호출.run();
            return true;
        } catch (AllocationRedisPort.FencedOutException e) {
            return false;
        }
    }

    /** 옛 버전 리더가 매기던 번호. 밀리초로 받아 곱하므로 실제보다 조금 뒤처진다. */
    private long 옛_버전이_매기는_번호() {
        Long millis = redis.execute(connection -> connection.serverCommands().time())
                .blockFirst(기다림);
        return millis * 1000L;
    }

    private long 표(String key) {
        String raw = redis.opsForValue().get(key).block(기다림);
        return raw == null ? 0 : Long.parseLong(raw);
    }

    private Duration 수명(String key) {
        return redis.getExpire(key).block(기다림);
    }

    private boolean 줄이_있는가() {
        return Boolean.TRUE.equals(
                redis.hasKey(RedisKeys.queue(COUPON, SHARDS, SHARD)).block(기다림));
    }

    private void 매진된_줄을_세운다() {
        redis.opsForZSet().add(RedisKeys.queue(COUPON, SHARDS, SHARD), "m1", 1).block(기다림);
        redis.opsForValue().set(RedisKeys.stock(COUPON), "0").block(기다림);
    }

    private static Optional<String> 표가_남았다(String 무엇, long 임기, long 표) {
        return 임기 == 표 ? Optional.empty()
                : Optional.of("%s 표가 안 남았다 — 넣은 임기 %d, 읽은 표 %d"
                        .formatted(무엇, 임기, 표));
    }

    private static Optional<String> 넘는다(String 무엇, long 임기, long 표) {
        return 임기 > 표 ? Optional.empty()
                : Optional.of("%s 표를 못 넘었다 — 새 임기 %d, 남은 표 %d"
                        .formatted(무엇, 임기, 표));
    }

    private static Optional<String> 같다(String 무엇, long 기대, long 실제) {
        return 기대 == 실제 ? Optional.empty()
                : Optional.of("%s — 기대 %d, 실제 %d".formatted(무엇, 기대, 실제));
    }

    private static Optional<String> 막혔다(String 무엇, boolean 막힘) {
        return 막힘 ? Optional.empty() : Optional.of(무엇 + "이 통과했다");
    }

    private static Optional<String> 통과했다(String 무엇, boolean 통과) {
        return 통과 ? Optional.empty() : Optional.of(무엇 + "이 막혔다");
    }

    private static Optional<String> 살아있다(String 무엇, boolean 산다) {
        return 산다 ? Optional.empty() : Optional.of(무엇 + "이 사라졌다");
    }

    private static Optional<String> 유령이_안_덮었다(Map<String, String> 스냅샷) {
        return Map.of("v", "2").equals(스냅샷) ? Optional.empty()
                : Optional.of("유령의 재료가 실렸다 — " + 스냅샷);
    }

    private static Optional<String> 지웠다(List<String> 지운쿠폰) {
        return List.of(COUPON).equals(지운쿠폰) ? Optional.empty()
                : Optional.of("새 임기의 삭제가 안 나갔다 — " + 지운쿠폰);
    }

    private static Optional<String> 수명이_남았다(String 무엇, Duration 하한, Duration 남은) {
        if (남은 != null && 남은.compareTo(하한) <= 0
                && 남은.toMillis() > 하한.toMillis() * 9 / 10) {
            return Optional.empty();
        }
        return Optional.of("%s 의 남은 수명이 하한과 다르다 — 하한 %s, 남은 %s"
                .formatted(무엇, 하한, 남은));
    }
}

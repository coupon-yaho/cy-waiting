package com.kafkick.waiting.adapter.redis;

import com.kafkick.waiting.control.CapacityReport;
import com.kafkick.waiting.control.CapacitySample;
import com.kafkick.waiting.control.TimedCoupons;
import com.kafkick.waiting.control.TimedSnapshot;
import com.kafkick.waiting.control.ControlPlaneProperties;
import com.kafkick.waiting.control.FailureWindow;
import com.kafkick.waiting.control.SnapshotSource;
import com.kafkick.waiting.domain.allocation.Grant;
import com.kafkick.waiting.domain.coupon.QueueMode;
import com.kafkick.waiting.control.QueueSweeper;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 스케줄러가 레디스에 내는 명령.
 *
 * <p>수요 수집은 <b>Lua 가 아니다.</b> 쿠폰마다 슬롯이 갈려 클러스터에서 못 돈다.
 * 재고도 샤드 무관 키라 같은 스크립트에서 못 읽는다. 잃는 것은 진단 편의뿐이다.
 */
@Component
public final class AllocationRedisPort implements SnapshotSource {

    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> APPLY =
            RedisScript.of(new ClassPathResource("redis/allocation_apply.lua"), List.class);

    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> PUBLISH =
            RedisScript.of(new ClassPathResource("redis/snapshot_publish.lua"), List.class);

    /** 보고와 기준 시각을 <b>같은 노드에서 같은 순간에</b> 읽는다. */
    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> CAPACITY_READ =
            RedisScript.of(new ClassPathResource("redis/capacity_read.lua"), List.class);

    /** 재료와 그것을 잰 시각을 같이 읽는다. 나이를 한 시계로 재려는 것이다. */
    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> SNAPSHOT_READ =
            RedisScript.of(new ClassPathResource("redis/snapshot_read.lua"), List.class);

    /** 배분 대상과 그것을 읽은 시각. 발행 시각이 여기서 나온다. */
    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> ACTIVE_READ =
            RedisScript.of(new ClassPathResource("redis/active_read.lua"), List.class);

    /**
     * 스크립트에 한 번에 넘기는 인자 상한.
     *
     * <p>루아 스택이 넘치면 그 판이 통째로 실패한다. 쿠폰이 그만큼 많아지면
     * 나눠 실어야 하는데, 그러면 원자성이 깨지므로 <b>그때는 설계를 다시 본다.</b>
     */
    private static final int MAX_PUBLISH_FIELDS = 3_000;

    /** 한 판이 동시에 낼 수 있는 읽기. 무제한이면 한 판이 커넥션을 독점한다. */
    private static final int MAX_CONCURRENT_READS = 16;

    /** 값이 JSON 인 것은 계약이다 — 위치 기반 문자열은 필드가 늘면 깨진다 (D-C3). */
    private static final ObjectMapper JSON = new ObjectMapper();

    private static final Logger log = LoggerFactory.getLogger(AllocationRedisPort.class);

    private final ReactiveStringRedisTemplate redis;
    private static final RedisScript<List> SWEEP =
            RedisScript.of(new ClassPathResource("redis/sweep.lua"), List.class);

    /**
     * 쿠폰별 HSCAN 커서.
     *
     * <p><b>이어 가야 한다.</b> 매번 0 에서 시작하면 해시 앞쪽만 계속 훑고
     * 뒤쪽 기록은 영영 안 지워져, 유예 해시가 한 방향으로만 자란다.
     */
    private final Map<String, String> sweepCursors = new ConcurrentHashMap<>();

    private final int shards;
    private final FailureWindow rejected = FailureWindow.create();
    private final FailureWindow malformed = FailureWindow.create();
    private final FailureWindow badPolicy = FailureWindow.create();
    /** 신선도의 기준 시각. 뒤로 가는 것을 여기서 막는다 (A-9). */
    private final ServerClock serverClock = ServerClock.create();

    /** 마지막으로 성공한 정책 판. 읽기가 실패하면 여기로 되돌아간다. */
    private final AtomicReference<Map<String, QueueMode>> lastModes =
            new AtomicReference<>(Map.of());

    /**
     * <b>모든 노드가 쓴다.</b> 배분은 리더만 돌지만 판정 재료를 받아 오는 것은
     * 전 노드가 하므로, 배분 토글 뒤에 두면 요청만 받는 노드가 재료를 못 받는다.
     */
    @Autowired
    AllocationRedisPort(ReactiveStringRedisTemplate redis, ControlPlaneProperties properties) {
        this(redis, properties.scheduler().shards());
    }

    private AllocationRedisPort(ReactiveStringRedisTemplate redis, int shards) {
        if (shards < 1) {
            throw new IllegalArgumentException("shards 는 1 이상이어야 한다: %d".formatted(shards));
        }
        this.redis = Objects.requireNonNull(redis, "redis 는 필수다");
        this.shards = shards;
    }

    public static AllocationRedisPort of(ReactiveStringRedisTemplate redis, int shards) {
        return new AllocationRedisPort(redis, shards);
    }

    /** 시계가 뒤로 간 사실을 남긴다. 조용히 보정하면 왜 그랬는지를 영영 못 밝힌다. */
    public ClockSkewTracker clockSkew() {
        return serverClock.skew();
    }

    /**
     * 뒷단이 스스로 적어 둔 여유를 읽는다.
     *
     * <p><b>밖에서 쓰는 키라 아무 값이나 들어온다.</b> 깨진 값 하나가 판을 죽이면
     * 멀쩡한 인스턴스 몫까지 사라져 전역 크레딧이 하한으로 떨어진다 (4.4.6).
     */
    public Mono<CapacitySample> capacitySample() {
        AtomicBoolean dropped = new AtomicBoolean();
        return redis.execute(CAPACITY_READ, List.of(RedisKeys.CAPACITY))
                .next()
                .map(raw -> readSample(raw, dropped))
                .doOnNext(sample -> {
                    if (!dropped.get()) {
                        malformed.exited().ifPresent(recovered ->
                                log.info("가용량 보고가 다시 깨끗하다 — {}초 만에, 그동안 {}건 걸렀다",
                                        recovered.elapsedSeconds(), recovered.swallowed()));
                    }
                })
                // **전부 버렸으면 그건 관측이 아니다.** 빈 목록을 내려보내면 부르는
                // 쪽이 "신선한 보고 0건" 으로 읽어 하한으로 떨어뜨린다. 형식이
                // 어긋나 전멸한 것과 뒷단이 정말 하나도 없는 것은 다르다.
                .flatMap(sample -> sample.reports().isEmpty() && dropped.get()
                        ? Mono.error(new IllegalStateException("가용량 보고를 전부 걸렀다"))
                        : Mono.just(sample));
    }

    /** 스크립트가 돌려준 {@code {now, field, value, ...}} 를 읽는다. */
    private CapacitySample readSample(List<Object> raw, AtomicBoolean dropped) {
        // **원시 값을 싣는다.** 단조 바닥값은 프로세스 전체의 최댓값이라, 다른
        // 슬롯의 앞선 시계가 여기 나이에 실리면 보고가 전부 낡음이 된다.
        long now = Long.parseLong(String.valueOf(raw.get(0)));
        serverClock.observe(now);
        List<CapacityReport> reports = new ArrayList<>();
        for (int i = 1; i + 1 < raw.size(); i += 2) {
            CapacityReport report = parse(String.valueOf(raw.get(i)),
                    String.valueOf(raw.get(i + 1)), dropped);
            if (report != null) {
                reports.add(report);
            }
        }
        return new CapacitySample(reports, now);
    }

    /**
     * 보고 하나를 읽는다. <b>없는 필드를 0 으로 접지 않는다</b> — 그러면 그
     * 인스턴스가 죽은 것처럼 보여 전역 크레딧이 조용히 줄어든다.
     */
    private CapacityReport parse(String instanceId, String value, AtomicBoolean dropped) {
        try {
            JsonNode node = JSON.readTree(value);
            JsonNode credits = node.get("credits");
            JsonNode ts = node.get("ts");
            if (credits == null || !credits.canConvertToLong()
                    || ts == null || !ts.canConvertToLong()) {
                return drop(instanceId, "필드가 없거나 수가 아니다", dropped);
            }
            return new CapacityReport(instanceId, credits.longValue(), ts.longValue());
        } catch (JacksonException e) {
            return drop(instanceId, e.getMessage(), dropped);
        }
    }

    private CapacityReport drop(String instanceId, String why, AtomicBoolean dropped) {
        dropped.set(true);
        // **구간의 첫 건만 남긴다.** 뒷단이 깨진 값을 계속 쓰면 매 틱 같은 줄이
        // 쌓이고, 그때 정작 봐야 할 것이 묻힌다.
        if (malformed.entered()) {
            log.warn("가용량 보고를 걸렀다 — {}: {}. 이 인스턴스 몫은 안 센다", instanceId, why);
        }
        return null;
    }

    /**
     * 목록에 없는 쿠폰은 보지 않는다. 끝난 쿠폰까지 보면 매 틱 왕복만 늘어난다.
     *
     * <p><b>밖에서 쓰는 키라 아무 값이나 들어온다.</b> 키에 못 쓰는 멤버 하나가
     * 판을 죽이면 멀쩡한 쿠폰 전부의 배분이 멎는데, 사람이 목록을 고치기 전에는
     * 안 풀린다. 그래서 걸러 내되 걸러 냈다는 사실을 남긴다.
     */
    /**
     * 배분 대상과 <b>그것을 읽은 레디스 시각</b>.
     *
     * <p>발행 시각을 리더 벽시계로 찍으면 같은 스냅샷이 노드마다 다르게 낡는다.
     */
    public Mono<TimedCoupons> activeCouponsTimed() {
        AtomicBoolean dropped = new AtomicBoolean();
        return redis.execute(ACTIVE_READ, List.of(RedisKeys.ACTIVE_COUPONS))
                .next()
                .map(raw -> {
                    List<?> parts = (List<?>) raw;
                    // **원시 값을 그대로 싣는다.** 단조 바닥값은 프로세스 전체의
                    // 최댓값이라, 다른 슬롯을 먼저 읽으면 이 읽기와 시각의 짝이
                    // 깨진다. 가드는 말이 되는 값인지 보고 역행을 남기는 몫이다.
                    long now = Long.parseLong(String.valueOf(parts.get(0)));
                    serverClock.observe(now);
                    List<String> coupons = new ArrayList<>();
                    for (int i = 1; i < parts.size(); i++) {
                        String couponId = String.valueOf(parts.get(i));
                        if (usable(couponId, dropped)) {
                            coupons.add(couponId);
                        }
                    }
                    coupons.sort(String::compareTo);
                    return new TimedCoupons(coupons, now);
                })
                .doOnNext(read -> {
                    if (!dropped.get()) {
                        rejected.exited().ifPresent(recovered ->
                                log.info("배분 대상이 다시 깨끗하다 — {}초 만에, 그동안 {}건 걸렀다",
                                        recovered.elapsedSeconds(), recovered.swallowed()));
                    }
                });
    }

    public Mono<List<String>> activeCoupons() {
        AtomicBoolean dropped = new AtomicBoolean();
        return redis.opsForSet().members(RedisKeys.ACTIVE_COUPONS)
                .filter(couponId -> usable(couponId, dropped))
                .sort()
                .collectList()
                // 걸러 낼 것이 없어진 판에서 창을 닫는다. 안 닫으면 나중에 다른
                // 오염이 들어와도 아무 신호가 안 나온다.
                .doOnNext(coupons -> {
                    if (!dropped.get()) {
                        rejected.exited().ifPresent(recovered ->
                                log.info("배분 대상이 다시 깨끗하다 — {}초 만에, 그동안 {}건 걸렀다",
                                        recovered.elapsedSeconds(), recovered.swallowed()));
                    }
                });
    }

    /**
     * 운영자가 정한 쿠폰별 대기열 정책.
     *
     * <p><b>밖에서 쓰는 키다.</b> 못 읽는 값이 하나 있다고 판을 죽이면 운영자의
     * 오타가 전 쿠폰의 배분을 멈춘다. 그 쿠폰만 기본값(적응형)으로 두고 남긴다.
     */
    public Mono<Map<String, QueueMode>> queueModes(List<String> couponIds) {
        if (couponIds.isEmpty()) {
            return Mono.just(Map.of());
        }
        AtomicBoolean dropped = new AtomicBoolean();
        // **활성 쿠폰만 묻는다.** 정책 해시에는 TTL 도 청소도 없어 끝난 쿠폰이
        // 쌓인다. 통째로 받으면 매 틱 그 전부를 파싱하고 몇 개만 쓴다.
        return redis.<String, String>opsForHash()
                .multiGet(RedisKeys.COUPON_POLICY, couponIds)
                .map(values -> {
                    Map<String, QueueMode> byCoupon = new LinkedHashMap<>();
                    for (int i = 0; i < couponIds.size(); i++) {
                        QueueMode mode = parseMode(couponIds.get(i), values.get(i), dropped);
                        if (mode != null) {
                            byCoupon.put(couponIds.get(i), mode);
                        }
                    }
                    return byCoupon;
                })
                .doOnNext(modes -> {
                    if (!dropped.get()) {
                        badPolicy.exited().ifPresent(recovered ->
                                log.info("쿠폰 정책이 다시 깨끗하다 — {}초 만에, 그동안 {}건 걸렀다",
                                        recovered.elapsedSeconds(), recovered.swallowed()));
                    }
                })
                // **물어본 쿠폰만 갱신한다.** 판마다 통째로 갈아치우면, 이번 판에
                // 안 물어본 쿠폰의 정책이 캐시에서 사라진다 — 그 쿠폰이 다음 판에
                // 돌아왔을 때 읽기가 실패하면 ALWAYS 가 조용히 적응형이 된다.
                .doOnNext(modes -> remember(couponIds, modes))
                // **정책은 부가 정보다.** 이것 하나 때문에 판이 죽으면 대기 수와
                // 재고가 멀쩡해도 스냅샷이 안 나간다. 빈 판으로 접으면 전원이
                // 적응형이 되어 ALWAYS 가 조용히 풀리므로, 직전 값을 다시 쓴다.
                .onErrorResume(e -> {
                    if (badPolicy.entered()) {
                        log.warn("쿠폰 정책을 못 읽는다 — 직전 값으로 돈다: {}", e.getMessage());
                    }
                    return Mono.just(recalled(couponIds));
                });
    }

    /** 물어본 쿠폰의 자리만 덮는다. 정책이 없어진 쿠폰은 그 자리를 비운다. */
    private void remember(List<String> couponIds, Map<String, QueueMode> modes) {
        lastModes.updateAndGet(prev -> {
            Map<String, QueueMode> merged = new LinkedHashMap<>(prev);
            couponIds.forEach(merged::remove);
            merged.putAll(modes);
            return Map.copyOf(merged);
        });
    }

    /** 직전 값 중 이번에 물어본 것만. 안 물어본 쿠폰을 끼워 주면 그건 관측이 아니다. */
    private Map<String, QueueMode> recalled(List<String> couponIds) {
        Map<String, QueueMode> known = lastModes.get();
        Map<String, QueueMode> subset = new LinkedHashMap<>();
        couponIds.forEach(couponId -> {
            QueueMode mode = known.get(couponId);
            if (mode != null) {
                subset.put(couponId, mode);
            }
        });
        return subset;
    }

    private QueueMode parseMode(String couponId, String value, AtomicBoolean dropped) {
        // 정책을 안 건 쿠폰이다. 없는 것은 고장이 아니라 기본값이다.
        if (value == null) {
            return null;
        }
        try {
            JsonNode node = JSON.readTree(value);
            JsonNode mode = node.get("mode");
            if (mode == null || !mode.isTextual()) {
                // **가장 흔한 오타가 여기다.** 필드 이름을 틀리거나 문자열만
                // 넣으면 예외가 안 나므로, 안 남기면 운영자가 걸었다고 믿는
                // 정책이 조용히 적응형이 된다.
                return dropPolicy(couponId, "mode 가 문자열이 아니다", dropped);
            }
            return QueueMode.valueOf(mode.asString().toUpperCase(Locale.ROOT));
        } catch (JacksonException | IllegalArgumentException e) {
            return dropPolicy(couponId, e.getMessage(), dropped);
        }
    }

    private QueueMode dropPolicy(String couponId, String why, AtomicBoolean dropped) {
        dropped.set(true);
        if (badPolicy.entered()) {
            log.warn("쿠폰 정책을 못 읽는다 — 그 쿠폰만 기본값으로 둔다: {} ({})", couponId, why);
        }
        return null;
    }

    private boolean usable(String couponId, AtomicBoolean dropped) {
        try {
            RedisKeys.queue(couponId, shards, 0);
            return true;
        } catch (IllegalArgumentException e) {
            dropped.set(true);
            if (rejected.entered()) {
                log.warn("배분 대상에 키로 못 쓰는 값이 있다 — 그것만 빼고 돈다: {}", e.getMessage());
            }
            return false;
        }
    }

    /**
     * 쿠폰별 대기 수. 샤드를 합친다.
     *
     * <p><b>위치가 아니라 쿠폰으로 짝짓는다.</b> 위치로 맞추면 응답이 한 칸만
     * 밀려도 A 의 대기가 B 의 재고와 붙는데, 그 조합은 도메인이 안 막으므로
     * <b>조용히 틀린 배분</b>이 나간다.
     */
    public Mono<Map<String, Long>> queueSizes(List<String> couponIds) {
        // **쿠폰마다 순서대로 왕복하면 틱이 밀린다.** 결과를 쿠폰으로 짝지으므로
        // 순서는 아무 뜻이 없다. 다만 무제한으로 풀면 한 판이 커넥션을 독점해
        // 다른 명령이 뒤로 밀리므로 동시성에 상한을 둔다.
        return Flux.fromIterable(couponIds)
                .flatMap(couponId -> shardSizes(couponId)
                        .map(size -> Map.entry(couponId, size)), MAX_CONCURRENT_READS)
                .collectMap(Map.Entry::getKey, Map.Entry::getValue);
    }

    private Mono<Long> shardSizes(String couponId) {
        List<String> keys = new ArrayList<>(shards);
        for (int shard = 0; shard < shards; shard++) {
            keys.add(RedisKeys.queue(couponId, shards, shard));
        }
        return Flux.fromIterable(keys)
                .flatMap(key -> redis.opsForZSet().size(key).defaultIfEmpty(0L))
                .reduce(0L, Long::sum);
    }

    /**
     * 이탈자를 걷어 낸다 (7.4).
     *
     * <p><b>커서를 쿠폰별로 이어 간다.</b> 매번 0 에서 시작하면 해시 앞쪽만
     * 계속 훑고 뒤쪽 기록은 영영 안 지워진다.
     */
    public Mono<QueueSweeper.SweepResult> sweep(List<String> couponIds, long nowSec,
            int scanLimit, long graceSec, int budget) {
        if (couponIds.isEmpty()) {
            return Mono.just(QueueSweeper.SweepResult.NOTHING);
        }
        return Flux.fromIterable(couponIds)
                .flatMap(id -> sweepOne(id, nowSec, scanLimit, graceSec, budget)
                        // **한 쿠폰이 실패해도 나머지는 쓴다.** 청소가 배분을
                        // 막으면 안 걷힌 것 하나가 그 틱 전체를 세운다.
                        .onErrorResume(e -> {
                            log.warn("이탈자 청소 실패 — 다음 틱에 다시 한다: 쿠폰={} {}",
                                    id, e.toString());
                            return Mono.just(QueueSweeper.SweepResult.NOTHING);
                        }), MAX_CONCURRENT_READS)
                .reduce(QueueSweeper.SweepResult.NOTHING, (a, b) -> new QueueSweeper.SweepResult(
                        a.swept() + b.swept(),
                        a.expiredSignals() + b.expiredSignals(),
                        a.expiredGrace() + b.expiredGrace()));
    }

    private Mono<QueueSweeper.SweepResult> sweepOne(String couponId, long nowSec,
            int scanLimit, long graceSec, int budget) {
        String cursor = sweepCursors.getOrDefault(couponId, "0");
        return redis.execute(SWEEP,
                        List.of(RedisKeys.queue(couponId, shards, 0),
                                RedisKeys.grace(couponId, shards, 0),
                                RedisKeys.alive(couponId, shards, 0)),
                        List.of(Integer.toString(scanLimit), Long.toString(nowSec),
                                Long.toString(graceSec), Integer.toString(budget), cursor))
                .next()
                .switchIfEmpty(Mono.error(new IllegalStateException("청소 결과가 비었다")))
                .map(raw -> {
                    List<?> values = (List<?>) raw;
                    sweepCursors.put(couponId, String.valueOf(values.get(3)));
                    return new QueueSweeper.SweepResult(toLongOrZero(values.get(0)),
                            toLongOrZero(values.get(1)), toLongOrZero(values.get(2)));
                });
    }

    private long toLongOrZero(Object value) {
        return value instanceof Number n ? n.longValue() : 0;
    }

    /** 쿠폰별 재고. <b>없으면 담지 않는다</b> — 부르는 쪽이 "모른다" 를 0 으로 접는다. */
    public Mono<Map<String, Long>> stocks(List<String> couponIds) {
        List<String> keys = couponIds.stream().map(RedisKeys::stock).toList();
        return redis.opsForValue().multiGet(keys).map(values -> {
            Map<String, Long> byCoupon = new LinkedHashMap<>();
            for (int i = 0; i < couponIds.size(); i++) {
                Long stock = toLong(values.get(i));
                if (stock != null) {
                    byCoupon.put(couponIds.get(i), stock);
                }
            }
            return byCoupon;
        });
    }

    /**
     * 들어온 인원을 돌려준다. 나눠 준 몫과 다르다 — 큐가 짧으면 남는다.
     *
     * <p><b>샤드가 하나인 동안만 옳다.</b> 여럿이면 몫을 샤드에 나눠 각각
     * 적용해야 하는데, 지금은 0번에만 나간다. 그래서 기동에서 하나로 막는다.
     */
    public Mono<Long> apply(Grant grant) {
        return redis.execute(APPLY,
                        List.of(RedisKeys.queue(grant.couponId(), shards, 0),
                                RedisKeys.admitted(grant.couponId(), shards, 0)),
                        List.of(Long.toString(grant.credit())))
                .next()
                .map(result -> Long.parseLong(String.valueOf(((List<?>) result).get(1))));
    }

    /**
     * 운영자가 적은 값을 읽습니다 (P-1).
     *
     * <p><b>리더만 읽습니다.</b> 전 노드가 매 틱 읽으면 그 자체가 요청 경로 밖의
     * 부하이고, 노드마다 다른 값을 볼 수 있습니다 — 스냅샷으로 퍼뜨리는 이유입니다.
     */
    public Mono<String> readTunables() {
        return redis.opsForValue().get(RedisKeys.TUNABLES);
    }

    /**
     * <b>통째로 갈아 끼우되 사이가 벌어지지 않게 한다.</b>
     *
     * <p>지우고 쓰는 것을 나눠 치면 그 사이에 끊길 때 키가 없는 채로 남고,
     * 전 노드가 판정 재료를 잃는다. 근거는 스크립트 주석에 있다.
     */
    public Mono<Void> publish(Map<String, String> hash) {
        if (hash.isEmpty()) {
            return Mono.error(new IllegalArgumentException("빈 스냅샷은 발행하지 않는다"));
        }
        if (hash.size() > MAX_PUBLISH_FIELDS) {
            return Mono.error(new IllegalStateException(
                    "한 번에 실을 수 있는 필드를 넘었다: %d > %d"
                            .formatted(hash.size(), MAX_PUBLISH_FIELDS)));
        }
        List<String> args = new ArrayList<>(hash.size() * 2);
        hash.forEach((field, value) -> {
            args.add(field);
            args.add(value);
        });
        return redis.execute(PUBLISH, List.of(RedisKeys.SNAPSHOT), args).next().then();
    }

    /**
     * 재료와 그것을 잰 시각을 <b>한 번에</b> 읽는다.
     *
     * <p>나이를 두 벽시계의 차로 재면 같은 스냅샷이 노드마다 다르게 낡는다.
     */
    public Mono<TimedSnapshot> loadTimed() {
        return redis.execute(SNAPSHOT_READ, List.of(RedisKeys.SNAPSHOT))
                .next()
                .map(raw -> {
                    List<?> parts = (List<?>) raw;
                    long now = Long.parseLong(String.valueOf(parts.get(0)));
                    serverClock.observe(now);
                    Map<String, String> hash = new LinkedHashMap<>();
                    for (int i = 1; i + 1 < parts.size(); i += 2) {
                        hash.put(String.valueOf(parts.get(i)), String.valueOf(parts.get(i + 1)));
                    }
                    return new TimedSnapshot(hash, now);
                });
    }

    @Override
    public Mono<Map<String, String>> load() {
        return redis.<String, String>opsForHash().entries(RedisKeys.SNAPSHOT)
                .collectMap(Map.Entry::getKey, Map.Entry::getValue);
    }

    private Long toLong(String raw) {
        if (raw == null) {
            return null;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}

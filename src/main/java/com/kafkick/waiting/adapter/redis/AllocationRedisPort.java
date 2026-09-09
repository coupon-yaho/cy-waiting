package com.kafkick.waiting.adapter.redis;

import com.kafkick.waiting.control.CapacityReport;
import com.kafkick.waiting.domain.routing.InstanceAddress;
import com.kafkick.waiting.control.CapacitySample;
import com.kafkick.waiting.control.TimedCoupons;
import com.kafkick.waiting.control.TimedSnapshot;
import com.kafkick.waiting.control.ControlPlaneProperties;
import com.kafkick.waiting.control.FailureWindow;
import com.kafkick.waiting.control.SnapshotCodec;
import com.kafkick.waiting.control.SnapshotSource;
import com.kafkick.waiting.domain.allocation.Grant;
import com.kafkick.waiting.domain.coupon.QueueMode;
import com.kafkick.waiting.control.QueueSweeper;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
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
     * <p>루아 스택이 넘치면 그 회차가 통째로 실패한다. 쿠폰이 그만큼 많아지면
     * 나눠 실어야 하는데, 그러면 원자성이 깨지므로 <b>그때는 설계를 다시 본다.</b>
     */
    private static final int MAX_PUBLISH_FIELDS = 3_000;

    /** 한 회차가 동시에 낼 수 있는 읽기. 무제한이면 한 회차가 커넥션을 독점한다. */
    private static final int MAX_CONCURRENT_READS = 16;

    /** 잠금을 겹쳐 보내는 폭. 읽기와 값은 같지만 이름이 거짓말을 하면 안 된다. */
    private static final int MAX_CONCURRENT_WRITES = 16;

    /**
     * 울타리 표 수명의 <b>하한</b>. 실제 값은 리스에서 유도한다.
     *
     * <p>안 주면 쿠폰이 활성에서 빠진 뒤에도 표가 남아, 시계가 뒤로 간 리더는
     * 스큐가 걷혀도 안 풀리고 자기 시각이 그 옛 표를 넘어야 풀린다. 반대로
     * 리스보다 짧으면 옛 리더가 아직 유효한 동안 표가 사라져 울타리가 없어진다.
     */
    private static final Duration MIN_FENCE_TTL = Duration.ofHours(1);

    /** 표가 견뎌야 하는 리스의 배수. 지연된 명령이 도착할 여유까지 본다. */
    private static final int FENCE_TTL_LEASES = 4;

    /**
     * 스냅샷 울타리의 <b>하한</b>. 쿠폰별 표와 달리 <b>1시간이 아니다</b>.
     *
     * <p>리더가 매 틱 다시 쓰므로 막아야 할 창이 리스 하나 더하기 틱뿐이다. 길게
     * 두면 시계가 뒤로 간 리더가 그 시간 내내 발행을 못 하고, 그동안 전 노드가
     * 얼어붙은 재료를 읽는다 — 이 울타리가 막으려던 것보다 나쁘다. 짧아서 생기는
     * 구멍은 없다: 거절은 수명을 갱신하지 않으므로 만료는 리스를 잃었다는 뜻이다.
     */
    private static final Duration MIN_SNAPSHOT_FENCE_TTL = Duration.ofSeconds(10);

    /** 값이 JSON 인 것은 계약이다 — 위치 기반 문자열은 필드가 늘면 깨진다. */
    private static final ObjectMapper JSON = new ObjectMapper();

    private static final Logger log = LoggerFactory.getLogger(AllocationRedisPort.class);

    private final ReactiveStringRedisTemplate redis;
    private static final RedisScript<List> SWEEP =
            RedisScript.of(new ClassPathResource("redis/sweep.lua"), List.class);

    private static final RedisScript<Long> SEAL_FENCES =
            RedisScript.of(new ClassPathResource("redis/fence_seal.lua"), Long.class);

    private static final RedisScript<Long> SEAL_SNAPSHOT_FENCE =
            RedisScript.of(new ClassPathResource("redis/snapshot_fence_seal.lua"), Long.class);

    private static final RedisScript<Long> DROP_QUEUE =
            RedisScript.of(new ClassPathResource("redis/drop_queue.lua"), Long.class);

    /**
     * 쿠폰별 HSCAN 커서. 매 회차 0 에서 시작하면 해시 뒤쪽이 영영 안 걷힌다.
     *
     * <p><b>이번 회차에 쓴 쿠폰만 남긴다.</b> 안 그러면 끝난 쿠폰의 항목이 JVM
     * 수명 내내 쌓인다 — 회차가 계속 열리는 제품이라 실제로 자란다.
     */
    private final Map<String, String> sweepCursors = new ConcurrentHashMap<>();

    private final int shards;

    /** 울타리 표의 수명. 리스보다 넉넉히 길어야 옛 리더가 사라지기 전에 안 걷힌다. */
    private final Duration fenceTtl;

    /** 스냅샷 울타리의 수명. 쿠폰별 표와 이유가 달라 따로 든다. */
    private final Duration snapshotFenceTtl;
    /** 옛 임기라 막힌 매진 큐 삭제. 그 창 동안 죽은 줄이 폴링 예산을 먹는다. */
    private final AtomicLong dropFenced = new AtomicLong();

    private final FailureWindow dropFencedWindow = FailureWindow.create();

    private final FailureWindow rejected = FailureWindow.create();
    private final FailureWindow malformed = FailureWindow.create();

    /** 주소만 어긋난 경우. 보고 자체는 살므로 위 창과 따로 센다. */
    private final FailureWindow addressMalformed = FailureWindow.create();
    private final FailureWindow badPolicy = FailureWindow.create();
    private final FailureWindow publishTrim = FailureWindow.create();

    /** 울타리가 발행을 막은 구간. 그 사이 전 노드가 얼어붙은 재료를 읽는다. */
    private final FailureWindow publishFence = FailureWindow.create();

    private final AtomicLong publishFenced = new AtomicLong();

    /** 울타리가 막은 입장 적용 건수. <b>회차가 아니라 쿠폰 단위다</b>. */
    private final AtomicLong applyFenced = new AtomicLong();


    /**
     * 상한을 넘겨 버린 미상 표시의 누적 수. <b>0 이 아니면 거짓 매진이 나갔다.</b>
     *
     * <p>미상인 채 발행한 수만 세면 실제로 해를 내는 사건이 안 잡힌다 — 무해한
     * 쪽만 세는 셈이다.
     */
    private final AtomicLong markersDropped = new AtomicLong();
    /** 신선도의 기준 시각. 뒤로 가는 것을 여기서 막는다. */
    private final ServerClock serverClock = ServerClock.create();

    /** 마지막으로 성공한 정책 회차. 읽기가 실패하면 여기로 되돌아간다. */
    private final AtomicReference<Map<String, QueueMode>> lastModes =
            new AtomicReference<>(Map.of());

    /**
     * <b>모든 노드가 쓴다.</b> 배분은 리더만 돌지만 판정 재료를 받아 오는 것은
     * 전 노드가 하므로, 배분 토글 뒤에 두면 요청만 받는 노드가 재료를 못 받는다.
     */
    @Autowired
    AllocationRedisPort(ReactiveStringRedisTemplate redis, ControlPlaneProperties properties) {
        // **표 수명을 리스에서 유도한다.** 고정값으로 두면 리스를 늘렸을 때
        // 옛 리더가 아직 유효한 동안 표가 사라져 울타리가 통째로 없어진다.
        this(redis, properties.scheduler().shards(),
                MIN_FENCE_TTL.compareTo(
                                properties.leader().lease().multipliedBy(FENCE_TTL_LEASES)) > 0
                        ? MIN_FENCE_TTL
                        : properties.leader().lease().multipliedBy(FENCE_TTL_LEASES),
                MIN_SNAPSHOT_FENCE_TTL.compareTo(
                                properties.leader().lease().multipliedBy(FENCE_TTL_LEASES)) > 0
                        ? MIN_SNAPSHOT_FENCE_TTL
                        : properties.leader().lease().multipliedBy(FENCE_TTL_LEASES));
    }



    private AllocationRedisPort(ReactiveStringRedisTemplate redis, int shards,
            Duration fenceTtl, Duration snapshotFenceTtl) {
        this.fenceTtl = Objects.requireNonNull(fenceTtl, "fenceTtl 은 필수다");
        this.snapshotFenceTtl =
                Objects.requireNonNull(snapshotFenceTtl, "snapshotFenceTtl 은 필수다");
        if (shards < 1) {
            throw new IllegalArgumentException("shards 는 1 이상이어야 한다: %d".formatted(shards));
        }
        this.redis = Objects.requireNonNull(redis, "redis 는 필수다");
        this.shards = shards;
    }

    public static AllocationRedisPort of(ReactiveStringRedisTemplate redis, int shards) {
        return new AllocationRedisPort(redis, shards, MIN_FENCE_TTL, MIN_SNAPSHOT_FENCE_TTL);
    }

    /**
     * 울타리 수명을 짧게 준다. <b>시험이 스스로 풀리는 것을 재려면 필요하다</b> —
     * 운영 값으로는 그 갈래를 재는 데 열 초가 걸린다.
     */
    static AllocationRedisPort withSnapshotFenceTtl(ReactiveStringRedisTemplate redis,
            int shards, Duration snapshotFenceTtl) {
        return new AllocationRedisPort(redis, shards, MIN_FENCE_TTL, snapshotFenceTtl);
    }

    /** 상한을 넘겨 버린 미상 표시의 누적 수. 0 이 아니면 거짓 매진이 나갔다. */
    public double markersDropped() {
        return markersDropped.get();
    }

    /** 시계가 뒤로 간 사실을 남긴다. 조용히 보정하면 왜 그랬는지를 영영 못 밝힌다. */
    public ClockSkewTracker clockSkew() {
        return serverClock.skew();
    }

    /**
     * 뒷단이 스스로 적어 둔 여유를 읽는다.
     *
     * <p><b>밖에서 쓰는 키라 아무 값이나 들어온다.</b> 깨진 값 하나가 회차를 죽이면
     * 멀쩡한 인스턴스 몫까지 사라져 전역 크레딧이 하한으로 떨어진다.
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
            // **주소가 없거나 모양이 어긋나도 보고는 산다.** 크레딧에는
            // 들고 라우팅 후보에서만 빠진다 — 버리면 그 몫만큼 전역 크레딧이
            // 조용히 줄어, 계약을 아직 안 따르는 배포 구간에 전체가 조여진다.
            JsonNode addr = node.get("addr");
            InstanceAddress parsed = addr == null || !addr.isTextual()
                    ? null : InstanceAddress.parse(addr.asText()).orElse(null);
            // **모양이 어긋난 것만 남긴다.** 아예 없는 것은 계약을 아직 안 따르는
            // 배포 구간의 정상 상태다. 있는데 못 읽는 것은 계약 위반이라, 안 남기면
            // 그 대가 영영 라우팅에서 빠진 채로 아무도 모른다.
            if (parsed == null && addr != null && addr.isTextual() && addressMalformed.entered()) {
                log.warn("가용량 보고의 주소가 모양에 안 맞는다 — {}. 이 인스턴스는 "
                        + "크레딧에는 들지만 라우팅 후보에서 빠진다", instanceId);
            }
            return new CapacityReport(instanceId, credits.longValue(), ts.longValue(), parsed);
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

    /**
     * 목록에 없는 쿠폰은 보지 않는다. 끝난 쿠폰까지 보면 매 틱 왕복만 늘어난다.
     *
     * <p><b>밖에서 쓰는 키라 아무 값이나 들어온다.</b> 키에 못 쓰는 멤버 하나가
     * 회차를 죽이면 멀쩡한 쿠폰 전부의 배분이 멎는데, 사람이 목록을 고치기 전에는
     * 안 풀린다. 그래서 걸러 내되 걸러 냈다는 사실을 남긴다.
     */
    public Mono<List<String>> activeCoupons() {
        AtomicBoolean dropped = new AtomicBoolean();
        return redis.opsForSet().members(RedisKeys.ACTIVE_COUPONS)
                .filter(couponId -> usable(couponId, dropped))
                .sort()
                .collectList()
                // 걸러 낼 것이 없어진 회차에서 창을 닫는다. 안 닫으면 나중에 다른
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
     * <p><b>밖에서 쓰는 키다.</b> 못 읽는 값이 하나 있다고 회차를 죽이면 운영자의
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
                // **물어본 쿠폰만 갱신한다.** 회차마다 통째로 갈아치우면, 이번 회차에
                // 안 물어본 쿠폰의 정책이 캐시에서 사라진다 — 그 쿠폰이 다음 회차에
                // 돌아왔을 때 읽기가 실패하면 ALWAYS 가 조용히 적응형이 된다.
                .doOnNext(modes -> remember(couponIds, modes))
                // **정책은 부가 정보다.** 이것 하나 때문에 회차가 죽으면 대기 수와
                // 재고가 멀쩡해도 스냅샷이 안 나간다. 빈 회차로 접으면 전원이
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
        // 순서는 아무 뜻이 없다. 다만 무제한으로 풀면 한 회차가 커넥션을 독점해
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
                .reduce(0L, (a, b) -> a + b);
    }

    /**
     * 세기 시작한 쿠폰의 줄 옆에 <b>울타리 표만</b> 세운다.
     *
     * <p>표는 지웠을 때만 생기므로 한 번도 안 지운 줄에는 표가 없다. 후보로
     * 올리는 순간 세워야 그 뒤에 오는 옛 회차가 걸린다.
     */
    public Mono<List<String>> claimSoldOutQueues(List<String> couponIds, long fence) {
        // **리더가 아니면 세울 자격도 없다.** 0 을 그대로 넘기면 스크립트가 앞에서
        // 되돌아 0 을 내는데, 그것을 "표가 섰다" 로 접으면 그 쿠폰이 다시는 후보에
        // 안 오른다.
        if (couponIds.isEmpty() || shards != 1 || fence <= 0) {
            return Mono.just(List.of());
        }
        return Flux.fromIterable(couponIds)
                // **선 것만 돌려준다.** 실패한 것을 확인으로 치면 그 줄은 표
                // 없이 유예를 보내고, 옛 회차가 그대로 지운다.
                .flatMap(id -> runDrop(id, fence, false)
                        // **-1 은 내 표가 안 섰다는 뜻이다** (CY-894). 그것을 확인으로
                        // 접으면 그 쿠폰이 다시는 후보에 안 올라 표 없이 유예를 보낸다.
                        .filter(result -> result >= 0)
                        .map(ignored -> id)
                        .onErrorResume(e -> Mono.empty()), MAX_CONCURRENT_READS)
                .collectList()
                .map(List::copyOf);
    }

    /**
     * 매진된 쿠폰의 줄과 딸린 키를 지운다.
     *
     * <p><b>한 쿠폰이 실패해도 나머지는 지운다.</b> 정리가 배분을 막으면
     * 안 지워진 것 하나가 그 틱 전체를 세운다.
     */
    public Mono<List<String>> dropSoldOutQueues(List<String> couponIds, long fence) {
        if (couponIds.isEmpty()) {
            return Mono.just(List.of());
        }
        // **샤딩을 켜면 지울 수 없다.** 재고는 샤드 무관 키라 줄과 슬롯이 갈린다.
        // 단독 배치는 받아 주지만 샤드 0 만 지워 나머지 샤드의 줄이 영구 고아가
        // 되고, 그 줄은 폴링 예산을 영원히 먹는다. 조용해서 여기서 소리 나게 막는다.
        if (shards != 1) {
            return Mono.error(new IllegalStateException(
                    "샤드가 여럿이면 매진 큐를 못 지운다 — 재고 세대가 있어야 한다: %d"
                            .formatted(shards)));
        }
        long before = dropFenced.get();
        return Flux.fromIterable(couponIds)
                // **지운 것만 쿠폰별로 돌려준다.** 합으로 접거나 안 지운 쿠폰까지
                // 실으면 부르는 쪽이 그것을 "지웠다" 로 읽어, 실패한 쿠폰과 살아난
                // 쿠폰이 다음 틱에 다시 안 온다.
                .flatMap(id -> dropOne(id, fence)
                        .filter(Boolean::booleanValue)
                        .map(dropped -> id)
                        .onErrorResume(e -> Mono.empty()), MAX_CONCURRENT_READS)
                .collectList()
                // **회차 단위로 판정한다.** 쿠폰마다 열고 닫으면 한 틱 안에서 창이
                // 여러 번 뒤집히고, 유령이 실제로 지운 회차가 "다시 지난다" 로 찍힌다.
                .doOnNext(dropped -> judgeFenceWindow(dropFenced.get() - before, fence));
    }

    /** 이 회차에 막힌 것이 있었나. 진입은 구간의 첫 회차에만, 해제는 짝으로 남긴다. */
    private void judgeFenceWindow(long fencedNow, long fence) {
        if (fencedNow > 0) {
            if (dropFencedWindow.entered()) {
                log.warn("매진 큐 삭제가 막혔다 — 옛 임기 {} 다. 이 회차에 {}건. "
                        + "그 줄은 다음 리더가 지운다", fence, fencedNow);
            }
            return;
        }
        dropFencedWindow.exited().ifPresent(recovered ->
                log.info("매진 큐 삭제가 다시 지난다 — {}초 동안 {}회차가 막혔다",
                        recovered.elapsedSeconds(), recovered.swallowed()));
    }

    /**
     * 줄과 생존 신호만 지운다. 나머지 셋은 지우면 <b>되돌릴 수 없는 손해</b>가 난다.
     * `admitted:` 는 입장 임계의 단조성이 깨져 입장한 사람이 토큰을 두 번 받고,
     * `grace:` 는 차례가 왔던 사람이 종료를 안 받게 막는 유일한 장치이며,
     * `coupons:active` 는 발급 계층 소유라 빼는 순간 매진 종결이 꺼져 미지 쿠폰이
     * fail-open 으로 흐른다. 재고는 쓰기 직전 스크립트 안에서 다시 본다.
     */
    private Mono<Long> runDrop(String couponId, long fence, boolean delete) {
        return redis.execute(DROP_QUEUE,
                        List.of(RedisKeys.queue(couponId, shards, 0),
                                RedisKeys.alive(couponId, shards, 0),
                                RedisKeys.stock(couponId),
                                RedisKeys.dropFence(couponId, shards, 0)),
                        List.of(Long.toString(fence), delete ? "1" : "0",
                                Long.toString(fenceTtl.toMillis())))
                .next()
                .defaultIfEmpty(0L);
    }

    private Mono<Boolean> dropOne(String couponId, long fence) {
        return runDrop(couponId, fence, true)
                .map(result -> {
                    // **막힌 것을 안 지운 것과 가른다.** 둘이 같으면 최대 유예 내내
                    // 죽은 줄이 폴링 예산을 먹는데 아무도 못 본다.
                    // -2 는 표가 아예 없다는 뜻이다. 잠금과 후보 표시가 둘 다
                    // 실패한 것이라, 자기가 유일한 리더라는 근거가 없다.
                    if (result < 0) {
                        dropFenced.incrementAndGet();
                        return false;
                    }
                    return result == 1L;
                })
                .onErrorResume(e -> {
                    // **매 건 남긴다.** 이 저장소의 유일한 비가역 쓰기인데 실패는
                    // 부르는 쪽에서 삼켜진다. 창을 걸면 프로세스 수명에 한 줄만
                    // 남아 정리가 멎어도 조용하다.
                    log.warn("매진 큐 정리 실패 — 다음 틱에 다시 한다: 쿠폰={} {}",
                            couponId, e.toString());
                    return Mono.error(e);
                });
    }

    /**
     * 이탈자를 걷어 낸다.
     *
     * <p><b>커서를 쿠폰별로 이어 간다.</b> 매번 0 에서 시작하면 해시 앞쪽만
     * 계속 훑고 뒤쪽 기록은 영영 안 지워진다.
     */
    public Mono<QueueSweeper.SweepResult> sweep(List<String> couponIds, long nowSec,
            int scanLimit, long graceSec, int budget) {
        return sweep(couponIds, nowSec, scanLimit, graceSec, budget, true);
    }

    /**
     * @param removeFront 앞줄에서 빼도 되는가. <b>거짓이어도 정리는 돈다</b> —
     *                    승계 유예 구간이 그 자리다
     */
    public Mono<QueueSweeper.SweepResult> sweep(List<String> couponIds, long nowSec,
            int scanLimit, long graceSec, int budget, boolean removeFront) {
        if (couponIds.isEmpty()) {
            return Mono.just(QueueSweeper.SweepResult.NOTHING);
        }
        sweepCursors.keySet().retainAll(couponIds);
        return Flux.fromIterable(couponIds)
                // **한 쿠폰이 실패해도 나머지는 쓴다.** 청소가 배분을 막으면
                // 안 걷힌 것 하나가 그 틱 전체를 세운다.
                .flatMap(id -> sweepOne(id, nowSec, scanLimit, graceSec, budget, removeFront)
                        .onErrorResume(e -> {
                            log.warn("이탈자 청소 실패 — 다음 틱에 다시 한다: 쿠폰={} {}",
                                    id, e.toString());
                            // **실패로 센다.** 성공으로 접으면 청소가 멎은 것이
                            // "걷을 게 없었다" 와 같은 값이 된다.
                            return Mono.just(QueueSweeper.SweepResult.FAILED);
                        }), MAX_CONCURRENT_READS)
                .reduce(QueueSweeper.SweepResult.NOTHING, (a, b) -> new QueueSweeper.SweepResult(
                        a.swept() + b.swept(),
                        a.expiredSignals() + b.expiredSignals(),
                        a.expiredGrace() + b.expiredGrace(),
                        a.failed() + b.failed()));
    }

    private Mono<QueueSweeper.SweepResult> sweepOne(String couponId, long nowSec,
            int scanLimit, long graceSec, int budget, boolean removeFront) {
        String cursor = sweepCursors.getOrDefault(couponId, "0");
        return redis.execute(SWEEP,
                        List.of(RedisKeys.queue(couponId, shards, 0),
                                RedisKeys.grace(couponId, shards, 0),
                                RedisKeys.alive(couponId, shards, 0),
                                RedisKeys.admitted(couponId, shards, 0)),
                        List.of(Integer.toString(scanLimit), Long.toString(nowSec),
                                Long.toString(graceSec), Integer.toString(budget), cursor,
                                removeFront ? "1" : "0"))
                .next()
                .switchIfEmpty(Mono.error(new IllegalStateException("청소 결과가 비었다")))
                .map(raw -> {
                    List<?> values = (List<?>) raw;
                    sweepCursors.put(couponId, String.valueOf(values.get(3)));
                    return new QueueSweeper.SweepResult(toLongOrZero(values.get(0)),
                            toLongOrZero(values.get(1)), toLongOrZero(values.get(2)), 0);
                });
    }

    private long toLongOrZero(Object value) {
        return value instanceof Number n ? n.longValue() : 0;
    }

    /**
     * 쿠폰별 재고. <b>못 읽으면 담지 않는다</b> — 키가 없거나 수가 아닐 때다.
     *
     * <p>빠진 자리를 0 으로 접으면 재고 키를 잃은 쿠폰이 매진이 된다. 부르는
     * 쪽이 그 빈자리를 미상으로 싣는다.
     */
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
     * 발행의 문을 새 임기로 잠근다. <b>쿠폰 쪽과 나란히 돈다</b> — 슬롯이 갈려
     * 한 스크립트로 못 묶고, 이어 붙이면 배분이 안 도는 시간이 곱해진다.
     *
     * <p>안 잠그면 새 리더의 첫 발행 전까지 유령의 발행이 자기 번호와 같아서
     * 통과한다. 그 회차의 정리와 청소가 그 뒤에 매달려 같이 나간다 (CY-911).
     *
     * @return 1 이면 잠갔다. 0 은 리더가 아니거나 이미 더 앞선 임기가 서 있다
     */
    public Mono<Long> sealSnapshotFence(long fence) {
        if (fence <= 0) {
            return Mono.just(0L);
        }
        return redis.execute(SEAL_SNAPSHOT_FENCE, List.of(RedisKeys.SNAPSHOT_FENCE),
                        List.of(Long.toString(fence),
                                Long.toString(snapshotFenceTtl.toMillis())))
                .next()
                .map(Number::longValue);
    }

    /**
     * 활성 쿠폰의 문을 새 임기로 잠근다. <b>승계 직후에 부른다</b> — 적용만으로는
     * 그 쿠폰에 크레딧이 갈 때까지 표에 옛 임기가 남고, 그 창에 유령이 먼저
     * 도착하면 자기 번호와 같아서 통과한다.
     *
     * <p><b>샤드 0 에만 나간다.</b> 적용과 같은 자리라 지금은 맞지만, 샤딩을 켜면
     * 나머지 샤드의 문이 안 잠긴다.
     *
     * @return 잠근 쿠폰 수. 넘긴 수보다 적으면 그만큼 못 잠갔다
     */
    public Mono<Long> sealFences(Collection<String> couponIds, long fence) {
        // **승계에서 창을 닫는다.** 리더십을 잃으면 정리가 안 돌아 해제가 영영
        // 안 찍히고, 다음 사건은 진입이 이미 열려 있어 한 줄도 안 남는다.
        dropFencedWindow.exited().ifPresent(recovered ->
                log.info("매진 큐 삭제 막힘 구간이 승계로 끝났다 — {}초 동안 {}회차",
                        recovered.elapsedSeconds(), recovered.swallowed()));
        if (fence <= 0 || couponIds.isEmpty()) {
            return Mono.just(0L);
        }
        // **샤드 0 만 잠근다.** 매진 큐 삭제 자체가 샤드가 여럿이면 거절하므로
        // 지금은 맞지만, 그 빗장을 푸는 날 나머지 샤드의 문이 안 잠긴 채로
        // 성공을 낸다 — 잠갔다는 로그가 있는데 줄이 지워진다.
        if (shards != 1) {
            return Mono.error(new IllegalStateException(
                    "샤드가 여럿이면 울타리를 다 못 잠근다: %d".formatted(shards)));
        }
        return Flux.fromIterable(couponIds)
                .flatMap(couponId -> redis.execute(SEAL_FENCES,
                                List.of(RedisKeys.applyFence(couponId, shards, 0),
                                        RedisKeys.dropFence(couponId, shards, 0)),
                                List.of(Long.toString(fence),
                                        Long.toString(fenceTtl.toMillis())))
                        .next()
                        // **스크립트가 낸 값을 그대로 접는다.** 1 로 갈면 "예외가 안
                        // 난 수" 가 되어, 안 잠근 것을 잠갔다고 센다.
                        .map(Number::longValue)
                        // 하나가 실패해도 나머지는 잠근다. 못 잠근 쿠폰은 그 자리에서
                        // 다시 막는다 — 안 잠긴 채로 지나가지 않는다.
                        .onErrorReturn(0L), MAX_CONCURRENT_WRITES)
                .reduce(0L, Long::sum);
    }

    /** 옛 임기라 막힌 매진 큐 삭제 건수. */
    public long dropFenced() {
        return dropFenced.get();
    }

    /**
     * 들어온 인원을 돌려준다. 나눠 준 몫과 다르다 — 큐가 짧으면 남는다.
     *
     * <p><b>샤드가 하나인 동안만 옳다.</b> 여럿이면 몫을 샤드에 나눠 각각
     * 적용해야 하는데, 지금은 0번에만 나간다. 그래서 기동에서 하나로 막는다.
     *
     * @param fence 이 회차의 임기. 옛 임기는 임계를 안 올린다. 0 이면 리더가 아니다
     */
    public Mono<Long> apply(Grant grant, long fence) {
        return redis.execute(APPLY,
                        List.of(RedisKeys.queue(grant.couponId(), shards, 0),
                                RedisKeys.admitted(grant.couponId(), shards, 0),
                                RedisKeys.applyFence(grant.couponId(), shards, 0)),
                        List.of(Long.toString(grant.credit()), Long.toString(fence),
                                Long.toString(fenceTtl.toMillis())))
                .next()
                .flatMap(result -> {
                    List<?> counts = (List<?>) result;
                    // **칸 수로 가른다.** {-1, 0} 은 임계가 없고 들일 사람도 없는
                    // 정상 회차와 같은 값이라, 그것으로 가르면 새 쿠폰과 빈 큐가
                    // 거절로 오독된다.
                    if (counts.size() < 3) {
                        return Mono.just(Long.parseLong(String.valueOf(counts.get(1))));
                    }
                    applyFenced.incrementAndGet();
                    // **오류로 올린다.** 회차가 몫을 0 으로 접는 자리가 이미 있고,
                    // 값으로 0 을 내면 임계가 안 올랐는데 몫만 실려 나간다.
                    return Mono.error(new FencedOutException(grant.couponId(), fence,
                            Long.parseLong(String.valueOf(counts.get(2)))));
                });
    }

    /**
     * 운영자가 배포 없이 고친 값을 읽는다.
     *
     * <p><b>리더만 읽는다.</b> 전 노드가 매 틱 읽으면 그 자체가 요청 경로 밖의
     * 부하이고, 노드마다 다른 값을 볼 수 있다 — 스냅샷으로 퍼뜨리는 이유다.
     */
    public Mono<String> readTunables() {
        return redis.opsForValue().get(RedisKeys.TUNABLES);
    }

    /**
     * <b>통째로 갈아 끼우되 사이가 벌어지지 않게 한다.</b>
     *
     * <p>지우고 쓰는 것을 나눠 치면 그 사이에 끊길 때 키가 없는 채로 남고,
     * 전 노드가 판정 재료를 잃는다. 근거는 스크립트 주석에 있다.
     *
     * @param fence 이 발행의 임기. 옛 임기는 새 임기를 못 덮는다. 0 이면 리더가 아니다
     */
    public Mono<Void> publish(Map<String, String> hash, long fence) {
        if (hash.isEmpty()) {
            return Mono.error(new IllegalArgumentException("빈 스냅샷은 발행하지 않는다"));
        }
        Map<String, String> toPublish = withinLimit(hash);
        if (toPublish.size() > MAX_PUBLISH_FIELDS) {
            return Mono.error(new IllegalStateException(
                    "한 번에 실을 수 있는 필드를 넘었다: %d > %d"
                            .formatted(toPublish.size(), MAX_PUBLISH_FIELDS)));
        }
        List<String> args = new ArrayList<>(toPublish.size() * 2 + 2);
        args.add(Long.toString(fence));
        args.add(Long.toString(snapshotFenceTtl.toMillis()));
        toPublish.forEach((field, value) -> {
            args.add(field);
            args.add(value);
        });
        int dropped = hash.size() - toPublish.size();
        return redis.execute(PUBLISH,
                        List.of(RedisKeys.SNAPSHOT, RedisKeys.SNAPSHOT_FENCE), args).next()
                .flatMap(result -> {
                    long blockedBy = blockedBy(result);
                    if (blockedBy < 0) {
                        publishFence.exited().ifPresent(recovered -> log.info(
                                "발행이 다시 나간다 — {}초 만에, 그동안 {}회차 막혔다",
                                recovered.elapsedSeconds(), recovered.swallowed()));
                        return Mono.just(result);
                    }
                    publishFenced.incrementAndGet();
                    // 구간의 첫 건만 남긴다. 막힌 동안 전 노드가 얼어붙은 재료를
                    // 읽으므로, 이 줄이 그 상태의 유일한 원인 신호다.
                    if (publishFence.entered()) {
                        log.error("발행이 울타리에 막혔다 — 이 노드의 임기 {}, 마지막으로 "
                                + "쓴 임기 {}. 전 노드가 곧 낡은 재료를 읽는다", fence, blockedBy);
                    }
                    return Mono.<List<?>>error(new FencedOutException(fence, blockedBy));
                })
                .doOnSuccess(done -> watchTrim(dropped))
                .then();
    }

    /**
     * 울타리가 거절했는가. <b>센티널로 본다</b> — 실린 수 0 으로 보면 값 충돌에
     * 기대게 되고, 빈 발행을 나중에 허용하는 순간 거절이 조용히 성공으로 읽힌다.
     */
    private long blockedBy(Object result) {
        if (!(result instanceof List<?> counts) || counts.size() < 3
                || !(counts.get(0) instanceof Number written) || written.longValue() != -1) {
            return -1;
        }
        return counts.get(2) instanceof Number seen ? seen.longValue() : 0;
    }

    /** 울타리가 발행을 거절한 회차 수. 0 이 아니면 이 노드의 재료가 안 나갔다. */
    public double publishFenced() {
        return publishFenced.get();
    }

    /** 울타리가 입장 적용을 거절한 건수. 쿠폰마다 오르므로 회차 수가 아니다. */
    public double applyFenced() {
        return applyFenced.get();
    }

    /**
     * 옛 임기의 쓰기가 거절됐다. 발행과 입장 적용이 같은 원인으로 여기 온다 —
     * 이 노드는 더 이상 리더가 아니거나, 그렇게 보이는 임기를 들고 있다.
     */
    public static final class FencedOutException extends IllegalStateException {

        FencedOutException(long fence, long blockedBy) {
            super("발행이 울타리에 막혔다 — 이 노드의 임기 %d, 마지막으로 쓴 임기 %d"
                    .formatted(fence, blockedBy));
        }

        FencedOutException(String couponId, long fence, long blockedBy) {
            // **임기 0 은 "막은 사람" 이 아니라 "내가 리더가 아니다" 다.** 그 값을
            // 마지막 기록자로 찍으면 운영자가 없는 임기를 찾는다.
            super(blockedBy > 0
                    ? "입장 적용이 울타리에 막혔다 — 쿠폰 %s, 이 노드의 임기 %d, 마지막으로 들인 임기 %d"
                            .formatted(couponId, fence, blockedBy)
                    : "입장 적용을 안 냈다 — 쿠폰 %s, 이 노드는 리더가 아니다"
                            .formatted(couponId));
        }
    }

    /**
     * 상한을 넘으면 <b>미상 표시부터 버린다</b>. 표시를 잃은 쿠폰은 거짓 매진으로
     * 읽히지만, 스냅샷이 아예 안 나가는 것보다는 낫다.
     *
     * <p>표시는 쿠폰마다 필드를 하나 더 쓴다. 그 두 배가 되는 순간은 재고를 통째로
     * 못 읽는 순간이라 하필 그때 발행이 죽는다 — 전 노드가 낡음으로 넘어가고
     * 정리도 청소도 같이 멎는다.
     */
    private Map<String, String> withinLimit(Map<String, String> hash) {
        if (hash.size() <= MAX_PUBLISH_FIELDS) {
            return hash;
        }
        // **필요한 만큼만 버린다.** 하나를 넘었다고 전부 버리면 안 버려도 될
        // 쿠폰까지 거짓 매진이 되고, 그 하나하나가 줄을 잃는 경로를 탄다.
        Map<String, String> trimmed = new LinkedHashMap<>(hash);
        int over = hash.size() - MAX_PUBLISH_FIELDS;
        for (String marker : droppableMarkers(hash)) {
            if (over <= 0) {
                break;
            }
            trimmed.remove(marker);
            over--;
        }
        return trimmed;
    }

    /**
     * 버린 사실을 남긴다. <b>발행이 끝난 뒤에만 부른다.</b>
     *
     * <p>앞에서 세면 지표가 "거짓 매진이 된 쿠폰 수" 가 아니라 "버리려고 시도한
     * 횟수" 가 된다. 같은 회차가 매 틱 실패하는 구간에서 그 수가 끝없이 부푼다.
     */
    private void watchTrim(int dropped) {
        if (dropped <= 0) {
            return;
        }
        markersDropped.addAndGet(dropped);
        // **몇 개인지만 남긴다.** 쿠폰 ID 는 라벨로도 로그로도 못 쏟는다.
        if (publishTrim.entered()) {
            log.warn("발행 필드가 상한을 넘어 재고 미상 표시 {}개를 버렸다 — 그 쿠폰들이 매진으로 읽힌다",
                    dropped);
        }
    }

    /**
     * 버려도 덜 아픈 표시부터. <b>줄이 빈 쿠폰이 먼저다.</b>
     *
     * <p>줄이 빈 쿠폰의 표시를 잃으면 신규 유입만 거절되고 다음 회차가 되돌린다.
     * 줄이 선 쿠폰의 표시를 잃으면 그 줄이 통째로 종결로 읽힌다.
     */
    private List<String> droppableMarkers(Map<String, String> hash) {
        List<String> empty = new ArrayList<>();
        List<String> queued = new ArrayList<>();
        hash.forEach((field, value) -> {
            if (!field.startsWith(SnapshotCodec.STOCK_UNKNOWN_FIELD)) {
                return;
            }
            String coupon = field.substring(SnapshotCodec.STOCK_UNKNOWN_FIELD.length());
            (waitingOf(hash.get(coupon)) > 0 ? queued : empty).add(field);
        });
        empty.addAll(queued);
        return empty;
    }

    /** 쿠폰 값의 대기 수. <b>못 읽으면 줄이 선 것으로 본다</b> — 덜 버리는 쪽이다. */
    private long waitingOf(String raw) {
        if (raw == null) {
            return 1;
        }
        String[] parts = raw.split(":");
        if (parts.length < 5) {
            return 1;
        }
        try {
            return Long.parseLong(parts[4]);
        } catch (NumberFormatException e) {
            return 1;
        }
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

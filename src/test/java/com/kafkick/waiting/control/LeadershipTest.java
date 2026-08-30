package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

/**
 * 배분은 리더 한 대만 돈다.
 *
 * <p><b>실패의 두 종류를 가른다.</b> "남이 쥐고 있다" 는 사실이라 즉시 내려오고,
 * "모른다"(오류·빈 응답·멈춤·취소)는 리스가 판단한다. 모르는 것을 즉시 하야로
 * 옮기면 락은 여전히 이 노드 것이라 남은 리스 동안 아무도 리더가 못 된다 —
 * 안전은 안 늘고 배분 공백만 생긴다.
 */
class LeadershipTest {

    private static final Duration LEASE = Duration.ofSeconds(2);
    private static final Duration ATTEMPT = Duration.ofMillis(400);

    /** 시험이 기다리는 상한. 리스와 무관하다 — 겸하면 리스를 줄일 때 같이 불안정해진다. */
    private static final Duration BLOCK = Duration.ofSeconds(5);

    private static final long ONE_SECOND_MILLIS = Duration.ofSeconds(1).toMillis();

    /** 벽시계를 안 탄다. 리스 경계가 실행 속도에 따라 갈리면 시험이 흔들린다. */
    private final AtomicLong ticker = new AtomicLong(1_000_000_000L);

    private ListAppender<ILoggingEvent> 로그;

    @BeforeEach
    void 로그를_받는다() {
        로그 = new ListAppender<>();
        로그.start();
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Leadership.class)).addAppender(로그);
    }

    @AfterEach
    void 로그를_뗀다() {
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Leadership.class)).detachAppender(로그);
    }

    private void 시간을_흘린다(Duration 만큼) {
        ticker.addAndGet(만큼.toNanos());
    }

    /**
     * <b>포맷 전 패턴</b>을 본다. 포맷하면 값과 문구가 뭉개져, {@code "7초"} 를
     * 찾는 단언이 {@code "17초"} 에도 통과한다. 문구를 다듬어도 안 깨진다.
     */
    private List<String> 로그_메시지(Level 수준) {
        return 로그.list.stream()
                .filter(e -> e.getLevel() == 수준)
                .map(ILoggingEvent::getMessage)
                .toList();
    }

    /** 값은 문자열에서 찾지 말고 인자에서 직접 잰다. */
    private List<Object[]> 로그_인자(Level 수준, String 패턴조각) {
        return 로그.list.stream()
                .filter(e -> e.getLevel() == 수준 && e.getMessage().contains(패턴조각))
                .map(ILoggingEvent::getArgumentArray)
                .toList();
    }

    private Leadership leadership(Supplier<Mono<LeaderLock>> acquire) {
        return leadership(acquire, Mono::empty);
    }

    private Leadership leadership(Supplier<Mono<LeaderLock>> acquire, Supplier<Mono<Void>> release) {
        return Leadership.of("node-1", LEASE, ATTEMPT, acquire, release, ticker::get);
    }

    private static Mono<LeaderLock> 내_락() {
        return Mono.just(LeaderLock.mine("node-1", LEASE.toMillis(), 1));
    }

    private Leadership 리더가_된다(Supplier<Mono<LeaderLock>> 그다음) {
        AtomicInteger 호출 = new AtomicInteger();
        Leadership leadership = leadership(
                () -> 호출.incrementAndGet() == 1 ? 내_락() : 그다음.get());
        leadership.renew().block(BLOCK);
        assertThat(leadership.isLeader()).isTrue();
        return leadership;
    }

    @Test
    @DisplayName("획득하면_리더다")
    void 획득하면_리더다() {
        Leadership leadership = leadership(LeadershipTest::내_락);

        leadership.renew().block(BLOCK);

        assertThat(leadership.isLeader()).isTrue();
    }

    @Test
    @DisplayName("남이_쥐고_있으면_리스를_안_기다리고_내려온다")
    void 남이_쥐고_있으면_리스를_안_기다리고_내려온다() {
        // 사실을 알았다. 여기서 리스를 기다리면 아무 이득 없이 배분만 밀린다.
        Leadership leadership = 리더가_된다(() -> Mono.just(LeaderLock.heldBy("node-2", 1_500)));

        leadership.renew().block(BLOCK);

        assertThat(leadership.isLeader()).isFalse();
        assertThat(로그_인자(Level.INFO, "리더를 잃었다")).singleElement()
                .satisfies(인자 -> assertThat(인자[1]).isEqualTo("node-2"));
        // 남이 쥔 것은 **정상 응답**이다. 실패로 찍으면 진짜 장애와 섞인다.
        assertThat(로그_메시지(Level.WARN)).noneMatch(m -> m.contains("리더 확인 실패"));
    }

    @Test
    @DisplayName("모르는_동안은_리스가_남아_있으면_리더로_둔다")
    void 모르는_동안은_리스가_남아_있으면_리더로_둔다() {
        // 로컬만 내려가고 락은 여전히 이 노드 것이다. 여기서 하야하면 남은 리스
        // 동안 **아무도** 리더가 못 된다 — 안전은 안 늘고 배분 공백만 생긴다.
        Leadership leadership = 리더가_된다(() -> Mono.error(new IllegalStateException("끊겼다")));

        시간을_흘린다(Duration.ofMillis(1_500));
        leadership.renew().block(BLOCK);

        assertThat(leadership.isLeader()).isTrue();
    }

    @Test
    @DisplayName("확인_없이_리스가_지나면_리더가_아니다")
    void 확인_없이_리스가_지나면_리더가_아니다() {
        // STW 나 루프 정지로 값이 늙는 경로는 저마다 모양이 달라 하나씩 못 막는다.
        // 나이로 재면 한 자리에서 접힌다.
        Leadership leadership = 리더가_된다(() -> Mono.error(new IllegalStateException("끊겼다")));

        시간을_흘린다(LEASE);

        assertThat(leadership.isLeader()).isFalse();
    }

    @Test
    @DisplayName("확인_없이_리스가_지나면_경고를_남긴다")
    void 확인_없이_리스가_지나면_경고를_남긴다() {
        // 이 노드가 조용히 리더를 잃은 것을 아는 유일한 신호다. 판정은 나이가
        // 이미 하고 있으므로, 여기서 재는 것은 **기록이 남는가** 다.
        Leadership leadership = 리더가_된다(() -> Mono.error(new IllegalStateException("끊겼다")));

        시간을_흘린다(LEASE.plusSeconds(4));
        assertThat(leadership.isLeader()).isFalse();

        assertThat(로그_인자(Level.WARN, "리스가 지나")).singleElement()
                .satisfies(인자 -> assertThat(인자[0]).isEqualTo(6L));
    }

    @Test
    @DisplayName("리스가_남아_있으면_경고를_안_남긴다")
    void 리스가_남아_있으면_경고를_안_남긴다() {
        Leadership leadership = 리더가_된다(() -> Mono.error(new IllegalStateException("끊겼다")));

        시간을_흘린다(LEASE.minusNanos(1));
        assertThat(leadership.isLeader()).isTrue();
        assertThat(로그_메시지(Level.WARN)).noneMatch(m -> m.contains("리스가 지나"));

        // 경계는 판정과 같은 자리여야 한다. 갈리면 리더가 아닌데 기록은 안 남는
        // 구간이 생긴다.
        시간을_흘린다(Duration.ofNanos(1));
        assertThat(leadership.isLeader()).isFalse();
        assertThat(로그_메시지(Level.WARN)).anyMatch(m -> m.contains("리스가 지나"));
    }

    @Test
    @DisplayName("연장을_아예_안_불러도_리스가_지나면_리더가_아니다")
    void 연장을_아예_안_불러도_리스가_지나면_리더가_아니다() {
        // 주기 계약이 런타임에 자기집행된다. 주석은 이걸 못 막는다.
        Leadership leadership = 리더가_된다(LeadershipTest::내_락);

        시간을_흘린다(LEASE);

        assertThat(leadership.isLeader()).isFalse();
    }

    @Test
    @DisplayName("임계와_같은_나이는_이미_지난_것으로_본다")
    void 임계와_같은_나이는_이미_지난_것으로_본다() {
        Leadership leadership = 리더가_된다(LeadershipTest::내_락);

        시간을_흘린다(LEASE.minusNanos(1));
        assertThat(leadership.isLeader()).isTrue();

        시간을_흘린다(Duration.ofNanos(1));
        assertThat(leadership.isLeader()).isFalse();
    }

    @Test
    @DisplayName("응답이_멈추면_한_판_안에_끝난다")
    void 응답이_멈추면_한_판_안에_끝난다() {
        // 멈춤은 오류가 아니라서 오류 처리에 안 걸린다. 이게 없으면 루프가 조용히
        // 멎고, 그 사이 리스가 만료돼 다른 노드가 잡는데도 이 노드는 참으로 얼어붙는다.
        Leadership leadership = leadership(Mono::never);

        // **상한을 건다.** 그냥 verifyComplete 이면 타임아웃을 지웠을 때 시험이
        // 실패하는 게 아니라 매달린다 — 안 무는 검증과 구별이 안 된다.
        StepVerifier.withVirtualTime(leadership::renew)
                .thenAwait(ATTEMPT)
                .expectComplete()
                .verify(BLOCK);
    }

    @Test
    @DisplayName("리더였다가_빈_응답을_받으면_리스가_지나고_내려온다")
    void 리더였다가_빈_응답을_받으면_리스가_지나고_내려온다() {
        // 빈 응답은 오류가 아니라 조용한 실패다. **리더가 아닌 상태에서 시작하면
        // 아무것도 못 잰다** — 처리를 지워도 여전히 거짓이라 통과한다.
        Leadership leadership = 리더가_된다(Mono::empty);

        leadership.renew().block(BLOCK);
        assertThat(leadership.isLeader()).isTrue();

        시간을_흘린다(LEASE);
        assertThat(leadership.isLeader()).isFalse();
    }

    @Test
    @DisplayName("확인_시각은_응답이_아니라_물으러_간_시각이다")
    void 확인_시각은_응답이_아니라_물으러_간_시각이다() {
        // 서버가 리스를 다시 건 것은 왕복 중 어느 시점이다. 응답 시각으로 재면
        // 왕복 시간만큼 남은 리스를 과대평가한다.
        Leadership leadership = leadership(() -> {
            시간을_흘린다(Duration.ofMillis(400));
            return 내_락();
        });

        leadership.renew().block(BLOCK);

        시간을_흘린다(LEASE.minusMillis(400));
        assertThat(leadership.isLeader()).isFalse();
    }

    @Test
    @DisplayName("해제_뒤에는_다시_리더가_되지_않는다")
    void 해제_뒤에는_다시_리더가_되지_않는다() {
        // 락만 비우고 종료 표시를 안 하면, 종료 중 스케줄러가 한 틱만 더 돌아도
        // **죽는 노드가 방금 비운 락을 다시 잡는다.** 다음 리더가 리스 만료를
        // 기다리게 되어 해제한 이득이 사라진다.
        Leadership leadership = leadership(LeadershipTest::내_락);
        leadership.renew().block(BLOCK);

        leadership.release().block(BLOCK);
        leadership.renew().block(BLOCK);

        assertThat(leadership.isLeader()).isFalse();
    }

    @Test
    @DisplayName("해제하면_리더에서_내려온다")
    void 해제하면_리더에서_내려온다() {
        AtomicInteger 해제 = new AtomicInteger();
        Leadership leadership = leadership(LeadershipTest::내_락,
                () -> Mono.fromRunnable(해제::incrementAndGet));
        leadership.renew().block(BLOCK);

        leadership.release().block(BLOCK);

        assertThat(해제).hasValue(1);
        assertThat(leadership.isLeader()).isFalse();
    }

    @Test
    @DisplayName("리스가_지나_내려온_뒤에도_해제를_부른다")
    void 리스가_지나_내려온_뒤에도_해제를_부른다() {
        // **로컬 상태로 거르면 안 된다.** 리더가 아니라는 것은 "남의 락" 과
        // "리스가 지나 내려왔지만 서버 락은 아직 내 것" 을 둘 다 뜻한다.
        // 뒤엣것에서 안 지우면 정상 종료인데도 다음 리더가 리스 만료를 기다린다.
        AtomicInteger 해제 = new AtomicInteger();
        Leadership leadership = leadership(LeadershipTest::내_락,
                () -> Mono.fromRunnable(해제::incrementAndGet));
        leadership.renew().block(BLOCK);
        시간을_흘린다(LEASE);
        assertThat(leadership.isLeader()).isFalse();

        leadership.release().block(BLOCK);

        assertThat(해제).hasValue(1);
    }

    @Test
    @DisplayName("해제를_두_번_불러도_한_번만_지운다")
    void 해제를_두_번_불러도_한_번만_지운다() {
        AtomicInteger 해제 = new AtomicInteger();
        Leadership leadership = leadership(LeadershipTest::내_락,
                () -> Mono.fromRunnable(해제::incrementAndGet));
        leadership.renew().block(BLOCK);

        leadership.release().block(BLOCK);
        leadership.release().block(BLOCK);

        assertThat(해제).hasValue(1);
    }

    @Test
    @DisplayName("종료_표시보다_먼저_출발한_획득은_다시_지운다")
    void 종료_표시보다_먼저_출발한_획득은_다시_지운다() {
        // 종료 표시는 이미 출발한 명령을 못 되돌린다. 그대로 두면 **죽는 노드가
        // 락을 쥔 채 나가고** 다음 리더가 리스 만료를 기다린다.
        AtomicInteger 해제 = new AtomicInteger();
        AtomicReference<Leadership> 자기 = new AtomicReference<>();
        // 획득이 나가 있는 동안 종료가 끼어든 모양이다. 조립 시점 검사로는 못 막는다.
        Leadership leadership = leadership(() -> {
            자기.get().release().block(BLOCK);
            return 내_락();
        }, () -> Mono.fromRunnable(해제::incrementAndGet));
        자기.set(leadership);

        leadership.renew().block(BLOCK);

        assertThat(leadership.isLeader()).isFalse();
        assertThat(해제).hasValue(2);
        assertThat(로그_메시지(Level.WARN)).anyMatch(m -> m.contains("종료 뒤에 락을 잡았다"));
    }

    @Test
    @DisplayName("늦게_도착한_옛_판이_확인_시각을_되돌리지_않는다")
    void 늦게_도착한_옛_판이_확인_시각을_되돌리지_않는다() {
        // **두 판이 겹쳐야 이걸 잰다.** 한 판씩 순서대로 돌리면 옛 판이라는 것이
        // 성립하지 않아, 되돌려도 안 죽는 시험이 된다.
        Duration 긴_리스 = Duration.ofMinutes(10);
        Duration 절반 = Duration.ofMinutes(5);
        Sinks.One<LeaderLock> 옛_판 = Sinks.one();
        AtomicInteger 호출 = new AtomicInteger();
        Leadership leadership = Leadership.of("node-1", 긴_리스, Duration.ofMinutes(2),
                () -> 호출.incrementAndGet() == 1 ? 옛_판.asMono() : 내_락(),
                Mono::empty, ticker::get);

        leadership.renew().subscribe();
        시간을_흘린다(절반);
        leadership.renew().block(BLOCK);
        옛_판.tryEmitValue(LeaderLock.mine("node-1", 긴_리스.toMillis(), 1));

        시간을_흘린다(절반);

        assertThat(leadership.isLeader()).isTrue();
    }

    @Test
    @DisplayName("리더로_있던_시간이_기동_시각부터_세지_않는다")
    void 리더로_있던_시간이_기동_시각부터_세지_않는다() {
        // 상태를 먼저 공개하고 시각을 나중에 쓰면, 그 사이에 읽는 쪽이 0 을 봐서
        // 부팅 이후 전체를 리더로 있던 시간으로 찍는다.
        Leadership leadership = leadership(LeadershipTest::내_락);
        leadership.renew().block(BLOCK);
        시간을_흘린다(Duration.ofSeconds(2));

        leadership.release().block(BLOCK);

        assertThat(로그_인자(Level.INFO, "리더에서 내려왔다")).singleElement()
                .satisfies(인자 -> assertThat(인자[0]).isEqualTo(2L));
    }

    @Test
    @DisplayName("해제_확인은_조립이_아니라_구독_시점에_한다")
    void 해제_확인은_조립이_아니라_구독_시점에_한다() {
        // 조립 때 확인하면 부르는 순간과 구독 사이가 창이 된다. 그 사이에 리더가
        // 되면 락을 쥔 채 종료한다.
        AtomicInteger 해제 = new AtomicInteger();
        Leadership leadership = leadership(LeadershipTest::내_락,
                () -> Mono.fromRunnable(해제::incrementAndGet));

        Mono<Void> 해제_요청 = leadership.release();
        leadership.renew().block(BLOCK);
        해제_요청.block(BLOCK);

        assertThat(해제).hasValue(1);
    }

    @Test
    @DisplayName("해제가_터져도_리더에서_내려온다")
    void 해제가_터져도_리더에서_내려온다() {
        Leadership leadership = leadership(LeadershipTest::내_락,
                () -> Mono.error(new IllegalStateException("끊겼다")));
        leadership.renew().block(BLOCK);

        leadership.release().block(BLOCK);

        assertThat(leadership.isLeader()).isFalse();
    }

    @Test
    @DisplayName("해제가_취소돼도_리더에서_내려온다")
    void 해제가_취소돼도_리더에서_내려온다() {
        // 종료 중에는 구독이 뜯길 수 있다.
        Leadership leadership = leadership(LeadershipTest::내_락, Mono::never);
        leadership.renew().block(BLOCK);

        Disposable 구독 = leadership.release().subscribe();
        구독.dispose();

        assertThat(leadership.isLeader()).isFalse();
    }

    @Test
    @DisplayName("해제_로그에_리더로_있던_시간이_있다")
    void 해제_로그에_리더로_있던_시간이_있다() {
        // 스플릿 브레인을 사후에 조사할 때 필요한 값이 정확히 "얼마나 오래
        // 리더였나" 다.
        Leadership leadership = leadership(LeadershipTest::내_락);
        leadership.renew().block(BLOCK);
        시간을_흘린다(Duration.ofSeconds(7));

        leadership.release().block(BLOCK);

        assertThat(로그_인자(Level.INFO, "리더에서 내려왔다")).singleElement()
                .satisfies(인자 -> assertThat(인자[0]).isEqualTo(7L));
    }

    @Test
    @DisplayName("전환은_바뀔_때만_찍는다")
    void 전환은_바뀔_때만_찍는다() {
        Leadership leadership = leadership(LeadershipTest::내_락);

        leadership.renew().block(BLOCK);
        leadership.renew().block(BLOCK);
        leadership.renew().block(BLOCK);

        assertThat(로그_메시지(Level.INFO)).filteredOn(m -> m.contains("리더가 됐다")).hasSize(1);
    }

    @Test
    @DisplayName("성공한_판은_실패로_안_찍힌다")
    void 성공한_판은_실패로_안_찍힌다() {
        // 오류를 전부 삼켜 루프를 살리는 구조라, 성공 경로에 버그가 생겨도
        // "리더 확인 실패" 한 줄로 조용히 지나간다. 그 한 줄이 유일한 구별점이다.
        Leadership leadership = leadership(LeadershipTest::내_락);

        leadership.renew().block(BLOCK);

        assertThat(leadership.isLeader()).isTrue();
        assertThat(로그_메시지(Level.WARN)).noneMatch(m -> m.contains("리더 확인 실패"));
    }

    @Test
    @DisplayName("연장_실패_경고는_이어지는_동안_한_번만_찍는다")
    void 연장_실패_경고는_이어지는_동안_한_번만_찍는다() {
        // 연장은 리스의 1/4 마다 돈다. 매 판 찍으면 5분 단절에 노드마다 수백 줄이고,
        // 여러 노드가 동시에 겪는 일이라 그만큼 곱해진다.
        Leadership leadership = leadership(() -> Mono.error(new IllegalStateException("끊겼다")));

        leadership.renew().block(BLOCK);
        leadership.renew().block(BLOCK);
        leadership.renew().block(BLOCK);

        assertThat(로그_메시지(Level.WARN)).filteredOn(m -> m.contains("리더 확인 실패")).hasSize(1);
    }

    @Test
    @DisplayName("실패가_걷히면_복구를_찍고_다시_경고할_수_있다")
    void 실패가_걷히면_복구를_찍고_다시_경고할_수_있다() {
        // **두 판 이상 실패해야 판 수를 잴 수 있다.** 한 판이면 어떤 값을 찍어도
        // 1 이라 세는 것과 안 세는 것이 같아 보인다. 그리고 **두 번 회복해야**
        // 세고 나서 비우는지를 잰다. 안 비우면 다음 번에 누적치를 찍는다.
        AtomicInteger 호출 = new AtomicInteger();
        Leadership leadership = leadership(() -> {
            int n = 호출.incrementAndGet();
            return n == 3 || n == 5 ? 내_락() : Mono.error(new IllegalStateException("끊겼다"));
        });

        leadership.renew().block(BLOCK);
        leadership.renew().block(BLOCK);
        시간을_흘린다(Duration.ofSeconds(3));
        leadership.renew().block(BLOCK);
        leadership.renew().block(BLOCK);
        시간을_흘린다(Duration.ofSeconds(1));
        leadership.renew().block(BLOCK);

        // 지속 시간만 남기면 한 판이 실패한 것인지 수백 판인지 사후에 못 가린다.
        assertThat(로그_인자(Level.INFO, "리더 확인 복구")).hasSize(2)
                .satisfies(목록 -> {
                    assertThat(목록.get(0)).containsExactly(3L, 2, "node-1");
                    assertThat(목록.get(1)).containsExactly(1L, 1, "node-1");
                });
        assertThat(로그_메시지(Level.WARN)).filteredOn(m -> m.contains("리더 확인 실패")).hasSize(2);
    }

    @Test
    @DisplayName("동시에_확인해도_리더_전환_로그는_한_번만_찍는다")
    void 동시에_확인해도_리더_전환_로그는_한_번만_찍는다() throws InterruptedException {
        // 배분 틱과 종료 경로는 서로 다른 스레드에서 들어온다.
        int 스레드 = 16;
        Leadership leadership = leadership(LeadershipTest::내_락);
        CountDownLatch 출발 = new CountDownLatch(1);
        CountDownLatch 도착 = new CountDownLatch(스레드);
        List<Throwable> 터진것 = Collections.synchronizedList(new ArrayList<>());
        ExecutorService 풀 = Executors.newFixedThreadPool(스레드);
        try {
            for (int i = 0; i < 스레드; i++) {
                풀.execute(() -> {
                    // **삼키면 안 된다.** 16개 중 15개가 터져도 카운트는 내려가고
                    // 로그는 한 줄이라, 통과한 것처럼 보인다.
                    try {
                        출발.await();
                        leadership.renew().block(BLOCK);
                    } catch (Throwable t) {
                        터진것.add(t);
                    } finally {
                        도착.countDown();
                    }
                });
            }
            출발.countDown();
            assertThat(도착.await(10, TimeUnit.SECONDS)).isTrue();
        } finally {
            풀.shutdownNow();
        }

        assertThat(터진것).isEmpty();
        assertThat(로그_메시지(Level.INFO)).filteredOn(m -> m.contains("리더가 됐다")).hasSize(1);
    }

    @Test
    @DisplayName("종료_뒤에는_레디스를_더_치지_않는다")
    void 종료_뒤에는_레디스를_더_치지_않는다() {
        // 종료 검사를 빼도 뒤처리가 정합성은 살린다. 대신 매 판 레디스를 치고
        // 경고가 폭포가 된다 — 죽은 노드가 남기는 소음이 진짜 장애를 가린다.
        AtomicInteger 획득 = new AtomicInteger();
        Leadership leadership = leadership(() -> {
            획득.incrementAndGet();
            return 내_락();
        });
        leadership.renew().block(BLOCK);
        leadership.release().block(BLOCK);

        leadership.renew().block(BLOCK);
        leadership.renew().block(BLOCK);

        assertThat(획득).hasValue(1);
    }

    @Test
    @DisplayName("리더였던_적_없으면_해제해도_내려왔다고_안_찍는다")
    void 리더였던_적_없으면_해제해도_내려왔다고_안_찍는다() {
        // 리더 시작 시각이 0 인 채로 찍으면 기동 이후 전체를 리더로 있던 시간으로
        // 보고한다. 사후 조사에서 그 값이 거짓이면 owner 를 남긴 뜻이 없다.
        Leadership leadership = leadership(LeadershipTest::내_락);

        leadership.release().block(BLOCK);

        assertThat(로그_메시지(Level.INFO)).noneMatch(m -> m.contains("리더에서 내려왔다"));
    }

    @Test
    @DisplayName("리더였던_적_없으면_남이_쥐어도_잃었다고_안_찍는다")
    void 리더였던_적_없으면_남이_쥐어도_잃었다고_안_찍는다() {
        // 팔로워는 매 판 이 응답을 받는다. 무조건 찍으면 노드마다 분당 수백 줄이다.
        Leadership leadership = leadership(() -> Mono.just(LeaderLock.heldBy("node-2", 1_500)));

        leadership.renew().block(BLOCK);
        leadership.renew().block(BLOCK);

        assertThat(leadership.isLeader()).isFalse();
        assertThat(로그_메시지(Level.INFO)).noneMatch(m -> m.contains("리더를 잃었다"));
    }

    @Test
    @DisplayName("종료_뒤_남이_쥐었다는_응답이_종료를_지우지_않는다")
    void 종료_뒤_남이_쥐었다는_응답이_종료를_지우지_않는다() {
        // 종료와 겹친 판이 "남이 쥐었다" 로 돌아올 때 상태를 통째로 덮으면
        // **종료 표시가 지워진다.** 그 뒤 연장이 죽는 노드를 다시 리더로 만든다.
        AtomicReference<Leadership> 자기 = new AtomicReference<>();
        AtomicInteger 호출 = new AtomicInteger();
        Leadership leadership = leadership(() -> {
            if (호출.incrementAndGet() == 1) {
                자기.get().release().block(BLOCK);
                return Mono.just(LeaderLock.heldBy("node-2", 1_500));
            }
            return 내_락();
        });
        자기.set(leadership);

        leadership.renew().block(BLOCK);
        leadership.renew().block(BLOCK);

        assertThat(leadership.isLeader()).isFalse();
    }

    @Test
    @DisplayName("소유자를_모르는_응답도_읽을_수_있게_찍는다")
    void 소유자를_모르는_응답도_읽을_수_있게_찍는다() {
        // 경합에서 진 뒤 다시 읽는 찰나에 락이 풀리면 스크립트가 빈 소유자와
        // 음수 TTL 을 돌려준다. 빈 값을 그대로 찍으면 문장에 구멍이 생긴다.
        Leadership leadership = 리더가_된다(() -> Mono.just(LeaderLock.heldBy("", -2)));

        leadership.renew().block(BLOCK);

        assertThat(로그_인자(Level.INFO, "리더를 잃었다")).singleElement()
                .satisfies(인자 -> assertThat(인자[1]).isEqualTo("(그 사이 풀렸다)"));
    }

    @Test
    @DisplayName("소유자_ID_는_기동마다_다르다")
    void 소유자_ID_는_기동마다_다르다() {
        // 고정하면 재기동한 자신을 이전 소유자로 오인해, 죽기 전에 잡아 둔 락을
        // 새 프로세스가 자기 것으로 알고 연장한다.
        assertThat(Leadership.newOwnerId()).isNotEqualTo(Leadership.newOwnerId());
    }

    @Test
    @DisplayName("설정이_잘못되면_안_뜬다")
    void 설정이_잘못되면_안_뜬다() {
        // **어느 가드가 던졌는지까지 본다.** 메시지를 안 보면 리스 검사를 통째로
        // 지워도 비율 검사가 대신 던져서 통과한다.
        assertThatThrownBy(() -> Leadership.of(null, LEASE, ATTEMPT, LeadershipTest::내_락, Mono::empty))
                .isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith("ownerId");
        assertThatThrownBy(() -> Leadership.of(" ", LEASE, ATTEMPT, LeadershipTest::내_락, Mono::empty))
                .isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith("ownerId");
        assertThatThrownBy(() -> Leadership.of("node-1", Duration.ZERO, ATTEMPT,
                LeadershipTest::내_락, Mono::empty))
                .isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith("lease");
        assertThatThrownBy(() -> Leadership.of("node-1", LEASE.negated(), ATTEMPT,
                LeadershipTest::내_락, Mono::empty))
                .isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith("lease");
        assertThatThrownBy(() -> Leadership.of("node-1", LEASE, Duration.ZERO,
                LeadershipTest::내_락, Mono::empty))
                .isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith("attemptTimeout");
        assertThatThrownBy(() -> Leadership.of("node-1", LEASE, ATTEMPT.negated(),
                LeadershipTest::내_락, Mono::empty))
                .isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith("attemptTimeout");
        assertThatThrownBy(() -> Leadership.of("node-1", null, ATTEMPT, LeadershipTest::내_락, Mono::empty))
                .isInstanceOf(NullPointerException.class).hasMessageStartingWith("lease");
        assertThatThrownBy(() -> Leadership.of("node-1", LEASE, null, LeadershipTest::내_락, Mono::empty))
                .isInstanceOf(NullPointerException.class).hasMessageStartingWith("attemptTimeout");
        assertThatThrownBy(() -> Leadership.of("node-1", LEASE, ATTEMPT, null, Mono::empty))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("acquire");
        assertThatThrownBy(() -> Leadership.of("node-1", LEASE, ATTEMPT, LeadershipTest::내_락, null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("release");
    }

    @Test
    @DisplayName("한_판이_리스의_오분의_일을_넘으면_안_뜬다")
    void 한_판이_리스의_오분의_일을_넘으면_안_뜬다() {
        // 명령 타임아웃이 리스보다 길면 성공해도 도착할 때 이미 만료다. 아무도
        // 리더가 못 되고, 예외도 로그도 안 난다.
        // 회복하는 판까지 넣으면 시도는 다섯 번 들어간다. 넷으로 세면 계획서가
        // 깨진다고 못박은 값을 생성자가 통과시킨다.
        Duration 딱_맞음 = LEASE.dividedBy(5);
        assertThat(Leadership.of("node-1", LEASE, 딱_맞음, LeadershipTest::내_락, Mono::empty)
                .ownerId()).isEqualTo("node-1");

        assertThatThrownBy(() -> Leadership.of("node-1", LEASE, 딱_맞음.plusNanos(1),
                LeadershipTest::내_락, Mono::empty))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("소유자를_모르는_락은_만들_수_없다")
    void 소유자를_모르는_락은_만들_수_없다() {
        // 못 잡았어도 누가 쥐었는지는 사실이다. 버리면 "그럼 누가 리더였나" 에
        // 답할 수 없다.
        assertThatThrownBy(() -> new LeaderLock(false, null, ONE_SECOND_MILLIS, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
    /**
     * <b>판 번호는 리더일 때만 나간다</b> (CY-766).
     *
     * <p>되돌릴 수 없는 쓰기가 이 번호를 들고 나간다. 리더가 아닌데 옛 번호를
     * 그대로 내주면 강등된 노드가 자기가 쥐었던 판인 척 줄을 지운다.
     */
    @Test
    @DisplayName("리더가_아니면_판_번호가_0이다")
    void 리더가_아니면_판_번호가_0이다() {
        AtomicBoolean 내것 = new AtomicBoolean(true);
        Leadership leadership = Leadership.of("node-1", LEASE, ATTEMPT,
                () -> Mono.just(내것.get()
                        ? LeaderLock.mine("node-1", LEASE.toMillis(), 777)
                        : LeaderLock.heldBy("node-2", LEASE.toMillis())),
                () -> Mono.empty());

        leadership.renew().block();
        assertThat(leadership.fence()).as("리더일 때").isEqualTo(777);

        내것.set(false);
        leadership.renew().block();

        assertThat(leadership.isLeader()).isFalse();
        assertThat(leadership.fence()).as("강등되면 번호를 안 내준다").isZero();
    }

    /**
     * <b>리스가 지나면 판 번호를 안 내준다</b> (CY-766).
     *
     * <p>울타리가 막으려던 주 시나리오다 — 갱신 루프가 멎거나 STW 로 리스가
     * 지났는데 아직 강등 신호를 못 받은 노드. 그때 옛 번호를 그대로 내주면
     * 그 노드가 새 리더의 줄을 지운다.
     */
    @Test
    @DisplayName("리스가_지나면_판_번호를_안_내준다")
    void 리스가_지나면_판_번호를_안_내준다() {
        Leadership leadership = 리더가_된다(Mono::never);
        assertThat(leadership.fence()).as("전제 — 잡았을 때는 번호가 있다").isPositive();

        시간을_흘린다(LEASE.plusSeconds(1));

        assertThat(leadership.isLeader()).isFalse();
        assertThat(leadership.fence()).as("확인 없이 늙은 번호는 안 내준다").isZero();
    }

    /**
     * <b>늦게 온 옛 판의 응답이 번호를 되돌리지 않는다</b> (CY-766).
     *
     * <p>되돌아가면 멀쩡한 리더가 비가역 쓰기를 옛 번호로 내보낸다. 확인 시각을
     * 안 미는 것과 같은 이유인데, 번호만 무방비였다.
     */
    @Test
    @DisplayName("늦게_온_응답이_판_번호를_안_되돌린다")
    void 늦게_온_응답이_판_번호를_안_되돌린다() {
        AtomicInteger 호출 = new AtomicInteger();
        Leadership leadership = leadership(() -> switch (호출.incrementAndGet()) {
            case 1 -> Mono.just(LeaderLock.mine("node-1", LEASE.toMillis(), 500));
            // 겹친 판 중 늦게 도착한 옛 판이 작은 번호를 들고 온다.
            default -> Mono.just(LeaderLock.mine("node-1", LEASE.toMillis(), 100));
        });

        leadership.renew().block(BLOCK);
        leadership.renew().block(BLOCK);

        assertThat(leadership.fence()).as("뒤로 안 민다").isEqualTo(500);
    }

    /** 잡기 전에는 번호가 없다. 0 이면 울타리가 전부 거절한다 — 안전한 쪽이다. */
    @Test
    @DisplayName("잡기_전에는_판_번호가_0이다")
    void 잡기_전에는_판_번호가_0이다() {
        Leadership leadership = Leadership.of("node-1", LEASE, ATTEMPT,
                () -> Mono.just(LeaderLock.mine("node-1", LEASE.toMillis(), 5)),
                () -> Mono.empty());

        assertThat(leadership.fence()).isZero();
    }
}
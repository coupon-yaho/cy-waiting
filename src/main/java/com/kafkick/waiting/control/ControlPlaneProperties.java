package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import java.time.Duration;

/**
 * 제어 평면의 시간 예산.
 *
 * <p><b>어긋나면 안 뜨게 한다.</b> 주석으로 적어 두면 값을 바꾸는 사람이 안 읽고
 * 배분이 멎는 사고로 배운다. 특히 연장은 어긋나도 예외가 안 나고 조용히 리더가
 * 없어진다.
 *
 * @param scheduler 배분 주기
 * @param leader    리더 리스와 연장
 */
public record ControlPlaneProperties(Scheduler scheduler, Leader leader, Capacity capacity) {

    public ControlPlaneProperties {
        if (scheduler == null || leader == null || capacity == null) {
            throw new IllegalArgumentException("scheduler·leader·capacity 는 필수다");
        }
        // 신선도가 틱보다 짧으면 한 틱만 밀려도 전 인스턴스가 낡음이 된다.
        // 그러면 하한으로 떨어져 아무 문제 없는 쿠폰까지 줄을 세운다.
        if (capacity.freshness().compareTo(scheduler.tick()) <= 0) {
            throw new IllegalArgumentException(
                    "신선도는 틱보다 길어야 한다: freshness=%s tick=%s"
                            .formatted(capacity.freshness(), scheduler.tick()));
        }
        // 리스가 한 틱도 못 버티면 회차를 도는 도중에 리더십을 잃는다. 그 회차의
        // 배분은 나갔는데 다음 리더도 같은 회차를 돌아 두 배가 나간다.
        if (leader.lease().compareTo(scheduler.tick()) <= 0) {
            throw new IllegalArgumentException(
                    "리스는 틱보다 길어야 한다: lease=%s tick=%s"
                            .formatted(leader.lease(), scheduler.tick()));
        }
    }

    public static ControlPlaneProperties defaults() {
        return new ControlPlaneProperties(
                // 유예 90틱 = 90초. 폴링 최대 간격 60초의 1.5배다 (7.3.2).
                new Scheduler(Duration.ofSeconds(1), Duration.ofSeconds(3), 1, 90),
                new Leader(Duration.ofSeconds(2), Duration.ofMillis(300), Duration.ofMillis(100)),
                new Capacity(Duration.ofSeconds(60), Duration.ofSeconds(3), 5, 10_000, 3, 1));
    }

    /**
     * @param rampUp         찬 인스턴스가 제 몫을 내기까지의 시간
     * @param freshness      이보다 오래된 보고는 안 센다
     * @param floor          신선한 보고가 하나도 없을 때 쓸 값
     * @param perInstanceCap 인스턴스 하나가 주장할 수 있는 상한
     * @param rampDownTicks  노드 수 감소를 확정하기까지의 연속 관측 수
     * @param expectedNodes  첫 관측 전에 쓸 분모
     */
    public record Capacity(Duration rampUp, Duration freshness, long floor, long perInstanceCap,
            int rampDownTicks, int expectedNodes) {

        public Capacity {
            Durations.requirePositive(rampUp, "rampUp");
            Durations.requirePositive(freshness, "freshness");
            if (floor < 0) {
                throw new IllegalArgumentException("floor 는 0 이상이어야 한다: %d".formatted(floor));
            }
            if (perInstanceCap < 1) {
                throw new IllegalArgumentException(
                        "perInstanceCap 은 1 이상이어야 한다: %d".formatted(perInstanceCap));
            }
            if (rampDownTicks < 1) {
                throw new IllegalArgumentException(
                        "rampDownTicks 는 1 이상이어야 한다: %d".formatted(rampDownTicks));
            }
            if (expectedNodes < 1) {
                throw new IllegalArgumentException(
                        "expectedNodes 는 1 이상이어야 한다: %d".formatted(expectedNodes));
            }

            // **하한이 전면 차단과 같으면 안 된다.** 노드당 몫에 유휴 비율을 곱한
            // 값이 1 을 못 넘으면 한산한 쿠폰이 첫 요청부터 줄을 선다 — 하한을
            // 둔 이유가 사라진다. 노드를 늘리면서 하한을 그대로 두는 것이 그 길이다.
            if (floor > 0 && floor < (long) expectedNodes * CapacityCollector.IDLE_DIVISOR) {
                throw new IllegalArgumentException(
                        "하한이 노드 수를 못 받친다 — 한산 통과 상한이 0 이 된다: floor=%d nodes=%d"
                                .formatted(floor, expectedNodes));
            }
        }
    }

    /**
     * @param tick           배분 주기. 완료 후 지연이다
     * @param firstTickDelay 첫 회차를 미루는 시간. 가용량 보고가 모이기를 기다린다
     * @param shards         큐 샤드 수
     */
    public record Scheduler(Duration tick, Duration firstTickDelay, int shards,
            int soldOutGraceTicks) {

        public Scheduler {
            Durations.requirePositive(tick, "tick");
            if (firstTickDelay == null || firstTickDelay.isNegative()) {
                throw new IllegalArgumentException(
                        "firstTickDelay 는 음수일 수 없다: %s".formatted(firstTickDelay));
            }
            // **하나만 받는다.** 여럿이면 몫을 샤드에 나눠 각각 적용해야 하는데
            // 지금 적용은 0번에만 나간다. 그러면 나머지 샤드의 줄은 영원히 안
            // 빠지고 아무 오류도 안 난다 — 산문으로 지킬 일이 아니다.
            //
            // 큐 등록 상한도 여기 걸린다. 상한은 쿠폰 전체의 수인데 스크립트는
            // 자기 샤드만 세므로, 이 줄을 풀면 실효 상한이 샤드 수만큼 커진다.
            if (shards != 1) {
                throw new IllegalArgumentException(
                        "shards 는 아직 1 만 지원한다 — 샤드별 적용이 없다: %d".formatted(shards));
            }
            // **폴링 최대 간격보다 길어야 한다** (7.3.2). 마지막 폴링이 언제
            // 오는지를 정하는 것은 낡음 한계가 아니라 **우리가 클라이언트에게
            // 준 간격**이다. 먼 밴드는 60초를 받으므로, 그보다 짧으면 그 사람이
            // 다시 왔을 때 줄이 이미 없다.
            if (tick.multipliedBy(soldOutGraceTicks)
                    .compareTo(PollIntervalPolicy.maxInterval()) <= 0) {
                throw new IllegalArgumentException(
                        "매진 유예는 폴링 최대 간격보다 길어야 한다: 유예=%s 간격=%s"
                                .formatted(tick.multipliedBy(soldOutGraceTicks),
                                        PollIntervalPolicy.maxInterval()));
            }
        }
    }

    /**
     * 마지막 성공부터 회복하는 회차가 끝날 때까지 들어가는 시도의 수.
     *
     * <p>세 번 놓쳐도 락을 안 잃으려면 그 뒤에 오는 회복 회차까지 리스 안에 들어와야
     * 한다. 세 번째 실패가 끝나는 시점에서 끊으면 지연 하나와 시도 하나가 빠진다.
     */
    private static final int ATTEMPTS = 4;

    /**
     * @param lease      리더 락 수명
     * @param attempt    연장 한 회차의 상한. 멈춤을 오류로 바꾼다
     * @param renewDelay 회차가 끝난 뒤 다음 회차까지의 지연
     */
    public record Leader(Duration lease, Duration attempt, Duration renewDelay) {

        public Leader {
            Durations.requirePositive(lease, "lease");
            Durations.requirePositive(attempt, "attempt");
            if (renewDelay == null || renewDelay.isNegative()) {
                throw new IllegalArgumentException(
                        "renewDelay 는 음수일 수 없다: %s".formatted(renewDelay));
            }
            Duration budget = attempt.plus(renewDelay).multipliedBy(ATTEMPTS).plus(attempt);
            if (budget.compareTo(lease) > 0) {
                throw new IllegalArgumentException(
                        "연장 예산이 리스를 넘는다 — 세 번 놓치기 전에 락을 잃는다: %s > %s"
                                .formatted(budget, lease));
            }
        }
    }

}

# Phase 2 — Domain Core

**선행**: Phase 1 · **상세 수준**: 완전 명세

---

## Goal

> **한산한 쿠폰이 대기열 없이 통과한다 (R1).**
> 그것이 단위 테스트로 증명되고, 판정·배분 로직에 크레딧 초과 배분과 순위 역행이 없다.

| 통과 수치 | |
|---|---|
| 순수 도메인 브랜치 커버리지 | **100%** |
| 뮤테이션 생존율 (PIT) | **≤ 10%** |
| 크레딧 초과 배분 (무작위 10만 회) | **0건** |
| 순위 역행 (무작위 10만 시퀀스) | **0건** |
| 도메인의 Spring·Redis·시계 참조 | **0건** (ArchUnit) |

**이 페이즈가 이 프로젝트의 존재 이유다.** 이전 구현이 실패한 지점이 정확히 여기고,
여기가 틀리면 나머지 8개 페이즈가 틀린 것을 정교하게 감쌀 뿐이다.

---

## 1. 왜 순수 계층으로 먼저 만드는가

순수해야 브랜치 100% 와 뮤테이션 테스트가 가능하고, 그래야 위 통과 수치를
보장할 수 있다. 이 계층의 오류가 곧 크레딧 초과 배분과 공정성 붕괴다.

R1 이 역전된 원인(`credit` 이 두 의미를 겸한 것)과 그 대응은 3.1절에 있다.
그 버그를 덮은 테스트가 도달 불가능한 상태를 픽스처로 썼다는 점은 3.7절.

---

## 2. 만드는 것

```
com.kafkick.waiting.domain/
├── coupon/     QueueMode  RuntimeState  CouponState  SnapshotMeta
├── admission/  AdmissionDecision  AdmissionDecider  SecondWindowLimiter
├── allocation/ CouponDemand  Grant  FairShareAllocator  ShardSplitter  CreditSmoother
└── queue/      PollIntervalPolicy  PollBudgetPlanner  RankEstimator
```

전부 POJO. Spring 애노테이션도, `Mono`도, `Instant.now()`도 없다.

**영속 계층이 없다.** `cy-be` 는 `storage/db/coupon/{entity,mapper,repository}` 로 3단
분리하는데, 게이트웨이는 **재고를 차감하지 않으므로**(비목표) 엔티티도 리포지터리도
없다. Redis 어댑터 하나가 전부다 (Phase 3). JPA 를 넣지 않는다.

**의존 방향은 `cy-be` 와 같다** — 도메인이 아무것도 의존하지 않고 나머지가 도메인을
본다. 저쪽은 모듈 경계로 강제하고 우리는 **ArchUnit 으로 강제한다**(2.8.1) —
게이트웨이는 배포 단위가 하나라 모듈로 쪼개지 않는다 (O-2).

---

## 3. 설계 결정

태스크가 이 절을 근거로 참조한다. 여기를 바꾸면 참조하는 태스크를 함께 고친다.

### 3.1 2계층 리미터 — R1의 구현

```
tier 1 — 쿠폰별:   한산한 쿠폰(IDLE)  →  globalCredit / N × idleCreditRatio
tier 2 — 노드 전역: 전 쿠폰 합산      →  globalCredit / N
```

`globalCredit`은 `SnapshotMeta`에 실려 오므로 추가 조회가 없다.

**tier 1 은 한산한 쿠폰 전용이다.** 막는 것은 *"한산한 쿠폰 하나가 노드의 유휴
예산을 독식하는 것"*이지 경합 쿠폰 간 공정성이 아니다 — 그건 `FairShareAllocator`
가 틱마다 `credit` 을 나누는 것으로 이미 끝난다.

**`idleCreditRatio` 는 tier 1 에만 건다** (기본 0.7 → Phase 9 통과 후 1.0).
tier 2 에 걸면 노드 전체가 조여져 경합 쿠폰까지 굶는다.

**두 상한이 같으면 tier 1 은 죽은 분기가 된다.** tier 2 는 tier 1 의 카운터를
항상 포함하므로 상한이 같으면 tier 2 가 먼저 차거나 동시에 찬다. 비율이 그래서 필요하다.

<a id="admit-once"></a>
#### 토큰 경로에 쿠폰별 상한을 걸지 않는 이유 (B-14)

**입장은 경계에서 한 번만 판정한다.** 크레딧은 이미 한 번 쓰였다.

```
FairShareAllocator 가 쿠폰별 credit 배분   ← 쿠폰 간 공정성은 여기서 결정된다
  → 그만큼 admitted 임계가 올라간다
  → 폴링해 온 사람이 토큰을 받는다 (지연 발급, D-3)
  → 토큰을 들고 오면 통과                  ← 여기서 credit 을 또 보면 이중 차감
```

토큰은 **"당신 차례가 왔다"는 허가증**이다. 그걸 받고 온 사람을 쿠폰별 상한으로
다시 거절하면 허가가 "아마도"가 된다 (F8 과 같은 이유).

**tier 2 는 대상이 다르다.** 쿠폰 간 공정성이 아니라 **이 노드가 초당 감당할 양**을
지킨다. F8 이 잡은 위험은 불공정이 아니라 **시간적 쏠림**이다 — 토큰 TTL 이 60초라
여러 틱치가 한 초에 몰린다. 핫 쿠폰이 tier 2 를 많이 쓰는 것은 `credit` 이 큰
만큼 정당하고, 몰리면 짧은 `Retry-After` 로 미룬다.

<a id="atomic-acquire"></a>
**두 리미터는 하나의 원자 판정이다.** 순서대로 `tryAcquire` 하면 앞엣것을 소비한
뒤 뒤엣것이 거부할 때 **통과하지 않은 요청이 예산을 깎는다.**

```
tryAcquireAll(tier1, tier2):
  둘 다 여유 있음  →  둘 다 차감, PASS_UNDER_CAP
  tier1 부족       →  아무것도 안 깎음, ENQUEUE_RATE_COUPON
  tier2 부족       →  아무것도 안 깎음, ENQUEUE_RATE_GLOBAL
  키 자리 없음     →  아무것도 안 깎음, ENQUEUE_KEY_SATURATED
```

**키 자리 부족은 예산 고갈과 다른 값이다.** 같이 묶으면 운영자가 쿠폰이나
노드를 조이는데, 조여야 할 것은 리미터의 키 상한이다. 예산을 먼저 보고 자리를
나중에 본다 — 예산이 말랐다면 그 키는 이미 자리를 잡고 있어 자리 문제가 아니다.

전부-아니면-전무다. 부분 획득 뒤 반납하는 형태로 만들지 않는다 — 반납 누락이
곧 예산 유실이고, 그건 조용히 통과량을 갉아먹는다.

한 쿠폰이 tier 1을 독식해도 다음 쿠폰은 큐로 가고 → 활성화되고 → 다음 틱에
`FairShareAllocator`가 공정 몫을 준다. **1틱 안에 자기 교정**된다.

> **`max(1, ...)`을 쓰지 않는다.** `credit=10`·노드 20대에서 각 노드가 1을 가지면
> 총 20이 되어 크레딧의 2배가 통과한다. 몫이 0인 노드는 큐로 보낸다 — 손실이 아니라 지연이다.
> 노드 간 결정적으로 갈리도록 인스턴스 ID 해시로 일부 노드에만 1을 배정한다.

<a id="f1"></a>
### 3.2 F1 — fail-open은 줄 선 사람을 추월시키면 안 된다

기존 구현은 `dataStale`을 최우선에 두어 `runtime`을 보지 않았고, **이미 `QUEUEING`인
쿠폰에도 새 유입을 통과**시켰다.

```
스케줄러 장애 (5초)
  ├─ 큐에 5만 명이 자리를 들고 대기 (admitted 임계 정지)
  └─ 신규 유입은 fail-open 으로 백엔드 직행 → 재고 소비
회복 → 재고 소진 → 정직하게 줄 선 5만 명 전원 SOLD_OUT
```

**가용성을 위해 공정성을 희생하는 거래인데, 선착순 쿠폰에서 공정성은 제품의
존재 이유다.** 버그가 아니라 잘못된 설계 결정이다.

| 상황 | 판정 |
|---|---|
| 스냅샷 낡음 + **큐가 비어 있음** (`waiting == 0`) | fail-open — 상한 안에서 통과 |
| 스냅샷 낡음 + **큐에 사람이 있음** (`waiting > 0`) | **큐 등록** — 추월 금지 |
| 위 + Redis도 죽어 큐 등록 실패 | 그때만 fail-open (Phase 5에서 처리) |

`waiting > 0`은 마지막으로 본 스냅샷의 사실이고 그 사람들의 순번은 Redis에 남아 있다.
**상태를 모른다는 것이 추월을 정당화하지 않는다.**

<a id="f4"></a>
### 3.3 F4 — 회복 전이에서 두 리미터가 동시에 열린다

정상 경로와 fail-open 경로가 **독립 카운터**를 쓰면 `dataStale`이 뒤집히는 순간
같은 초에 양쪽 쿼터가 모두 나간다. 최대 **1.5× 버스트**.

**결정**: 리미터를 경로별로 두지 않는다. **하나의 리미터에 상한만 경로별로 전달**한다.
같은 초 윈도우를 공유하므로 경로가 바뀌어도 이미 소비한 양이 차감된다.

<a id="f8"></a>
### 3.4 F8 — `PASS_TOKEN`도 상한을 거친다

입장 토큰 TTL은 60초다. 장애 중 발급된 토큰이 축적되었다가 회복 직후 한꺼번에
들어오면 상한을 우회한다. **허가 시점과 사용 시점이 60초 벌어질 수 있다는 점**을
놓친 것이다.

토큰 통과에도 tier 2를 적용하되, 초과 시 큐가 아니라 **짧은 `Retry-After` + 429**를
준다 — 이미 차례가 온 사람을 큐 뒤로 보내면 안 된다.

### 3.5 판정 순서

```
 1. stock <= 0                        → REJECT_SOLD_OUT
 2. hasValidToken                     → tier2 통과 시 PASS_TOKEN, 초과 시 RETRY_TOKEN
 3. dataStale && !hasQueue            → failOpen (상한 내 PASS, 초과 시 REJECT_OVERLOAD)
 4. mode == OFF && !hasQueue          → PASS_BYPASS
 5. waiting > 0 && waiting >= queueCapacity
                                      → REJECT_QUEUE_FULL    ← 큐로 가는 경로보다 앞
 6. dataStale && (waiting > 0 || justEnqueued)
                                      → ENQUEUE_STALE        (F1)
 7. mode == ALWAYS                    → ENQUEUE_ALWAYS
 8. runtime != IDLE || justEnqueued    → ENQUEUE_BACKLOG      (새치기 방지)
───────────── 여기부터 한산한 쿠폰 ─────────────
 9. tryAcquireAll(tier1, tier2)       → 부족한 쪽에 따라 ENQUEUE_RATE_COUPON /
                                          ENQUEUE_RATE_GLOBAL / ENQUEUE_KEY_SATURATED
10.                                   → PASS_UNDER_CAP
```

> **`hasQueue` 는 `waiting > 0 || justEnqueued` 다.** 한 번 정의하고 세 줄이
> 같은 것을 쓴다 — 풀어 쓰면 한 줄만 고쳐지고 나머지가 갈라진다.

> **3번이 4번보다 앞이다.** 낡은 구간은 상태를 모르는 구간이고 그래서 상한이
> 있다. 꺼진 쿠폰을 앞에 두면 그것만 무제한으로 뒷단에 꽂혀 상한이 있으나
> 마나가 된다 — 실측으로 5,000건이 전부 나갔다.

> **4번의 `!hasQueue` 가 불변식 4 다.** `mode` 는 사람이 고른 값이고 `waiting` 은
> 기계가 관측한 값이라 서로 독립이다. 줄이 남아 있는데 우회시키면 신규 유입이
> 그 줄을 통째로 추월하고 재고까지 먼저 먹는다 — 6번이 낡은 스냅샷에서 막은
> 것을 여기서 뚫는 셈이다. `OFF` 는 배분에 관여하지 않으므로 남은 줄은
> 정상적으로 빠지고, 비는 순간 이 줄이 다시 산다.

> **5번의 `waiting > 0` 가 R1 이다.** 한산한 쿠폰은 `credit` 이 0 이라 용량도
> 0 이고, 이 조건이 없으면 `0 >= 0` 이 참이 되어 전원이 `REJECT_QUEUE_FULL` 로
> 간다 — 무대기 통과 경로가 통째로 막힌다.

> **줄을 세울 때 쓰는 상한은 5번이 보는 값과 다르다.** 5번은 `credit × maxEtaSec`
> 만 본다. 실제로 등록할 때는 그 값이 0 이면 **가장 낮은 배수 속도(초당 1명)를
> 가정해** `1 × maxEtaSec` 으로 갈아탄다 (`AdmissionDecider.queueCapacity`).
> **판의 크기로 재지 않는다** — 그 수는 전 노드가 공유하는 줄 길이와 비교되고,
> 무엇보다 이 구간에는 그만큼 뺄 수 있다는 근거가 없다. 배분은 줄이 있어야 나가고 줄은
> 게이트웨이가 만드는데, 0 을 그대로 상한으로 쓰면 그 고리가 닫히지 않는다.
> 5번에는 폴백을 쓰지 않는다 — 거기는 줄이 이미 선 쿠폰만 보고, 뺄 수 없다고
> 아는 줄에 더 세우느니 거절이 낫다. 이 비대칭이 연 위험은
> [AIJ-0073](../ai/journal/2026/08/AIJ-0073-idle-queue-capacity.md).

**9번은 원자 판정이고, 여기 도달하는 것은 IDLE 쿠폰뿐이다.** `I4` 의 대우로
8번 시점에 남는 것은 IDLE 아니면 `waiting>0` 이고 후자는 전부 큐로 간다.
래치까지 보면 **`IDLE && !justEnqueued`** 만 9번에 도달한다.
경합 쿠폰이 통과하는 경로는 2번(토큰)뿐이며 거기에는 쿠폰별 상한을 걸지 않는다
([3.1절](#admit-once)).

두 리미터를 순서대로 치면 앞엣것을 소비한 뒤 뒤엣것이 거부할 때 전역 예산이 샌다
([3.1절 원자 획득](#atomic-acquire)).
어느 쪽이 부족했는지는 판정값으로 구분한다 — `ENQUEUE_RATE_COUPON` 은 **그 한산한
쿠폰이 유휴 몫을 다 쓴 것**이고, `ENQUEUE_RATE_GLOBAL` 은 **노드가 다 쓴 것**이라
대응이 다르다. 전자는 그 쿠폰만 조이면 되고, 후자는 노드를 늘려야 한다.
`ENQUEUE_KEY_SATURATED` 는 셋째다 — 예산은 남았는데 리미터가 키를 더 못 들고
있는 것이라 조일 것은 쿠폰도 노드도 아니다.

**두 예산의 키는 접두사로 갈라 둔다.** 쿠폰 ID 하나가 전역 키와 같아지는 순간
두 예산이 한 카운터로 합쳐져, 다른 쿠폰의 전역 트래픽이 그 쿠폰 몫을 먹는다.

<a id="latch"></a>
**8번의 `justEnqueued` 가 전이 구멍을 막는다.** `runtime` 은 스냅샷 값이라 이 노드가
방금 큐로 보낸 사실이 돌아오기까지 한 틱이 빈다. 그사이 리미터가 리필되면 **늦게 온
요청이 9·10번을 통과해 방금 줄 선 사람을 추월한다** — 장애가 아닌 정상 경로에서
불변식 4가 깨진다.

```
t=0     A 도착 → 상한 초과 → 큐 등록 (waiting > 0 이 됨)
t=0.6s  B 도착 → 스냅샷은 아직 IDLE → 8번 통과 → 버킷 리필됨 → 10번 통과
        B 가 A 를 추월
```

`justEnqueued` 는 **이 노드가 그 쿠폰을 방금 큐로 보냈는가**를 나타낸다.
`localRank` 와 같이 **도메인이 계산하지 않고 주입받는 값**이다 — 노드 로컬 상태이고
도메인은 스냅샷과 인자만 본다.

**해제는 시간으로 한다.** 등록이 스냅샷에 반영되는 데 걸리는 시간
([`scheduler.tick` + `snapshot.fetch`](04-control-plane.md))의 두 배가 하한이다.
**스냅샷을 아직 믿는 한계(`dataStaleAfter`)도 하한이다** — 래치가 먼저 풀리면 그
뒤로도 유효한 스냅샷에 방금 세운 줄이 안 보여 그 차이가 추월 창이 된다. 둘 중
큰 쪽에 절삭 여유를 더해 만료시킨다.

**`waiting > 0` 을 봤다고 바로 풀지는 않는다.** 그 스냅샷은 방금 넣은 사람을 아직
모른다 — 다음 판에 줄이 다 빠져 한산으로 뒤집히면 그 한 명이 통째로 추월당한다.
8번이 `runtime` 으로 잡는 것은 스냅샷에 실린 줄까지이고, 마지막 한 명은 여전히
래치가 덮어야 한다.

**대신 표식을 갱신하지 않는다.** 이미 걸린 래치에 다시 찍으면, 대기 판정이 다시
표식을 부르는 닫힌 고리 때문에 트래픽이 이어지는 동안 영영 안 풀린다. 수명은
처음 찍은 시각부터 잰다 — 래치가 덮으려는 구간의 시작이 거기다.

**만료 계산은 도메인이 하지 않는다.** 도메인은 시계를 참조할 수 없다 (Goal 통과
수치 · T2.8.1). `localRank` 와 같이 **어댑터가 계산해 `boolean` 으로 주입**하고,
도메인은 그 값을 판정에만 쓴다 → Phase 5 (5.3).

**`waiting > 0` 관측만을 해제 조건으로 삼으면 안 된다.** 등록한 사람이 곧바로
입장해 버리면 스냅샷에 `waiting > 0` 이 한 번도 안 나타나고, 그러면 **래치가 영영
안 풀려 그 노드에서 R1 이 죽는다.** 시간 만료가 있어야 한다.

오판 방향은 안전하다 — 큐가 실제로 비었는데 래치가 남으면 몇 명이 불필요하게 줄을
서고 만료되면 풀린다. 반대 방향(추월)은 생기지 않는다.

**4번과 6번에도 같이 건다.** 둘 다 `waiting` 을 보는데 그 값 역시 스냅샷이라
같은 지연을 겪는다. 4번은 8번보다 **앞**이므로, 8번에만 래치를 걸면 `dataStale`
구간에서 4번이 먼저 fail-open 통과를 시켜 래치가 무력화된다. **`waiting` 을 보는
모든 줄이 래치를 함께 봐야 한다.**

**1번이 맨 앞**: 매진 오판은 **안전한 방향으로만** 일어나고 다음 스냅샷이 되돌린다 (B-11).
위험한 방향(매진인데 통과)은 재고가 줄기만 하는 구간에서 안 생긴다.
`dataStale` 뒤에 두면 스케줄러 장애 시 매진 쿠폰이 fail-open 상한을 갉아먹는다.

**5번이 6~10번보다 앞**: 큐 상한은 `waiting > 0`일 때만 의미가 있는데, 그러면
`runtime != IDLE`이라 8번에서 먼저 반환된다. 뒤에 두면 **영원히 도달하지 않는
죽은 분기**가 된다.

### 3.6 불변식

| ID | 불변식 | 강제 |
|---|---|---|
| I1 | `runtime == IDLE ⟹ credit == 0` | 컴팩트 생성자 |
| I1' | `runtime == IDLE ⟹ waiting == 0` | 컴팩트 생성자 |
| I2 | `runtime == CLOSED ⟹ remainingStock == 0` | 컴팩트 생성자 |
| I3 | `runtime == DRAINING ⟹ credit >= waiting` | 컴팩트 생성자 |
| I3' | `runtime == QUEUEING ⟹ credit < waiting` (**I4 뒤에 검사**) | 컴팩트 생성자 |
| I4 | `waiting == 0 ⟹ runtime ∈ {IDLE, CLOSED}` | 컴팩트 생성자 |
| I5 | 표시 순위는 단조 비증가 | 속성 테스트 |
| I6 | `pollScale >= 1.0` | 생성자 정규화 |

**I1이 가장 중요하다.** `IDLE`과 `credit==0`은 독립 값이 아니라 같은 원인
(`waiting == 0`)에서 나온다.

**I3 와 I3' 은 짝이다.** 한쪽만 두면 같은 `(credit, waiting)` 이 두 런타임을 다
가질 수 있고, 두 발행자가 같은 사실을 다른 이름으로 적는다. 경계는 양쪽이 같은
자리여야 한다 — `credit == waiting` 은 다 뺄 수 있으므로 `DRAINING` 한쪽에만
속한다. 그래서 `waiting > 0` 구간에서 두 상태가 credit 축을 정확히 이분한다.

**단, `stock > 0` 일 때만이다.** I2 는 `CLOSED ⟹ stock == 0` 만 걸고 역방향이
없어서, `stock == 0` 이면 `CLOSED` 와 `DRAINING`(또는 `QUEUEING`)이 같은 값
조합을 공유한다. 유일성은 거기서 깨진다. 사다리 1번이 `stock <= 0` 을 먼저
걷어내 판정에는 해가 없고, 불변식을 더 넣으면 코덱이 떨구는 축만 는다 — 그래서
지금은 한계를 적어 두는 쪽을 골랐다 (CY-323).

**판정은 이걸로 달라지지 않는다.** 사다리는 `runtime` 을 `!= IDLE` 로만 보므로
둘이 같은 칸이다. 얻는 것은 **표현의 유일성**이다.

### 3.7 픽스처 설계 — 이 페이즈에서 가장 중요한 규칙

**자유형 생성자를 테스트에 노출하지 않는다.**

```java
// 금지 — 불변식을 어긴 조합을 만들 수 있다
new CouponState(mode, runtime, credit, stock, waiting, scale)

// 허용 — 각 팩토리가 도달 가능한 상황 하나씩만 만든다
CouponStates.idle(stock)
CouponStates.queueing(credit, stock, waiting)
CouponStates.draining(credit, stock, waiting)
CouponStates.closed(waiting)
CouponStates.off(stock)
CouponStates.unknown()
```

**픽스처가 불변식을 어길 수 있으면 테스트는 버그를 증명하지 못한다.**

---

## 4. Tasks

형식: [README 5절](README.md). 각 태스크는 TDD 사이클 1~3회 = 커밋 2~6개.
`★` 는 Goal 에 직결되는 태스크.

### 4.1 상태 모델

#### T2.1.1 · 열거 타입

- **산출물** `domain/coupon/QueueMode.java`, `RuntimeState.java`
- **근거** 3.5절 · 3.6절

1. **RED** `런타임_상태는_네_가지다` — `IDLE`, `QUEUEING`, `DRAINING`, `CLOSED`
2. **GREEN** enum 2종. 각 값에 **언제 이 상태가 되는지** Javadoc 한 줄
3. **완료** `CLOSED` 가 존재하고 "재고 소진 + 대기자 있음"으로 문서화됨

#### T2.1.2 · `CouponState` 와 불변식 ★

- **산출물** `domain/coupon/CouponState.java`
- **근거** 3.6절 (I1~I4, I6) · 3.7절 · [AIJ-0001](../ai/journal/2026/08/AIJ-0001-analyze-legacy-admission-logic.md)
- **선행** T2.1.1

1. **RED** `IDLE_상태에서_credit이_0이_아니면_생성에_실패한다` (I1)
2. **GREEN** 컴팩트 생성자에서 I1 검증, `IllegalArgumentException`
3. **RED** I2·I3·I4 각각에 대해 위반 케이스 (3 사이클)
4. **GREEN** 각 검증 추가
5. **RED** `pollScale이_1미만이면_1로_정규화된다` (I6)
6. **GREEN** 정규화
7. **완료** I1~I4·I6 위반 조합을 **생성자로 만들 수 없다**

#### T2.1.3 · 정적 팩토리 ★

- **산출물** `CouponState.java` (팩토리 8종 — `always`·`offWithQueue` 포함)
- **근거** 3.7절 · JS-12 · DS-2
- **선행** T2.1.2

1. **RED** `idle_팩토리는_credit_0과_waiting_0을_만든다`
2. **GREEN** `idle(stock)`
3. **RED** 나머지 5종 각각 (`queueing`/`draining`/`closed`/`off`/`unknown`)
4. **GREEN** 팩토리 추가. 각각 **이 상태가 실제로 어떻게 생기는지** Javadoc 한 줄
5. **완료** 8개 팩토리가 전부 도달 가능한 상황만 만든다

> **`offWithQueue` 는 상태가 둘이다.** 런타임을 못 박지 않고 `(credit, waiting)`
> 에서 유도하기 때문이다 — 못 박으면 다 뺄 수 있는 줄까지 `QUEUEING` 이 되어
> I3' 에 막힌다. "하나의 상태" 가 아니라 **"하나의 상황"** 이 계약이다.
> 유도하는 팩토리는 그 경계가 생성자와 같은 자리인지를 **팩토리에 대고** 재야
> 한다. 생성자 단언은 런타임을 인자로 받아 동어반복이다.

#### T2.1.4 · 테스트 픽스처 `CouponStates` ★

- **산출물** `testFixtures/.../coupon/CouponStates.java` (TS-3 · T1.2.4)
- **근거** 3.7절 · TS-3 — **이 태스크가 R1 버그의 재발을 막는 장치다**
- **선행** T2.1.3

1. **GREEN** 팩토리를 감싸는 픽스처. **자유형 생성 메서드를 두지 않는다**
2. **완료** 픽스처로 `(IDLE, credit>0)` 을 만들 수 없다 — 컴파일 불가 또는 예외

#### T2.1.5 · `SnapshotMeta`

- **산출물** `domain/coupon/SnapshotMeta.java`
- **근거** 3.1절 (globalCredit 전달 경로)

1. **RED** `게이트웨이_수가_0이면_1로_취급한다`
2. **GREEN** `effectiveGatewayCount()`
3. **완료** 0·음수에서 나눗셈이 터지지 않는다

#### T2.1.6 · 통과 상한 계산 ★

- **산출물** `CouponState.contendedCap(int)`, `idleCap(int, int)`
- **근거** 3.1절 · B-1 · B-2 — **R1의 핵심**
- **선행** T2.1.2, T2.1.5

1. **RED** `경합_쿠폰의_몫은_credit을_노드수로_나눈_값이다`
2. **GREEN** `contendedCap(gatewayCount)` = `credit / max(1, gatewayCount)`
3. **RED** `한산한_쿠폰은_전역_여유를_상한으로_쓴다` — credit 0인데 상한 > 0
4. **GREEN** `idleCap(globalCredit, gatewayCount)`
5. **RED** `credit이_노드수보다_작으면_총합이_credit을_넘지_않는다`
   — credit=10, 노드 20 → 전 노드 합 ≤ 10
6. **GREEN** `max(1,...)` 대신 인스턴스 ID 해시로 일부 노드에만 1 배정
7. **완료** 어떤 (credit, N) 조합에서도 노드 합이 credit 을 넘지 않는다

#### T2.1.7 · 큐 파생값

- **산출물** `queueDepthSec()`, `queueCapacity()`
- **근거** 3.5절 5번 · Phase 7 3.3절
- **주의** 등록 경로의 상한은 여기서 끝나지 않는다. 이 값이 0 일 때의 폴백은
  `AdmissionDecider.queueCapacity` 가 정한다 (3.5절 5번 각주)

1. **RED** `credit이_0이면_큐_깊이는_무한이다` — 나눗셈 예외 없음
2. **GREEN** 방어 분기
3. **RED** `큐_용량은_허용_최대_ETA와_credit의_곱이다`
4. **GREEN** `queueCapacity(maxEtaSec)`
5. **완료** credit 0 에서 두 메서드가 예외를 던지지 않는다

### 4.2 리미터

#### T2.2.1 · 초 단위 고정 윈도우

- **산출물** `domain/admission/SecondWindowLimiter.java`
- **근거** 3.1절 · TS-4 (시계 주입)

1. **RED** `상한_안이면_허용하고_넘으면_거부한다`
2. **GREEN** `tryAcquire(key, permits, epochSecond)`
3. **RED** `초가_넘어가면_카운터가_리셋된다` — `Clock` 조작
4. **GREEN** 윈도우 교체
5. **RED** `상한이_0이하면_아무것도_통과시키지_않는다`
6. **GREEN** 가드
7. **완료** 상한 0·음수·경계에서 정확

#### T2.2.2 · 경로 전환 시 카운터 이월 (F4) ★

- **산출물** `SecondWindowLimiter.java`
- **근거** 3.3절 · F4
- **선행** T2.2.1

1. **RED** `같은_초에_경로가_바뀌어도_합산_상한을_넘지_않는다`
   — 상한 100으로 60건 소비 후 같은 키·같은 초에 상한 50으로 전환 → 추가 허용 0
2. **GREEN** 리미터를 경로별로 나누지 않고 **상한만 인자로 받는다**
3. **완료** 경로 전환 순간의 버스트가 1.0× (1.5× 아님)

#### T2.2.5 · 원자 획득 (`tryAcquireAll`) ★

- **산출물** `SecondWindowLimiter.java`, `AdmissionDecider.java`
- **근거** 3.1절 원자 획득 · G2.12
- **선행** T2.2.1

1. **RED** `한쪽이_부족하면_다른_쪽도_차감하지_않는다`
   — tier2 여유 있고 tier1 부족 → 거부되고 **tier2 카운터가 그대로**
2. **GREEN** 두 상한을 함께 보고 전부-아니면-전무로 차감
3. **RED** `부족한_쪽에_따라_판정값이_갈린다` — COUPON / GLOBAL
4. **완료** 거부된 요청이 어느 예산도 소비하지 않는다

> 반납 방식으로 만들지 않는다. 반납 누락이 곧 조용한 예산 유실이다.

#### T2.2.3 · 맵 크기 상한

- **산출물** `SecondWindowLimiter.java`
- **근거** RD-7 (클라이언트 입력이 키가 되는 경로)

1. **RED** `윈도우_맵은_상한을_넘지_않는다` — 10만 키 주입
2. **GREEN** 지난 초의 윈도우 제거 + 절대 상한
3. **완료** 키를 무한히 넣어도 메모리가 유계

#### T2.2.4 · 동시성

- **산출물** `SecondWindowLimiter.java`
- **근거** RX-11

1. **RED** `병렬_호출에서도_상한을_넘지_않는다` — 스레드 16 × 1000회
2. **GREEN** `ConcurrentHashMap` + `AtomicInteger`
3. **완료** 초과 통과 0

### 4.3 판정

#### T2.3.1 · `AdmissionDecision`

- **산출물** `domain/admission/AdmissionDecision.java`
- **근거** 3.5절 (11개 분기 전부)

1. **RED** `모든_판정값은_통과_큐_거절_중_하나로_분류된다`
2. **GREEN** enum + `isPass()` / `isEnqueue()` / `isReject()`
3. **완료** 값 추가 시 분류 누락이 테스트로 잡힌다 (PK-A3)

#### T2.3.2 · 매진 우선 판정

- **산출물** `domain/admission/AdmissionDecider.java`
- **근거** 3.5절 1번 · B-3
- **선행** T2.1.4, T2.3.1

1. **RED** `재고가_없으면_스냅샷이_낡아도_매진으로_종결한다`
   — `CouponStates.closed(100)`, `dataStale=true` → `REJECT_SOLD_OUT`
2. **GREEN** 1번 분기를 최상단에
3. **완료** 매진이 fail-open 상한을 소비하지 않는다

#### T2.3.3 · 토큰 통과와 상한 (F8)

- **산출물** `AdmissionDecider.java`
- **근거** 3.4절 · F8
- **선행** T2.2.2

1. **RED** `유효한_토큰은_상태와_무관하게_통과한다`
2. **GREEN** 2번 분기
3. **RED** `토큰_통과도_전역_상한을_넘으면_재시도를_받는다` → `RETRY_TOKEN`
4. **GREEN** tier2 적용. **큐가 아니라 429**
5. **완료** 축적된 토큰이 상한을 우회하지 못한다

#### T2.3.4 · fail-open 분기 (F1) ★

- **산출물** `AdmissionDecider.java`
- **근거** 3.2절 · B-4 · [AIJ-0002](../ai/journal/2026/08/AIJ-0002-recovery-transition-findings.md)
- **선행** T2.3.2

1. **RED** `스냅샷이_낡고_큐가_비어_있으면_상한_안에서_통과시킨다`
   — `dataStale=true`, `waiting=0` → `PASS_FAIL_OPEN`
2. **GREEN** 4번 분기
3. **RED** `스냅샷이_낡아도_줄_선_사람이_있으면_추월시키지_않는다` ★
   — `dataStale=true`, `waiting=5000` → `ENQUEUE_STALE`
4. **GREEN** 6번 분기. **4번보다 뒤, 5번보다 뒤**
5. **RED** `fail_open_상한을_넘으면_거절한다` → `REJECT_OVERLOAD`
6. **GREEN** 상한 적용 (노드 수로 나눈다 — Phase 4 F5와 같은 논리)
7. **RED** `스냅샷이_아직_큐가_비었다고_해도_방금_보냈으면_통과시키지_않는다` ★
   — `dataStale=true`, `waiting=0`, `justEnqueued=true` → `ENQUEUE_STALE`
8. **GREEN** 4·6번에 래치 반영 ([래치](#latch))
9. **완료** `waiting > 0` 인 어떤 조합에서도 fail-open 통과가 0
10. **완료** **`waiting == 0` 이어도 래치가 서 있으면 fail-open 통과가 0**

#### T2.3.5 · 큐 상한

- **산출물** `AdmissionDecider.java`
- **근거** 3.5절 5번 (죽은 분기 방지)
- **선행** T2.1.7

1. **RED** `큐가_상한에_닿으면_거절한다` → `REJECT_QUEUE_FULL`
2. **GREEN** 5번 분기를 **6~10번보다 앞에**
3. **완료** `waiting >= capacity` 인 입력이 `ENQUEUE_BACKLOG` 로 가려지지 않는다

#### T2.3.6 · 새치기 방지

- **산출물** `AdmissionDecider.java`
- **근거** 3.5절 7·8번

1. **RED** `ALWAYS_모드는_유입과_무관하게_줄을_세운다`
2. **GREEN** 7번 분기
3. **RED** `이미_기다리는_사람이_있으면_새치기를_막는다` → `ENQUEUE_BACKLOG`
4. **GREEN** 8번 분기
5. **RED** `스냅샷이_아직_IDLE_이어도_방금_큐로_보냈으면_막는다` — `justEnqueued`
6. **GREEN** 8번 분기에 래치 반영 (주입받는 값)
7. **RED** `래치가_풀리면_다시_무대기_통과한다` — 같은 상태에 `justEnqueued=false`
8. **GREEN** 분기 복귀 (만료 **계산은 도메인 밖**, 여기서는 주입값만 본다)
9. **완료** `runtime != IDLE` 인 상태에서 통과 판정이 나오지 않는다
10. **완료** **큐 등록 직후 같은 쿠폰에 통과 판정이 나오지 않는다** — 4·6·8번 전부
11. **완료** 래치가 만료되어 **R1 이 되살아난다**

#### T2.3.7 · R1 — 한산한 쿠폰 통과 ★★

- **산출물** `AdmissionDecider.java`
- **근거** 3.1절 · B-1 · [AIJ-0001](../ai/journal/2026/08/AIJ-0001-analyze-legacy-admission-logic.md)
  — **이 프로젝트의 존재 이유**
- **선행** T2.1.6, T2.2.2, T2.3.6

1. **RED** `대기자가_없는_쿠폰은_큐를_거치지_않고_통과한다` ★
   — `CouponStates.idle(500)` (credit **0**), `meta(globalCredit=1000, gateways=1)`
   → `PASS_UNDER_CAP`
2. **GREEN** 9~11번 분기. IDLE 이면 `idleCap(globalCredit, N)` 을 상한으로
3. **RED** `전역_상한을_넘는_순간_큐가_생긴다` — 1001번째 요청 → `ENQUEUE_RATE_GLOBAL`
4. **GREEN** tier2 적용
5. **완료** credit 0 인 IDLE 쿠폰에 **큐 판정이 나오지 않는다**
   — 이전 구현에서 100% 실패하던 케이스

#### T2.3.8 · 쿠폰 독식 방지

- **산출물** `AdmissionDecider.java`
- **근거** 3.1절 (tier 1)
- **선행** T2.3.7

1. **RED** `한_쿠폰이_전역_여유를_독식하면_다음_쿠폰은_큐로_간다`
   → `ENQUEUE_RATE_COUPON`
2. **GREEN** 10번 분기
3. **RED** `다른_쿠폰의_상한은_따로_센다`
4. **GREEN** 쿠폰별 키
5. **완료** 쿠폰 간 격리. 다음 틱에 공정 몫으로 자기 교정

#### T2.3.9 · 판정 순서 전수 검증 ★

- **산출물** `test/.../admission/DecisionOrderTest.java`
- **근거** 3.5절 — **죽은 분기를 찾는 유일한 장치**
- **선행** T2.3.1~T2.3.8

1. **RED** `모든_판정값은_적어도_하나의_입력에서_도달한다`
   — 11개 분기 각각에 대해 도달하는 입력 조합을 만든다
2. **GREEN** 도달 불가 분기가 있으면 **순서를 고친다** (계획서 3.5절도 함께)
3. **완료** `AdmissionDecision` 의 모든 값이 도달 가능하다

> 이 테스트가 3.5절의 5번 문제(큐 상한이 죽은 분기였던 것)를 실증한다.
> 순서를 바꿀 때마다 이 테스트가 먼저 깨져야 한다.

### 4.4 배분

#### T2.4.1 · `CouponDemand` 와 `Grant`

- **산출물** `domain/allocation/CouponDemand.java`, `Grant.java`
- **근거** C-2 (재고는 크레딧을 깎는다)

1. **RED** `재고가_천장으로_작동한다` — waiting 100, stock 3 → `want()` 3
2. **GREEN** `want()` = `clamp(waiting, 0, stock)`
3. **RED** `대기자가_없으면_배분_대상이_아니다` → `isActive()` false
4. **GREEN** `isActive()`
5. **완료** `IDLE ⟹ credit==0` 불변식(I1)의 출처가 여기임이 테스트로 드러난다

#### T2.4.2 · 2패스 공정 배분

- **산출물** `domain/allocation/FairShareAllocator.java`
- **근거** C-1 · C-3
- **선행** T2.4.1

1. **RED** `균등_배분_후_남은_몫을_재배분한다`
   — globalCredit 1000, 핫 20만/콜드 40/콜드 3 → 957/40/3
2. **GREEN** 굶주린 쿠폰에 균등 배분, 못 쓴 몫을 다시 굶주린 쪽으로.
   **2패스는 하한이지 상한이 아니다** — 쿠폰이 많고 요구량이 들쭉날쭉하면
   두 번으로 못 채우고, 남긴 만큼 대기자가 이유 없이 기다린다
3. **RED** `핫에_20만이_밀려도_콜드는_첫_틱에_전부_빠진다`
4. **GREEN** (1패스가 이미 보장)
5. **RED** `정수_나눗셈_나머지는_다음_틱으로_넘긴다`
6. **GREEN** 굶주린 수보다 적게 남으면 멎는다 — 균등하게 나눌 방법이 없다.
   나머지를 누구에게 주면 그 쿠폰만 이득이고, 노드마다 다른 쪽을 고르면
   총합이 전역 크레딧을 넘는다
7. **완료** 기아 불가 + 유휴 낭비 0 이 동시에 성립

#### T2.4.3 · 크레딧 초과 배분 속성 테스트 ★

- **산출물** `test/.../allocation/AllocationPropertyTest.java`
- **근거** Goal 통과 수치 · TS-6
- **선행** T2.4.2

1. **RED** `무작위_10만회에서_배분_총합이_전역_크레딧을_넘지_않는다`
2. **GREEN** (T2.4.2가 통과시켜야 함. 실패하면 배분 로직 수정)
3. **완료** 위반 0건

#### T2.4.4 · `ShardSplitter` 인터페이스

- **산출물** `domain/allocation/ShardSplitter.java` + 단일 샤드 구현
- **근거** DS-7 (두 번째 사례가 Phase 10에 예정)

1. **RED** `샤드가_하나면_전량을_그_샤드에_배정한다`
2. **GREEN** 인터페이스 + `SingleShardSplitter`
3. **완료** Phase 10에서 비례 분할 구현을 **끼워 넣을 수 있다**

### 4.5 폴링

#### T2.5.1 · 폴링 간격 정책

- **산출물** `domain/queue/PollIntervalPolicy.java`
- **근거** D-2 · TS-4

1. **RED** `ETA_밴드마다_기본_간격이_다르다` — 5/30/120초 경계
2. **GREEN** 밴드 4종
3. **RED** `지터가_적용된다` — 주입된 난수원으로 결정적 검증
4. **GREEN** 난수원 주입 (DS-1 — 직접 호출 금지)
5. **RED** `min과_max로_클램프된다`
6. **GREEN** 클램프
7. **완료** 같은 밴드가 동기화되지 않는다

#### T2.5.2 · 생존 TTL

- **산출물** `PollIntervalPolicy.aliveTtl(...)`
- **근거** 백그라운드 탭 분당 1회 스로틀

1. **RED** `생존_TTL은_하한_아래로_내려가지_않는다`
2. **GREEN** `max(minAliveTtl, interval × factor)`
3. **완료** 30초 간격에서도 TTL 하한이 지켜진다

#### T2.5.3 · 폴링 예산 계획

- **산출물** `domain/queue/PollBudgetPlanner.java`
- **근거** D-2 (큐의 시간 깊이가 부하를 정한다)

1. **RED** `큐를_훑지_않고_닫힌_식으로_예상_폴링을_구한다`
2. **GREEN** 밴드별 인원 × 빈도 합
3. **RED** `배수율이_0이면_전원이_가장_먼_밴드다`
4. **GREEN** 방어 분기
5. **RED** `예산이_남아도_배수는_1_미만으로_내려가지_않는다`
6. **GREEN** `max(1.0, ...)`
7. **완료** 한산할 때 오히려 부하를 만들지 않는다

#### T2.5.4 · 비활성 쿠폰 제외 ★

- **산출물** `PollBudgetPlanner.java`
- **근거** Phase 7 3.3절 (매진 큐의 교차 오염)
- **선행** T2.5.3

1. **RED** `매진_쿠폰의_대기자는_전역_폴링_예산에_들어가지_않는다`
   — 매진 쿠폰 10만 명 + 정상 쿠폰 1000명 → scale 이 정상 쿠폰만 반영
2. **GREEN** `isActive()` 필터
3. **완료** 죽은 큐가 **다른 쿠폰의** 폴링 간격을 늘리지 않는다

### 4.6 평활화

#### T2.6.1 · credit EWMA

- **산출물** `domain/allocation/CreditSmoother.java`
- **근거** Phase 4 F9 · Phase 10 5.2절 (ETA 오차의 지배항)

1. **RED** `EWMA는_설정된_시상수로_수렴한다` — α=0.2, 5틱 (시험 계수다. 운영값은 0.3)
2. **GREEN** `α×credit + (1-α)×prev`
3. **RED** `첫_관측치가_초기값이_된다`
4. **GREEN** 초기화
5. **완료** GC 스파이크가 표시 ETA 를 두 배로 만들지 않는다

#### T2.6.2 · 히스테리시스

- **산출물** `CreditSmoother.java` 또는 `StateTransitionPolicy.java`
- **근거** 3.1절 (R1 수정이 만드는 진동)
- **선행** T2.6.1

1. **RED** `진입과_해제_임계가_비대칭이다` — 진입 100%, 해제 70%
2. **GREEN** 비대칭 임계
3. **RED** `해제_후_최소_유지_시간_동안_재진입하지_않는다`
4. **GREEN** 최소 유지 시간
5. **RED** `임계선_근처_유입에서_전이가_N회_이하다`
6. **GREEN** (위 둘이 통과시켜야 함)
7. **완료** 사용자에게 "대기 없음 → 500명 → 대기 없음" 이 보이지 않는다

#### T2.6.3 · 상태 직렬화 (F9)

- **산출물** `CreditSmoother.java`
- **근거** Phase 4 F9 (리더 교체 시 유실)

1. **RED** `평활화_상태를_내보내고_되살릴_수_있다`
2. **GREEN** 상태 스냅샷 record + 복원 팩토리
3. **완료** Phase 4 가 스냅샷 메타에 실어 이월할 수 있다

### 4.7 순위와 ETA

#### T2.7.1 · 순위 추정

- **산출물** `domain/queue/RankEstimator.java`
- **근거** Phase 10 5절 (샤딩 대비)

1. **RED** `전역_순위는_로컬_순위에_샤드_수를_곱한_값이다`
2. **GREEN** `localRank × shards`
3. **RED** `샤드가_하나면_로컬_순위가_곧_전역_순위다`
4. **GREEN** (자명하게 성립)
5. **완료** Phase 10 에서 샤드 수만 바꾸면 된다

> `localRank` 는 도메인이 계산하지 않는다. **주입받는 값**이다 —
> score 기반 전환(A-9) 이후 로컬 순위는 `ZCOUNT` 결과이고, 그건 어댑터의 몫이다.
> 도메인은 순수하게 유지한다 (T2.8.1). 이 경계 덕분에 순위의 출처가
> 뺄셈이든 `ZCOUNT` 든 도메인은 안 바뀐다.

#### T2.7.2 · 단조성 속성 테스트 ★

- **산출물** `test/.../queue/RankMonotonicityTest.java`
- **근거** Goal 통과 수치 · I5 · TS-6
- **선행** T2.7.1

1. **RED** `무작위_10만_시퀀스에서_표시_순위가_증가하지_않는다`
   — 단조 감소하는 `localRank` 를 주입하며 표시 순위 검증
2. **GREEN** (T2.7.1이 통과시켜야 함)
3. **완료** 위반 0건

> **이 테스트가 검증하는 것은 "입력이 단조면 출력도 단조"까지다.**
> score 전환(A-9) 이후 `localRank` 는 어댑터의 `ZCOUNT` 결과라
> **입력이 단조라는 보장은 Phase 3 의 G3.11 이 진다.**

#### T2.7.3 · ETA 계산과 표시

- **산출물** `RankEstimator.java` 또는 `EtaPolicy.java`
- **근거** Phase 10 5.2절 · 4.3 (버킷 표시)
- **선행** T2.6.1, T2.7.1

1. **RED** `ETA는_EWMA_credit으로_나눈다` — 순간 credit 아님
2. **GREEN** EWMA 사용
3. **RED** `배수율이_0_이하면_모름이다`
4. **GREEN** 모름은 `NaN` — 표시는 **가장 넓은 구간**으로 접는다 (CY-282)
5. **RED** `표시_ETA는_거친_버킷이다` — "약 1분" / "약 5분" / "10분 이상"
6. **GREEN** 버킷화
7. **완료** ±1.5초 오차가 사용자 눈에 보이지 않는다

### 4.8 아키텍처 검증

#### T2.8.1 · 도메인 순수성 (ArchUnit) ★

- **산출물** `test/.../ArchitectureTest.java`
- **근거** DS-1 · Goal 통과 수치 — **순수성은 한 번 깨지면 조용히 번진다**

1. **RED** `도메인은_Spring_Reactor_Redis를_참조하지_않는다`
2. **GREEN** (위반이 있으면 코드 수정)
3. **RED** `도메인은_시계와_난수를_직접_읽지_않는다`
4. **GREEN** 주입으로 전환
5. **RED** `도메인은_바깥_계층을_참조하지_않는다` — 어댑터·설정·웹으로 나가는 의존 0
6. **RED** `모든_클래스가_com_kafkick_waiting_아래에_있다`
7. **완료** `import` 하나만 들어와도 CI 가 막는다

> `cy-be` 는 이 경계를 **Gradle 모듈**로 강제한다 (`core` 가 아무것도 의존하지 않음).
> 게이트웨이는 배포 단위가 하나라 모듈로 쪼개지 않으므로(O-2) **같은 보장을
> ArchUnit 이 대신 해야 한다.** 5번이 없으면 도메인이 어댑터를 부르기 시작하는데,
> 컴파일은 되므로 아무도 모른다.

---

## 5. Exit Gate

Goal 의 수치를 항목별로 판정한다. 하나라도 미달이면 Phase 4·5를 시작하지 않는다.

| ID | 기준 | 검증 |
|---|---|---|
| **G2.1** | **R1 — credit 0 인 IDLE 쿠폰이 큐 없이 통과** | T2.3.7 |
| **G2.2** | **F1 — `dataStale` + `waiting>0` 에서 추월 0** | T2.3.4 |
| G2.3 | F4 — 경로 전환 시 같은 초 합산 상한 초과 0 | T2.2.2 |
| G2.4 | F8 — 토큰 통과가 tier2 상한을 넘지 않음 | T2.3.3 |
| **G2.16** | **큐 등록 직후 스냅샷 갱신 전에도 추월이 생기지 않는다** ([래치](#latch)) | T2.3.4 · T2.3.6 |
| **G2.17** | **`justEnqueued=false` 면 같은 상태에서 무대기 통과가 복귀한다** — 래치가 죽은 분기를 만들지 않는다 | T2.3.6 |
| G2.5 | 불변식 I1~I6 위반을 픽스처로 만들 수 **없다** | T2.1.4 |
| G2.6 | 순수 도메인 **브랜치 커버리지 100%** | JaCoCo |
| G2.7 | **뮤테이션 생존율 ≤ 10%** | PIT |
| G2.8 | **크레딧 초과 배분 0** — 무작위 10만 회 | T2.4.3 |
| G2.9 | **순위 단조성 위반 0** — 무작위 10만 시퀀스 | T2.7.2 |
| G2.10 | 도메인이 Spring·Redis·시계를 참조하지 **않는다** | T2.8.1 |
| G2.15 | **도메인 → 바깥 계층 의존 0** · 전 클래스가 `com.kafkick.waiting` 아래 | T2.8.1 |
| G2.11 | `AdmissionDecision` 의 모든 값이 도달 가능 | T2.3.9 |
| **G2.12** | **거부된 요청이 어느 리미터도 소비하지 않는다** — 원자 획득 | T2.2.5 |
| G2.13 | `idleCreditRatio` 가 tier 1 에만 적용된다 — tier 2 상한 불변 | T2.2.5 |
| G2.14 | **토큰 보유자가 쿠폰별 상한으로 거절되지 않는다** — 배분받은 몫을 다 쓸 수 있다 | T2.3.3 |

### 게이트 커밋

```
chore(gate): Phase 2 게이트 통과

분기 커버리지 100% / 뮤테이션 생존 4.2%
R1(T2.3.7) · F1(T2.3.4) · F4 · F8 시나리오 통과
속성 테스트: 배분 10만회 크레딧 초과 배분 0, 순위 10만 시퀀스 역행 0
ArchUnit: 도메인 금지 import 0

Refs: CY-48
```

---

## 6. 하지 않는 것

| 항목 | 이유 |
|---|---|
| Redis 접근 | Phase 3 |
| Spring 애노테이션 | 이 계층은 POJO. 배선은 Phase 4·5 |
| 실제 시계·난수 | 주입. 직접 호출 금지 (G2.10) |
| 샤드 비례 분할 구현 | 인터페이스만 (T2.4.4). 구현은 Phase 10 |
| 서킷 상태 연동 | Phase 8 (F3) |

---

## 지라 티켓

티켓에는 라벨을 붙이지 않는다 — 보드에서 방해만 된다. 계획서 ID 와의 대응은 [91-jira-map.md](91-jira-map.md) 9절에 있다.

**에픽** [`CY-19`](https://shseol.atlassian.net/browse/CY-19) 입장 판정 도메인

| 계획서 | 티켓 | 이름 |
|---|---|---|
| `2.1` | [`CY-40`](https://shseol.atlassian.net/browse/CY-40) | **쿠폰 상태와 통과 상한 계산** |
| `2.1.1` | [`CY-128`](https://shseol.atlassian.net/browse/CY-128) | 쿠폰의 대기열 상태와 런타임 상태를 열거형으로 |
| `2.1.2` | [`CY-129`](https://shseol.atlassian.net/browse/CY-129) | 재고·여유·대기 인원을 담는 상태 타입. 생성자에서 모순 조합을 거부한다 |
| `2.1.3` | [`CY-130`](https://shseol.atlassian.net/browse/CY-130) | 상태별 팩토리 메서드 |
| `2.1.4` | [`CY-131`](https://shseol.atlassian.net/browse/CY-131) | 테스트 픽스처도 그 팩토리만 쓰게 한다. 자유형 생성을 두지 않는다 |
| `2.1.5` | [`CY-132`](https://shseol.atlassian.net/browse/CY-132) | 스냅샷 신선도 메타데이터 |
| `2.1.6` | [`CY-133`](https://shseol.atlassian.net/browse/CY-133) | 한산한 쿠폰의 통과 상한 계산 |
| `2.1.7` | [`CY-134`](https://shseol.atlassian.net/browse/CY-134) | 큐 깊이와 용량 파생값. 여유가 0일 때 0으로 나누지 않게 방어 |
| `2.2` | [`CY-41`](https://shseol.atlassian.net/browse/CY-41) | **초당 통과 상한 리미터** |
| `2.2.1` | [`CY-135`](https://shseol.atlassian.net/browse/CY-135) | 초 단위 고정 윈도우 카운터 |
| `2.2.2` | [`CY-136`](https://shseol.atlassian.net/browse/CY-136) | 정상 판정과 장애 대응 판정이 바뀔 때 카운터를 이월한다 |
| `2.2.3` | [`CY-137`](https://shseol.atlassian.net/browse/CY-137) | 쿠폰이 많아져도 메모리가 무한히 늘지 않게 상한 |
| `2.2.4` | [`CY-138`](https://shseol.atlassian.net/browse/CY-138) | 여러 요청이 동시에 들어와도 카운터가 정확하게 |
| `2.2.5` | **미발번** | 두 리미터를 전부-아니면-전무로 획득 (B-12) |
| `2.3` | [`CY-42`](https://shseol.atlassian.net/browse/CY-42) | **입장 판정 로직** |
| `2.3.1` | [`CY-139`](https://shseol.atlassian.net/browse/CY-139) | 판정 결과 타입 |
| `2.3.2` | [`CY-140`](https://shseol.atlassian.net/browse/CY-140) | 매진을 가장 먼저 본다. 재고가 없으면 나머지를 볼 필요가 없다 |
| `2.3.3` | [`CY-141`](https://shseol.atlassian.net/browse/CY-141) | 입장 토큰을 든 요청도 상한을 거친다. 허가 시점과 사용 시점이 벌어질 수 있다 |
| `2.3.4` | [`CY-142`](https://shseol.atlassian.net/browse/CY-142) | 상태를 모를 때의 처리 |
| `2.3.5` | [`CY-143`](https://shseol.atlassian.net/browse/CY-143) | 큐가 꽉 찼을 때 거절 |
| `2.3.6` | [`CY-144`](https://shseol.atlassian.net/browse/CY-144) | 이미 줄 선 사람을 추월시키지 않는 검사 |
| `2.3.7` | [`CY-145`](https://shseol.atlassian.net/browse/CY-145) | 한산한 쿠폰은 큐 없이 통과 |
| `2.3.8` | [`CY-146`](https://shseol.atlassian.net/browse/CY-146) | 한 쿠폰이 전체 여유를 독식하지 못하게 |
| `2.3.9` | [`CY-147`](https://shseol.atlassian.net/browse/CY-147) | 위 순서가 전부 도달 가능한지 전수 검증 |
| `2.4` | [`CY-43`](https://shseol.atlassian.net/browse/CY-43) | **쿠폰별 크레딧 공정 배분** |
| `2.4.1` | [`CY-148`](https://shseol.atlassian.net/browse/CY-148) | 쿠폰별 수요와 배분 결과를 담는 타입 |
| `2.4.2` | [`CY-149`](https://shseol.atlassian.net/browse/CY-149) | 균등 배분 후 남는 몫을 다시 나누는 2단계 배분 |
| `2.4.3` | [`CY-150`](https://shseol.atlassian.net/browse/CY-150) | 무작위 10만 회에서 배분 총합이 총 여유를 넘지 않는지 검증 |
| `2.4.4` | [`CY-151`](https://shseol.atlassian.net/browse/CY-151) | 나중에 큐를 쪼갤 때 쓸 분할 인터페이스만 미리 정의 |
| `2.5` | [`CY-44`](https://shseol.atlassian.net/browse/CY-44) | **폴링 간격과 생존 판단** |
| `2.5.1` | [`CY-152`](https://shseol.atlassian.net/browse/CY-152) | 대기 깊이에 따라 폴링 간격을 늘리고 줄이는 정책 |
| `2.5.2` | [`CY-153`](https://shseol.atlassian.net/browse/CY-153) | 폴링이 곧 생존 신호다 |
| `2.5.3` | [`CY-154`](https://shseol.atlassian.net/browse/CY-154) | 전체 폴링 부하가 예산을 넘지 않도록 간격을 역산 |
| `2.5.4` | [`CY-155`](https://shseol.atlassian.net/browse/CY-155) | 대기자가 없는 쿠폰은 폴링 대상에서 뺀다 |
| `2.6` | [`CY-45`](https://shseol.atlassian.net/browse/CY-45) | **가용량 급변 평활화** |
| `2.6.1` | [`CY-156`](https://shseol.atlassian.net/browse/CY-156) | 여유 값을 지수 이동평균으로 다듬는다 |
| `2.6.2` | [`CY-157`](https://shseol.atlassian.net/browse/CY-157) | 상태가 경계에서 진동하지 않도록 올릴 때와 내릴 때 기준을 다르게 |
| `2.6.3` | [`CY-158`](https://shseol.atlassian.net/browse/CY-158) | 평활화 상태를 저장해 리더가 바뀌어도 이어진다 |
| `2.7` | [`CY-46`](https://shseol.atlassian.net/browse/CY-46) | **대기 순위와 예상 시간 계산** |
| `2.7.1` | [`CY-159`](https://shseol.atlassian.net/browse/CY-159) | 큐가 쪼개져도 성립하는 순위 추정 |
| `2.7.2` | [`CY-160`](https://shseol.atlassian.net/browse/CY-160) | 무작위 10만 시퀀스에서 표시 순위가 증가하지 않는지 검증 |
| `2.7.3` | [`CY-161`](https://shseol.atlassian.net/browse/CY-161) | 예상 시간은 순간 여유가 아니라 평활화한 값으로 나눈다. 표시는 '약 1분' 같은 거친 구간으로 |
| `2.8` | [`CY-47`](https://shseol.atlassian.net/browse/CY-47) | **도메인 순수성 자동 검증** |
| `2.8.1` | [`CY-162`](https://shseol.atlassian.net/browse/CY-162) | 도메인 패키지가 프레임워크·인프라·현재 시각을 참조하면 실패하는 검사 |
| 게이트 | [`CY-48`](https://shseol.atlassian.net/browse/CY-48) | 입장 판정 도메인 종료 게이트 |

브랜치는 티켓 단위로 판다. 커밋 푸터는 지라 키 하나만 남긴다 — 계획서 ID 를 박지 않는다.

```
Refs: CY-42
```

# 작업 로그 색인

엔트리를 추가하면 **여기 한 줄을 더한다.** 최신이 위.

`confidence: low` 는 ⚠️ 로 표시한다 — 관련 사고가 나면 가장 먼저 확인할 곳이다.

---

| ID | 날짜 | 종류 | 제목 | 확신 | 승격 |
|---|---|---|---|---|---|
| [AIJ-0013](2026/08/AIJ-0013-local-review-before-pr.md) | 2026-08-19 | implement | 훅이 못 본 파일들 — PR 전 브랜치 전체 검사 | medium | — |
| [AIJ-0012](2026/08/AIJ-0012-allocation-polling-smoothing.md) | 2026-08-19 | implement | 배분·폴링·평활화 — 계획서 예시와 완료 조건의 충돌 | high | — |
| [AIJ-0011](2026/08/AIJ-0011-admission-ladder-and-mutation-gaps.md) | 2026-08-19 | implement | 판정 사다리 · 순위 추정 · 뮤테이션이 짚은 경계 | high | — |
| [AIJ-0010](2026/08/AIJ-0010-domain-state-and-limiter.md) | 2026-08-19 | implement | 순수 도메인 — 불변식·통과 상한·리미터 | high | — |
| [AIJ-0009](2026/08/AIJ-0009-build-foundation-and-quality-gates.md) | 2026-08-18 | implement | 빌드 기반과 품질 임계 · CI 첫 실행 결함 4건 | high | — |
| [AIJ-0008](2026/08/AIJ-0008-header-identity-and-front-lb.md) | 2026-08-18 | decision | 인증을 헤더 식별자로 · 락 소유권 · 앞단 LB 부재 | medium | D-A1 |
| [AIJ-0007](2026/08/AIJ-0007-queue-order-by-timestamp.md) | 2026-08-14 | decision | 큐 순서를 timestamp 로 · 차단 5건 해소 | medium | 90-decisions 2.7·2.8 |
| [AIJ-0006](2026/08/AIJ-0006-repository-layout.md) | 2026-08-14 | decide | 저장소 배치 — cy-be 와 분리, 게이트웨이는 하나로 | high | O-1~O-3 |
| [AIJ-0005](2026/08/AIJ-0005-ci-pipeline-and-jira-action.md) | 2026-08-13 | implement | CI 파이프라인 · Jira 액션 분리 | medium | — |
| [AIJ-0004](2026/08/AIJ-0004-review-governance-hooks.md) | 2026-08-13 | review | 훅·문서 재검토 — 결함 8건 | high | — |
| [AIJ-0003](2026/08/AIJ-0003-establish-ai-governance.md) | 2026-08-13 | design | AI 작업 규범 체계 수립 | medium | — |
| [AIJ-0002](2026/08/AIJ-0002-recovery-transition-findings.md) | 2026-08-13 | analyze | 회복 전이 재검토 — 설계 결함 9건 | medium | B-4 |
| [AIJ-0001](2026/08/AIJ-0001-analyze-legacy-admission-logic.md) | 2026-08-13 | analyze | 어댑티브 판정 역방향 동작 확인 | high | B-1 |

---

## 확신이 낮은 판단 (우선 확인 대상)

아직 없음. `confidence: low` 엔트리가 생기면 여기 모은다.

## 검증하지 못한 가정

| 출처 | 가정 | 확인 방법 |
|---|---|---|
| ~~AIJ-0002~~ | ~~콜드 백엔드 인스턴스가 `credits`를 과대 보고한다 (F6)~~ | ✅ 60초 선형 램프로 확정 (A-12·A-13, 2026-08-17) |
| AIJ-0002 | 리미터 이중 개방의 영향이 최대 1.5× (F4) | 부하 시험 실측 |
| AIJ-0002 | 회복 버스트 허용치 1.2배가 적절하다 (RC4) | 정상 시 변동폭 측정 후 재조정 |
| AIJ-0003 | journal 체계가 유지된다 | 2~3주 뒤 준수율 확인 |
| AIJ-0004 | 자기검증 46건이 실제 Java 코드를 충분히 덮는다 | Phase 2 에서 오탐 빈도 확인 |
| AIJ-0004 | 계획서에 남은 죽은 분기가 없다 | T2.3.9 판정 순서 전수 검증 |
| AIJ-0005 | CodeRabbit `@coderabbitai pause` 가 동작한다 | 첫 PR 에서 라벨 부착 후 확인 |
| AIJ-0005 | CodeRabbit 요약 형식이 유지된다 (`Actionable comments posted: N`) | 중계 알림의 건수가 맞는지 대조 |
| AIJ-0005 | 재사용 워크플로에서 `timeout-minutes` 표현식이 동작한다 | 첫 CI 실행 |
| AIJ-0005 | 훅과 CI 의 journal 제외 목록이 계속 일치한다 | 한쪽만 고치면 갈라진다. 정기 대조 |
| AIJ-0006 | `cy-be` 팀이 Redis 키 계약 소유(O-3)에 동의한다 | 협의 필요 |
| AIJ-0006 | 계약 픽스처를 두 저장소가 드리프트 없이 공유할 수 있다 | Phase 4 착수 전 설계 |
| AIJ-0008 | `tick` 1s · `lease` 2s 가 실제 틱 지연을 견딘다 | Phase 4 G4.3 실측 후 재조정 |
| AIJ-0008 | 앞단 LB 한 대가 1GbE 에서 목표 RPS 를 감당한다 (PPS 가 먼저 막힐 수 있다) | 10.7.5 기준선 측정 |
| AIJ-0008 | `queueToken` 을 게이트웨이가 발급한다는 데 발급 계층이 동의한다 | 계약표(00-req 8절)에 항목 없음. 협의 필요 |
| AIJ-0008 | IP 리미터가 시험 환경(NAT·출발지 IP 8개)에서 부하 생성기를 막지 않는다 | 시험 프로파일 상한 별도 지정 |
| ~~AIJ-0009~~ | ~~PIT 임계 90%(생존 ≤10%)가 타당하다~~ | ✅ 도메인 125 뮤턴트에서 생존 2.4%. 임계가 느슨해 8% 도 통과했다 — **숫자보다 어디가 살았는지를 본다** (AIJ-0011) |
| ~~AIJ-0009~~ | ~~JaCoCo PACKAGE 규칙이 빈 패키지에서 조용히 통과하지 않는다~~ | ✅ 도메인 분기 100% 미달 시 빌드 실패 확인 |
| ~~AIJ-0010~~ | ~~`SecondWindowLimiter` 가 요청 경로의 동시성을 견딘다~~ | ✅ `synchronized` + `AtomicAcquireTest` |
| AIJ-0010 | 노드 번호(`nodeIndex`)가 틱마다 안정적이다 | Phase 4 하트비트 설계 시 확인. 바뀌면 배분이 출렁인다 |
| AIJ-0011 | 남은 생존 뮤턴트 3건이 정말 등가다 | 논증으로만 확인했다. 도구가 보장하지 않는다 |
| AIJ-0011 | 샤드 균등 분포 가정이 쏠린 순간에도 성립한다 | Phase 3 에서 실측 |
| AIJ-0013 | `guard-pr.sh` 의 base 추출이 모든 입력 형태를 다룬다 | `--base origin/develop` 같은 중복 접두 미처리 |
| AIJ-0012 | 폴링 밴드 간격 1/3/10 초가 예산 4,000 RPS 와 맞는다 | 30 초만 계획서가 못 박았다. Phase 6 실측 |
| AIJ-0012 | ETA 버킷 경계 30/90/450 초가 이탈 판단 구간과 맞는다 | 문구에서 역산했다. 실측 필요 |
| AIJ-0012 | 히스테리시스 최소 유지 3틱이 적절하다 | 계획서에 기본값이 없다 |
| AIJ-0007 | `everysec` 유실 1초가 재등록으로 덮인다 | Phase 6 부하 하네스 (6.6) |
| AIJ-0007 | `TIME` 을 쓰는 Lua 가 복제에서 안전하다 | Phase 3 |
| AIJ-0007 | 폴링마다 붙는 `ZCOUNT` 부하가 감당된다 | Phase 3 |

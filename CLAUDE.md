# waiting — 대기열 게이트웨이

> 이 파일은 **규범의 진입점**이다. `AGENTS.md`는 이 파일의 심볼릭 링크이므로
> 어느 도구로 열어도 같은 내용이다. 규칙을 여기 복사하지 말고 **링크**한다 —
> 사본이 생기는 순간 둘이 갈라지고, 그때부터 아무도 믿지 않는다.

---

## 1. 프로젝트 한 줄 요약

Spring WebFlux 기반 **적응형 대기열 게이트웨이**. 뒷단은 쿠폰 서비스 N대.
대규모 선착순 쿠폰 발급에서 **뒷단 가용량에 맞춰 유입을 조절**하고,
부하가 없는 쿠폰은 대기열 없이 통과시킨다.

| 요구 | 내용 |
|---|---|
| R1 | 부하 없는 쿠폰은 즉시 통과, 몰리는 쿠폰만 대기열 |
| R2 | 뒷단 N대의 가용량 비율에 맞춰 서빙 |
| R3 | 매진 쿠폰은 Redis·백엔드를 거치지 않고 게이트웨이가 종결 |
| R4 | 피크 100K / 동시 대기 20,000 |
| R5 | **장애 진입·유지·회복 전 구간에서 정합성과 공정성 유지** |

**현재 상태**: 제로베이스 재작성 중. **Phase 9(Routing) — v0.6.0. 기본 전략을 라운드로빈으로 뒤집었고 게이트 둘이 남았다.**
Phase 1~7 은 닫혔다. Phase 8 은 부분 충족으로 닫았고, Phase 9 는 검증(9.4)을 다 쟀다 — **라운드로빈으로는** 전부 넘겼다(종료 시 유출과 앓는 대 배제까지). 게이트웨이 둘에서 쏠림까지 재고 기본값을 뒤집었다: P2C 를 고른 이유였던 몰림은 안 나타났고 오히려 P2C 가 작은 대에 +36% 를 보낸다. 남은 것은 그 P2C 게이트와, 뒷단이 보고에 `inFlight` 를 실어야 닫히는 콜드 스타트다. 무엇이 남았는지는 각 게이트 표가 든다. 브랜치는 태스크 단위로 딴다 (WF-3).
진행 상황은 [plan/README.md](plan/README.md) 2절, 저장소 소개는 [README.md](README.md).

---

## 2. 절대 어기면 안 되는 것

작업 전에 이것부터 확인한다. 나머지 규칙은 상황에 따라 판단하지만 이 넷은 예외가 없다.

| # | 불변식 | 깨지면 |
|---|---|---|
| **1** | **요청 경로에서 Redis를 치지 않는다** | 게이트웨이의 존재 이유가 사라진다 |
| **2** | **초과 발급 0** | 재고보다 많이 발급되면 제품이 성립하지 않는다 |
| **3** | **순번 역행 0** | 사용자가 뒤로 밀린다. 선착순의 신뢰가 깨진다 |
| **4** | **줄 선 사람을 추월시키지 않는다** | 장애 중에도. 공정성은 가용성과 맞바꿀 수 없다 |

4번이 특히 함정이다. "상태를 모른다"는 것이 추월을 정당화하지 않는다.
→ [plan/02-domain-core.md#f1](plan/02-domain-core.md)

---

## 3. 디렉터리 지도

### 형제 저장소

```
waiting-gateway/
├── waiting/          ← 이 저장소. 게이트웨이 본체
├── ci-actions/       ← Atlassian 연동 액션 (Jira, Confluence). 별도 git 저장소
│                     원격은 coupon-yaho/cy-ci-actions — uses: 에 쓸 이름이다
└── waiting-legacy/   ← 제로베이스 이전 구현. git 밖. 읽기 전용
```

발급 계층은 별도 저장소 `cy-be` 에 있다 (v1/v2/v3 비교 실험).
게이트웨이를 거기 넣지 않은 근거는 [plan/90-decisions.md](plan/90-decisions.md) O-1.
**게이트웨이 안에서는 더 쪼개지 않는다** — 저장소 하나, **배포 단위도 하나**다.
배분 스케줄러는 같은 바이너리의 토글이고 **리더로 뽑힌 노드 한 대만** 배분을 돈다.
틱 지연이 커지면 그때 별도 배포 단위로 뗀다. 저장소 경계는 배포 단위가 아니라
불변식이 걸치는 범위를 따른다 (O-2).

| 저장소 | 무엇 | 손대는 때 |
|---|---|---|
| `waiting` | 게이트웨이 코드·계획·규범 | 대부분 |
| [`ci-actions`](https://github.com/coupon-yaho/cy-ci-actions) | Jira·Confluence 액션 (TypeScript) | CI 연동을 고칠 때 |
| `waiting-legacy` | 설계 논거 참조 | 읽기만. **수정 금지 (WF-5)** |

### 이 저장소

```
waiting/
├── README.md                          ← 저장소 대문 (GitHub 이 이걸 보여준다)
├── CLAUDE.md ─── AGENTS.md(symlink)   ← 지금 이 파일
├── plan/                              ← 무엇을 왜 만드는가 (페이즈·게이트·결정)
├── ai/                                ← AI 작업 규범 (도구 무관)
│   ├── rules/                         ← 코딩·설계 규칙
│   └── journal/                       ← AI 작업 로그 (근거 축적)
├── .claude/                           ← Claude Code 배선
│   ├── agents/                        ← 리뷰 서브에이전트
│   ├── hooks/                         ← 자동 검사 스크립트
│   ├── commands/                      ← 슬래시 커맨드
│   └── settings.json                  ← 훅 등록
├── .github/                           ← CI. 최소 단위로 쪼개 참조한다
│   └── CI.md                          ← 파이프라인 구조 (README 로 두면 저장소 대문을 가로챈다)
│   ├── actions/                       ← composite action (스텝 단위)
│   ├── workflows/_*.yml               ← 재사용 워크플로 (잡 단위)
│   └── workflows/{pr,main,nightly}.yml ← 진입점
├── .coderabbit.yaml                   ← PR 리뷰 규칙 (ai/rules/ 를 참조)
└── src/                               ← 순수 도메인 (Phase 2)
```

**`ai/` 와 `.claude/` 의 분담**

| | 담는 것 | 이유 |
|---|---|---|
| `ai/` | 규칙·로그·절차 — **지식** | 도구가 바뀌어도 남는다. 사람도 읽는다 |
| `.claude/` | 훅·에이전트·커맨드 — **배선** | Claude Code가 이 경로에서만 자동 인식한다 |

에이전트 정의가 `.claude/agents/`에 있는 것은 취향이 아니라 **디스커버리 제약**이다.
내용상의 규칙은 전부 `ai/rules/`를 참조하게 하여 사본을 만들지 않는다.

---

## 4. 작업 전 필독

| 상황 | 읽을 것 |
|---|---|
| **항상** | [ai/rules/00-index.md](ai/rules/00-index.md) — 규칙 색인 |
| **무엇을 왜 만드는지** | [plan/00-requirements.md](plan/00-requirements.md) — 요구사항·비목표·제약·가정 |
| 무엇을 만들지 모를 때 | [plan/README.md](plan/README.md) → 해당 페이즈 문서 |
| 코드를 쓰기 전 | 해당 패키지 규칙 ([ai/rules/70-packages.md](ai/rules/70-packages.md)) |
| 왜 이렇게 됐는지 모를 때 | [plan/90-decisions.md](plan/90-decisions.md) → [ai/journal/](ai/journal) — 색인은 `.github/scripts/journal-index.sh` 가 만든다 |
| 착수가 막혔을 때 | [plan/README.md](plan/README.md) 0절 — 차단 중인 결정 |
| 기존 구현이 궁금할 때 | `../waiting-legacy/` — **읽기 전용. 수정 금지** |

> **비목표를 만들고 있지 않은지 확인한다.** 게이트웨이는 재고를 차감하지 않고,
> 발급 로직을 갖지 않으며, 순위·ETA 의 정확한 값을 보장하지 않는다.
> 전체 목록: [plan/00-requirements.md](plan/00-requirements.md) 5절

---

## 5. 작업 절차

```
0. 브랜치 생성    feature/CY-<번호>-<슬러그>   ← Jira 키 필수. 없으면 CI 가 막는다
1. 페이즈 문서에서 태스크 확인          plan/0N-*.md
2. 해당 규칙 확인                       ai/rules/
3. RED   — 실패하는 테스트    커밋: test(scope): ...
4. GREEN — 최소 구현          커밋: feat(scope): ...
5. REFACTOR (선택)            커밋: refactor(scope): ...
6. 작업 로그 기록                       /journal  또는 ai/journal/
7. 로컬 리뷰                            /review  ← PR 전에 끝낸다
8. PR → develop               squash 금지. TDD 사이클 커밋이 이력의 목적이다
```

**git flow 는 Phase 1 착수부터 적용한다.** 그 전(base 작업)은 `main` 직접 커밋 + amend.
브랜치 전략 전문: [ai/rules/60-workflow.md](ai/rules/60-workflow.md) WF-3

**6번을 건너뛰지 않는다.** 왜 그렇게 했는지, 무엇을 버렸는지는 코드에 남지 않는다.
→ [ai/journal/README.md](ai/journal/README.md)

---

## 6. 규칙과 자동 검사

전문과 MUST 목록, 훅이 무엇을 차단하는지는
[ai/rules/00-index.md](ai/rules/00-index.md) 한 곳에 있다.
여기 옮겨 적지 않는다 — 사본이 생기면 둘이 갈라진다.

훅 정의는 [.claude/settings.json](.claude/settings.json), 스크립트는 `.claude/hooks/`.

---

## 8. 리뷰 — PR 을 올리기 전에 끝낸다

**`/review` 를 돌린다.** 원격에서 지적받고 고치는 왕복은 비싸고, 그 사이
잘못된 코드가 브랜치에 남는다. 절차 전문: [.claude/commands/review.md](.claude/commands/review.md)

```bash
.claude/hooks/review-branch.sh          # 기계 검사 — CodeRabbit 이 볼 것을 먼저 본다
./gradlew build jacocoTestCoverageVerification pitest
```

`gh pr create` 는 **기계 검사가 통과해야 실행된다** (`.claude/hooks/guard-pr.sh`).
막히면 우회하지 말고 고친다.

> **왜 브랜치 전체를 다시 보는가.** `check-java.sh`·`check-lua.sh` 는
> `Write|Edit` 훅이라 **그 도구로 쓴 파일만** 본다. 힙독이나 스크립트로 만든
> 파일은 통째로 지나가고, 실제로 그렇게 들어간 위반이 CodeRabbit 까지 갔다.

기계가 통과했다고 끝이 아니다. 해당 영역 에이전트를 돌린다 — 사람 리뷰의
앞단이지 대체가 아니다.

| 에이전트 | 언제 |
|---|---|
| `domain-guardian` | `domain` 패키지를 건드렸을 때 |
| `admission-auditor` | 판정·배분·폴링 — **결과가 맞는지** |
| `resilience-auditor` | 장애·회복 경로, fail-open, 서킷, 리트라이 |
| `redis-cluster-checker` | 키 스킴·샤딩 |
| `lua-optimizer` | Lua 의 정확성과 비용 |
| `security-reviewer` | 시크릿·CI 공급망·입력 검증 |
| `test-quality-reviewer` | 테스트를 추가·수정했을 때 |
| `style-enforcer` | 커밋 직전 (전 영역) |

**릴리스 전에는 전부 돌린다.** 되돌릴 수 없는 병합이라 한 관점이라도 비면
그만큼 못 본 채로 나간다.

정의: `.claude/agents/`

---

## 9. 하위 CLAUDE.md

패키지 규칙이 50줄을 넘거나 루트 규칙과 충돌하면 그 패키지에 `CLAUDE.md` 를 만든다.
그 전까지는 [ai/rules/70-packages.md](ai/rules/70-packages.md) 에 둔다.
만들 때는 루트 규칙을 반복하지 않고 차이만 적는다.

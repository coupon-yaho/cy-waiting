# Jira 매핑

**계획서 태스크 ID와 Jira 이슈 키의 대응은 여기서만 관리한다.**

계획 문서(`plan/0N-*.md`)에는 Jira 번호를 쓰지 않는다.
계획서 ID는 우리가 소유해 안정적이고, Jira 키는 발번 시점에 정해진다.
둘을 분리해 두면 Jira를 재구성해도 계획 문서를 손대지 않는다.

---

## 1. 스페이스

| 항목 | 값 |
|---|---|
| 프리픽스 | **`CY`** |
| 상태 | 기존 사용 중인 스페이스. 번호는 발번 시점에 정해진다 |

---

## 2. 계층

```
Epic       =  Phase                   이름은 반드시 "대기열 — " 로 시작
 ├─ Task   =  페이즈 문서의 Task 절   예: "2.3 판정"
 │   └─ Sub-task = 그 태스크 안의 개별 일·결함·후속
 └─ Task   =  Exit Gate               게이트도 작업이다
Sprint     =  시간 컨테이너. 에픽을 가로지른다
```

**`CY` 는 게이트웨이 전용이 아니다.** 쿠폰 서비스·관리자·관제가 같은 프로젝트를
쓴다. 그래서 에픽 이름에 `대기열 — ` 을 붙인다 — 안 붙이면 보드에서 남의 에픽과
구분이 안 된다.

### 부모 없는 이슈를 만들지 않는다 (WF-8)

`작업` 은 에픽 아래, `하위 작업` 은 작업 아래. 리뷰 지적이나 릴리스 결함처럼
나중에 나온 것도 예외가 아니다 — **어느 태스크의 영역인지** 정하고 그 아래
`하위 작업` 으로 단다.

계획서 세부 ID(`2.3.9`)를 그대로 복제하라는 뜻은 아니다. 복제하면 계획이 바뀔 때
둘이 갈라진다. 하위 작업으로 뜨는 것은 **실제로 사람이 따로 집어 드는 단위** —
결함, 후속 조치, 쪼갠 일 — 뿐이고, 나머지 세부 항목은 Task 설명의 체크리스트로
남는다.

추적은 커밋 한 줄이 계속 담당한다.

```
Refs: CY-145     ← Jira
```

### Phase ≠ Sprint

페이즈는 **완료 기준으로 닫히고**, 스프린트는 **시간으로 닫힌다.**
페이즈를 스프린트에 맞추면 게이트 기준을 스프린트 끝에 맞춰 낮추게 된다 —
이 프로젝트에서 가장 피해야 할 실패 모드다.

**게이트 미달로 스프린트가 끝나면 그 페이즈는 다음 스프린트로 넘어간다.**
기준은 그대로 둔다.

---

## 3. Epic 매핑

9절 대응표와 함께 관리한다.

| Phase | 에픽 이름 | Epic |
|---|---|---|
| 1 | 검증 기반 구축 | `CY-18` |
| 2 | 입장 판정 도메인 | `CY-19` |
| 3 | 레디스 키와 스크립트 | [`CY-227`](https://shseol.atlassian.net/browse/CY-227) |
| 4 | 제어 평면 (스냅샷·가용량·배분) | [`CY-267`](https://shseol.atlassian.net/browse/CY-267) |
| 5 | 요청 경로 | `CY-22` |
| 6 | 보호 장치와 실측 | — |
| 7 | 대기열 생명주기 | — |
| 8 | 장애 회복력 | `CY-24` |
| 9 | 가용량 기반 라우팅 | `CY-25` |
| 10 | 규모 확장 | `CY-26` |

표의 "에픽 이름" 은 `대기열 — ` 뒤에 오는 부분이다. 실제 제목은
`대기열 — 레디스 키와 스크립트` 처럼 붙여 쓴다.

> **제목에 번호를 넣지 않는다.** 계획서 ID 는 라벨(`phase-1`, `plan-2.3`, `gate-1`)로
> 붙는다 — 사람은 이름을 읽고 기계는 라벨로 찾는다.

---

## 4. Task 매핑

페이즈 문서의 각 태스크 절이 Task 하나가 된다. 세부 항목(`2.3.9`)은 그 Task
설명의 체크리스트로 들어간다 — 통째로 복제하면 계획이 바뀔 때마다 둘이 갈라진다.

**따로 집어 드는 단위만 `하위 작업` 으로 뜬다** — 결함, 리뷰 지적, 후속 조치.
이것들은 계획서에 없던 것이라 체크리스트로는 안 잡히고, 저마다 브랜치와 PR 이
붙는다. 뜰 때 부모 태스크를 반드시 지정한다 (WF-8).

키 목록은 8절에 있다. 스크립트가 채우므로 여기 옮겨 적지 않는다.

| 라벨 | 무엇 |
|---|---|
| `phase-N` | 에픽 |
| `plan-N.M` | 태스크 절 |
| `gate-N` | 종료 게이트 |
| `d-x1` `d-c1` … | 협의 |

---

## 5. 결정 항목 매핑

차단 항목은 **선행 티켓**으로 만들어 해당 에픽을 블로킹시킨다.

| 결정 | 내용 | 차단 대상 | 우선 |
|---|---|---|---|
| **D-X1** | 적응형 대기열 팀 합의 (상위 PRD 9절은 Out of Scope) | **프로젝트 전체** | 🔴 |
| ~~D-C1~~ | ~~가용량 보고 계약 확대~~ | — | ✅ `{instanceId, addr, credits, ts}` + 60초 램프 (A-11·A-12·A-13) |
| D-X2 | Spring Boot 버전 (PRD 3.x vs 실제 4.1.0) | 전체 | 🟡 |
| D-L1 | 큐 진입 지연 예산 (절대값) | Phase 10 착수 판정 | 🟡 |
| ~~D-C2~~ | ~~매진 응답 식별 신호~~ | — | ✅ `409` + `problem+json` (B-10·B-11) |
| ~~D-C3~~ | ~~`coupon:policy` 형식 확장~~ | — | ✅ JSON + `pcall(cjson.decode)` (E-12) |
| ~~D-P1~~ | ~~Redis 영속성 정책~~ | — | ✅ `everysec` + 복제본 (E-6) |
| ~~D-R1~~ | ~~서비스 디스커버리~~ | — | ✅ 가용량 보고가 곧 레지스트리 (R-1) |
| ~~D-S1~~ | ~~샤드 해시 함수~~ | — | ✅ CRC16 (E-7) |
| ~~D-T1~~ | ~~토큰 재사용 방지 주체~~ | — | ✅ 발급 계층 멱등성 (A-10) |
| ~~D-Q1~~ | ~~유령 대기자 보정 방식~~ | — | ✅ 표본 alive 존재율 (D-9) |
| ~~D-A1~~ | ~~인증 계층 존재 여부~~ | — | ✅ 상위 PRD 12절 |
| ~~D-O1~~ | ~~저장소 배치~~ | — | ✅ 별도 저장소 (O-1) |
| ~~D-M1~~ | ~~커밋 제목 언어~~ | — | ✅ 한글 |

**협의 항목은 티켓으로 만들지 않는다.** 다른 팀과의 합의라 우리 보드에서 진행 상태를
추적할 수 있는 성질이 아니다. 이 표가 유일한 기록이다.

> **D-C1·D-C2·D-C3 는 2026-08-17 에 확정됐다** ([90-decisions.md](90-decisions.md) 2.10절).
> D-C1 은 D-R1 을 가용량 보고 기반 레지스트리로 풀면서 Phase 9 전체가 걸려 있었는데,
> 뒷단 팀 합의로 풀렸다. **남은 🔴 는 D-X1 하나다.**

🔴 는 해소 전까지 해당 에픽을 시작하지 않는다.
D-X1·D-X2 는 **다른 팀과의 협의**가 필요하므로 협의 티켓을
별도로 만들고 이 표에 함께 기록한다.

---

## 6. 커밋에서 되짚기

커밋 푸터는 **Jira 키 하나만** 남긴다.

```
feat(admission): 전역 크레딧 기반 통과 상한 산출

Refs: CY-145
```

```bash
git log --grep='Refs: CY-145'      # 특정 이슈의 모든 커밋
```

계획서 ID 를 커밋에 박지 않는 이유는 [60-workflow.md](../ai/rules/60-workflow.md) 3절에 있다 —
문서를 개편하면 ID 가 밀리는데 이력은 못 고친다. 계획서와 티켓의 대응은
9절 표 한 곳에서만 관리하고, 계획이 바뀌면 그 표만 고친다.

---

## 7. CI 와의 고리

**티켓 키가 브랜치명을 타고 CI 로 들어간다.** 사람이 Jira 를 손으로 만질 일이 없다.

```
CY-42 입장 판정
   ↓  브랜치를 이 키로 판다
feature/CY-42-r1-idle-pass
   ↓  PR 을 연다
CI 가 브랜치명에서 CY-42 를 뽑아
   · 진행 중으로 전이
   · PR 링크를 이슈에 붙인다 (같은 URL 은 중복되지 않는다)
   · CodeRabbit 지적이 있으면 코멘트로 남긴다
   ↓  main 에 병합
   · 완료로 전이
   · 결과를 컨플루언스 리포트에 남긴다
```

배선은 [`.github/workflows/`](../.github/workflows/) 의 `_report.yml` 과
`coderabbit-relay.yml` 이고, 액션은 `coupon-yaho/cy-ci-actions/actions/jira@v1` 다.

**브랜치명에 키가 없으면 CI 가 조용히 건너뛴다.** 그래서 규범 검증 잡이 브랜치명을
먼저 막는다 ([60-workflow.md](../ai/rules/60-workflow.md) WF-3).

### 필요한 시크릿

저장소 Settings → Secrets and variables → Actions 에 넣는다.

| 이름 | 값 |
|---|---|
| `ATLASSIAN_BASE_URL` | `https://shseol.atlassian.net` |
| `ATLASSIAN_USER_EMAIL` | 토큰을 발급한 계정의 이메일 |
| `ATLASSIAN_API_TOKEN` | 그 계정의 API 토큰 |

인증이 `email:token` 이라 **둘이 같은 계정 것이어야 한다.** 다르면 401 이 나는데,
연동 실패는 기본적으로 경고로 낮아지므로 **워크플로는 초록불인 채 아무 일도 하지
않는다.** 처음 붙일 때 이슈에 코멘트가 실제로 달리는지 한 번 확인한다.

---

## 8. 갱신 책임

| 시점 | 할 일 |
|---|---|
| 계획서에 태스크 절이 늘거나 이름이 바뀜 | 티켓을 만들고 9절 대응표에 추가한다 |
| 페이즈 착수로 문서가 완전 명세로 승격 | 같다. 멱등하므로 새로 생긴 것만 만든다 |
| 협의 결과가 나옴 | 5절 표를 고치고 해당 티켓을 닫는다 |

**표를 손으로 채우지 않는다.** `--map` 이 3·5·9절을 채운다. 손으로 고치면 다음
실행에 덮이거나 갈라진다.

갱신은 `docs(plan): Jira 이슈 매핑 갱신` 커밋으로 남긴다.

## 9. 계획서 ID 와 티켓 대응

**티켓에 라벨을 붙이지 않는다** — 보드에서 방해만 된다. 연결은 이 표가 진다.

| 계획서 | 티켓 |
|---|---|
| `phase-1` | [`CY-18`](https://shseol.atlassian.net/browse/CY-18) |
| `phase-2` | [`CY-19`](https://shseol.atlassian.net/browse/CY-19) |
| `phase-3` | [`CY-227`](https://shseol.atlassian.net/browse/CY-227) |
| `phase-5` | [`CY-22`](https://shseol.atlassian.net/browse/CY-22) |
| `phase-8` | [`CY-24`](https://shseol.atlassian.net/browse/CY-24) |
| `phase-9` | [`CY-25`](https://shseol.atlassian.net/browse/CY-25) |
| `phase-10` | [`CY-26`](https://shseol.atlassian.net/browse/CY-26) |
| `plan-1.3` | [`CY-34`](https://shseol.atlassian.net/browse/CY-34) |
| `plan-1.3.1` | [`CY-105`](https://shseol.atlassian.net/browse/CY-105) |
| `plan-1.3.2` | [`CY-106`](https://shseol.atlassian.net/browse/CY-106) |
| `plan-1.3.3` | [`CY-107`](https://shseol.atlassian.net/browse/CY-107) |
| `plan-1.3.4` | [`CY-108`](https://shseol.atlassian.net/browse/CY-108) |
| `plan-1.3.5` | [`CY-109`](https://shseol.atlassian.net/browse/CY-109) |
| `plan-1.3.6` | [`CY-110`](https://shseol.atlassian.net/browse/CY-110) |
| `plan-1.3.7` | [`CY-111`](https://shseol.atlassian.net/browse/CY-111) |
| `plan-1.3.8` | [`CY-112`](https://shseol.atlassian.net/browse/CY-112) |
| `plan-2.1` | [`CY-40`](https://shseol.atlassian.net/browse/CY-40) |
| `plan-2.1.1` | [`CY-128`](https://shseol.atlassian.net/browse/CY-128) |
| `plan-2.1.2` | [`CY-129`](https://shseol.atlassian.net/browse/CY-129) |
| `plan-2.1.3` | [`CY-130`](https://shseol.atlassian.net/browse/CY-130) |
| `plan-2.1.4` | [`CY-131`](https://shseol.atlassian.net/browse/CY-131) |
| `plan-2.1.5` | [`CY-132`](https://shseol.atlassian.net/browse/CY-132) |
| `plan-2.1.6` | [`CY-133`](https://shseol.atlassian.net/browse/CY-133) |
| `plan-2.1.7` | [`CY-134`](https://shseol.atlassian.net/browse/CY-134) |
| `plan-2.2` | [`CY-41`](https://shseol.atlassian.net/browse/CY-41) |
| `plan-2.2.1` | [`CY-135`](https://shseol.atlassian.net/browse/CY-135) |
| `plan-2.2.2` | [`CY-136`](https://shseol.atlassian.net/browse/CY-136) |
| `plan-2.2.3` | [`CY-137`](https://shseol.atlassian.net/browse/CY-137) |
| `plan-2.2.4` | [`CY-138`](https://shseol.atlassian.net/browse/CY-138) |
| `plan-2.3` | [`CY-42`](https://shseol.atlassian.net/browse/CY-42) |
| `plan-2.3.1` | [`CY-139`](https://shseol.atlassian.net/browse/CY-139) |
| `plan-2.3.2` | [`CY-140`](https://shseol.atlassian.net/browse/CY-140) |
| `plan-2.3.3` | [`CY-141`](https://shseol.atlassian.net/browse/CY-141) |
| `plan-2.3.4` | [`CY-142`](https://shseol.atlassian.net/browse/CY-142) |
| `plan-2.3.5` | [`CY-143`](https://shseol.atlassian.net/browse/CY-143) |
| `plan-2.3.6` | [`CY-144`](https://shseol.atlassian.net/browse/CY-144) |
| `plan-2.3.7` | [`CY-145`](https://shseol.atlassian.net/browse/CY-145) |
| `plan-2.3.8` | [`CY-146`](https://shseol.atlassian.net/browse/CY-146) |
| `plan-2.3.9` | [`CY-147`](https://shseol.atlassian.net/browse/CY-147) |
| `plan-2.4` | [`CY-43`](https://shseol.atlassian.net/browse/CY-43) |
| `plan-2.4.1` | [`CY-148`](https://shseol.atlassian.net/browse/CY-148) |
| `plan-2.4.2` | [`CY-149`](https://shseol.atlassian.net/browse/CY-149) |
| `plan-2.4.3` | [`CY-150`](https://shseol.atlassian.net/browse/CY-150) |
| `plan-2.4.4` | [`CY-151`](https://shseol.atlassian.net/browse/CY-151) |
| `plan-2.5` | [`CY-44`](https://shseol.atlassian.net/browse/CY-44) |
| `plan-2.5.1` | [`CY-152`](https://shseol.atlassian.net/browse/CY-152) |
| `plan-2.5.2` | [`CY-153`](https://shseol.atlassian.net/browse/CY-153) |
| `plan-2.5.3` | [`CY-154`](https://shseol.atlassian.net/browse/CY-154) |
| `plan-2.5.4` | [`CY-155`](https://shseol.atlassian.net/browse/CY-155) |
| `plan-2.6` | [`CY-45`](https://shseol.atlassian.net/browse/CY-45) |
| `plan-2.6.1` | [`CY-156`](https://shseol.atlassian.net/browse/CY-156) |
| `plan-2.6.2` | [`CY-157`](https://shseol.atlassian.net/browse/CY-157) |
| `plan-2.6.3` | [`CY-158`](https://shseol.atlassian.net/browse/CY-158) |
| `plan-2.7` | [`CY-46`](https://shseol.atlassian.net/browse/CY-46) |
| `plan-2.7.1` | [`CY-159`](https://shseol.atlassian.net/browse/CY-159) |
| `plan-2.7.2` | [`CY-160`](https://shseol.atlassian.net/browse/CY-160) |
| `plan-2.7.3` | [`CY-161`](https://shseol.atlassian.net/browse/CY-161) |
| `plan-2.8` | [`CY-47`](https://shseol.atlassian.net/browse/CY-47) |
| `plan-2.8.1` | [`CY-162`](https://shseol.atlassian.net/browse/CY-162) |
| `plan-6.5` | [`CY-38`](https://shseol.atlassian.net/browse/CY-38) |
| `plan-6.5.1` | [`CY-125`](https://shseol.atlassian.net/browse/CY-125) |
| `plan-6.5.2` | [`CY-126`](https://shseol.atlassian.net/browse/CY-126) |
| `plan-6.5.3` | [`CY-127`](https://shseol.atlassian.net/browse/CY-127) |
| `plan-6.6` | [`CY-37`](https://shseol.atlassian.net/browse/CY-37) |
| `plan-6.6.1` | [`CY-119`](https://shseol.atlassian.net/browse/CY-119) |
| `plan-6.6.2` | [`CY-120`](https://shseol.atlassian.net/browse/CY-120) |
| `plan-6.6.3` | [`CY-121`](https://shseol.atlassian.net/browse/CY-121) |
| `plan-6.6.4` | [`CY-122`](https://shseol.atlassian.net/browse/CY-122) |
| `plan-6.6.5` | [`CY-123`](https://shseol.atlassian.net/browse/CY-123) |
| `plan-6.6.7` | [`CY-124`](https://shseol.atlassian.net/browse/CY-124) |
| `plan-8.0` | [`CY-36`](https://shseol.atlassian.net/browse/CY-36) |
| `plan-8.0.1` | [`CY-114`](https://shseol.atlassian.net/browse/CY-114) |
| `plan-8.0.2` | [`CY-115`](https://shseol.atlassian.net/browse/CY-115) |
| `plan-8.0.3` | [`CY-116`](https://shseol.atlassian.net/browse/CY-116) |
| `plan-8.0.4` | [`CY-117`](https://shseol.atlassian.net/browse/CY-117) |
| `plan-8.0.5` | [`CY-118`](https://shseol.atlassian.net/browse/CY-118) |
| `gate-1` | [`CY-39`](https://shseol.atlassian.net/browse/CY-39) |
| `gate-2` | [`CY-48`](https://shseol.atlassian.net/browse/CY-48) |
| `plan-3.1` | [`CY-228`](https://shseol.atlassian.net/browse/CY-228) |
| `plan-3.2` | [`CY-229`](https://shseol.atlassian.net/browse/CY-229) |
| `plan-3.3` | [`CY-230`](https://shseol.atlassian.net/browse/CY-230) |
| `plan-3.4` | [`CY-231`](https://shseol.atlassian.net/browse/CY-231) |
| `plan-3.5` | [`CY-232`](https://shseol.atlassian.net/browse/CY-232) |
| `plan-3.6` | [`CY-233`](https://shseol.atlassian.net/browse/CY-233) |
| `plan-3.7` | [`CY-234`](https://shseol.atlassian.net/browse/CY-234) |
| `plan-3.8` | [`CY-235`](https://shseol.atlassian.net/browse/CY-235) |
| `plan-3.9` | [`CY-236`](https://shseol.atlassian.net/browse/CY-236) |
| `gate-3` | [`CY-237`](https://shseol.atlassian.net/browse/CY-237) |

# Phase 1 — Foundation

**선행**: 없음 · **상세 수준**: 완전 명세

---

## Goal

> **코드를 쓰기 전에 갖춰야 할 것만 세운다.**
> 빌드가 돌고, 규칙이 기계적으로 강제되고, 품질 미달이면 빌드가 깨진다.

| 통과 수치 | |
|---|---|
| 빌드 | `./gradlew build` **CI 통과** |
| 애플리케이션 | 기동 후 **정상 종료** |
| 규약 위반 커밋 | **거부됨** |
| 품질 임계 | 미달 시 **빌드 실패** |
| 훅 자기검증 | **전건 통과** |

---

## 1. 왜 이것만 먼저인가

**여기 있는 것은 전부 코드 없이 성립하고 첫 커밋부터 쓰인다.** 빌드·CI·커밋 규약은
Phase 2 의 첫 테스트를 쓰는 순간부터 필요하다.

**하네스와 관측은 여기 없다.** 끊을 게이트웨이도, 200 을 줄 서버도, 낼 지표도 없는
시점에 만들면 **게이트가 통과할 수 없다.** 페이즈가 자기 규칙에 걸린다.

| 옮긴 것 | 어디로 | 왜 |
|---|---|---|
| 부하 하네스 · 관측 | Phase 6 | 게이트웨이가 요청을 받기 시작해 잴 것이 생기는 지점 |
| 장애 주입 하네스 | Phase 8 직전 | 쓰기 직전. 요구가 확정된 뒤에 만든다 |

"미리 만들어두면 압박 없이 제대로 만든다"는 논리는 맞지만 **"쓰기 직전"이면
충족된다.** 실제로 복제본 승격 시나리오가 나중에 추가되면서 하네스 요구가 늘었다 —
미리 만들었으면 그때는 없던 요구다.

---

## 2. Tasks

형식: [README 5절](README.md).

### 2.1 저장소 초기화 — 완료됨

| ID | 작업 | 상태 |
|---|---|---|
| T1.1.1 | `git init` + `.gitignore` / `.gitattributes` | 완료 |
| T1.1.2 | `plan/` 문서 | 완료 |
| T1.1.3 | `ai/` 규칙·작업 로그, `.claude/` 훅·에이전트 | 완료 |

> 커밋 해시를 적지 않는다. base 작업 구간에는 amend 로 이력을 다듬고 있어
> 해시가 계속 바뀐다. git flow 를 시작하면 그때부터 해시가 안정적이 된다.

### 2.2 빌드 스캐폴딩

#### T1.2.1 · Gradle 프로젝트

- **산출물** `settings.gradle`, `build.gradle`, `gradle/wrapper/`
- **근거** Java 21 · Spring Boot 4.1.0 · Spring Cloud 2025.1.2

1. **GREEN** wrapper + toolchain 21 + Spring Cloud BOM
2. **툴체인 자동 provisioning 을 켠다** — `settings.gradle` 에 foojay resolver.
   개발 기기·러너의 JDK 가 21 이 아닐 수 있다 (지금 이 기기는 24 다)
3. **패키지 루트는 `com.kafkick.waiting`** — `cy-be` 와 같은 `com.kafkick` 아래 둔다
4. **의존성은 최소만** — `webflux`, `gateway-server-webflux`, `actuator`
5. **완료** `./gradlew build` 성공 — **JDK 21 이 없는 기기에서도**

> **`com.kafkick` 은 조직 규칙이다.** `cy-be` 가 `com.kafkick.{api,core,storage}` 를
> 쓰고 있고, **두 저장소를 합칠 가능성**이 있어 지금부터 맞춘다. 합칠 때
> 패키지를 전부 옮기는 것이 가장 비싸다.

#### T1.2.4 · testFixtures 소스셋

- **산출물** `build.gradle` (`java-test-fixtures` 플러그인)
- **근거** TS-3 (픽스처는 도달 가능한 상태만) · `cy-be` 의 `storage` 모듈 선례

1. **GREEN** `java-test-fixtures` 플러그인 + `src/testFixtures/java`
2. **완료** 픽스처가 **프로덕션 클래스패스에 없고** 테스트에서 재사용된다

> 픽스처를 `src/test` 에 두면 다른 소스셋에서 못 쓴다. 프로덕션에 두면
> 도달 불가 상태를 만드는 생성자가 **운영 코드에 노출된다** — Phase 2 의 `2.1.4` 가 막으려는 것이
> 정확히 그것이다.

> **toolchain 만 선언하고 resolver 를 빼면 "No matching toolchain" 으로 빌드가 죽는다.**
> 러너는 `setup-java` 가 21 을 깔아주지만 개발 기기는 아무도 안 깔아준다.
>
> Redis·resilience4j·loadbalancer 를 **미리 넣지 않는다.** 해당 페이즈에 도달하기
> 전에 잘못된 배선이 생긴다. Phase 2 는 순수 POJO 라 Spring 도 거의 안 쓴다.

#### T1.2.2 · 기동 스모크

- **산출물** `WaitingApplication.java`, `WaitingApplicationTest.java`
- **선행** T1.2.1

1. **RED** `애플리케이션_컨텍스트가_뜬다` — 빈 `@SpringBootTest`
2. **GREEN** `@SpringBootApplication` 진입점
3. **완료** 기동 후 정상 종료 (컨텍스트 누수 없음)

#### T1.2.3 · 커버리지·뮤테이션 임계

- **산출물** `build.gradle` (JaCoCo, PIT)
- **근거** Phase 2 Goal (브랜치 100% / 생존 ≤10%)

1. **GREEN** JaCoCo — 계층별 임계 (도메인 100%, 어댑터 80%)
2. **GREEN** PIT — **`domain` 패키지 한정**
3. **GREEN** `testFixtures` 를 커버리지·뮤테이션 **대상에서 제외**
4. **완료** 임계 미달 시 `build` 가 실패한다

> **픽스처를 대상에 넣으면 도메인 100% 가 영원히 안 나온다.** 픽스처는 테스트가
> 쓰는 도구지 테스트 대상이 아니다. `testFixtures` 소스셋의 클래스는 기본적으로
> 프로덕션 클래스로 집계되므로 **명시적으로 빼야 한다** (T1.2.4).

> PIT 를 도메인에만 거는 이유: 판정·배분 오류가 곧 초과 발급이다.
> 인프라 계층은 통합·카오스가 잡는다. 전체에 걸면 빌드가 느려 아무도 안 돌린다.

### 2.3 CI — 골격 완료

파이프라인은 이미 있다 ([`.github/`](../.github/CI.md)).
**코드가 생기면 자동으로 켜진다** — `gradlew` 존재를 감지해 건너뛰거나 실행한다.

| ID | 작업 | 상태 |
|---|---|---|
| T1.3.1 | 재사용 워크플로 (`_build`/`_test`/`_security`/`_load-test`/`_report`) | ✅ |
| T1.3.2 | 진입점 분리 — PR / main / nightly 가 각기 다른 것을 돈다 | ✅ |
| T1.3.3 | 규범 검증 잡 (훅 self-test · 커밋 규약 · journal · 링크) | ✅ |
| T1.3.4 | 보안 (Trivy CVE · gitleaks · dependency-review) | ✅ |
| T1.3.5 | CodeRabbit → Slack·Jira 중계 | ✅ |
| T1.3.6 | Jira 상태 전이 · Confluence 리포트 | ✅ |

#### T1.3.7 · 서드파티 액션 SHA 고정

- **산출물** `.github/workflows/*.yml`, `.github/actions/*/action.yml`
- **근거** 공급망 — 태그는 옮겨질 수 있다

1. **GREEN** `actions/*`, `aquasecurity/*`, `gitleaks/*`, `gradle/*`, `grafana/*` 를
   커밋 SHA 로 고정하고 `# v4.1.1` 주석을 남긴다
2. **완료** 태그 참조가 0건. 이후 갱신은 dependabot 이 한다

> **최초 고정은 손으로 해야 한다.** dependabot 은 이미 SHA 인 것을 올려줄 뿐
> 태그를 SHA 로 바꿔주지 않는다.

#### T1.3.8 · 브랜치 보호 규칙

- **산출물** GitHub 저장소 설정 (코드로 남길 수 없다)
- **근거** WF-3

1. `main`·`develop` 에 필수 체크 지정 — 규범 / 단위 / 통합 / 카오스 / 보안
2. **squash merge 를 비활성화**한다 — 설정으로 막지 않으면 언젠가 눌린다
3. **완료** 체크 미통과 PR 을 병합할 수 없다
### 2.4 커밋 규약 강제

#### T1.4.1 · git 훅

- **산출물** `.githooks/commit-msg`, 설치 스크립트
- **근거** WF-1 · [AIJ-0004](../ai/journal/2026/08/AIJ-0004-review-governance-hooks.md)

1. **GREEN** `.claude/hooks/check-commit-msg.sh` 의 검증을 git 훅에서도 실행
2. **완료** Claude Code 밖에서 커밋해도 규약이 걸린다

> 도구 훅만 두면 터미널 직접 커밋이 우회한다. **한쪽만 막으면 막지 않은 것과 같다** —
> 실제로 `git -c ... commit` 탐지 누락으로 초기 커밋 전부가 검사를 우회했다.

---

### 2.5 공개 저장소 표면

**공개 저장소다.** 지금 첫 화면은 빈 파일 목록이고, 라이선스가 없으면 기본값이
**전권 유보**라 아무도 합법적으로 쓰거나 기여할 수 없다.

| ID | 작업 | 완료 |
|---|---|---|
| T1.5.1 | 루트 `README.md` — 무엇을·왜·어떻게 실행 | 처음 온 사람이 **10줄 안에** 무엇인지 안다 |
| T1.5.2 | `LICENSE` | 선택한 라이선스가 파일로 있다 |
| T1.5.3 | `CONTRIBUTING.md` — **외부 기여를 받는지** 명시 | 밖에서 온 사람이 헛수고하지 않는다 |
| T1.5.4 | GitHub 이슈 처리 — 끄거나 Jira 로 안내 | 추적이 두 곳으로 갈리지 않는다 |

> **`CLAUDE.md` 를 README 로 쓰지 않는다.** 그건 작업 규범이라 바깥 사람이 읽을
> 문서가 아니다. 서로 링크만 건다.
>
> **지금 구조로는 외부 기여가 불가능하다.** 브랜치명에 `CY-` 키를 요구하는데
> 그 키는 우리 Jira 에서만 나온다 (`pr.yml` 이 형식 불일치를 차단한다). PR 템플릿도
> Jira·Phase·근거 문서를 요구한다.
>
> **받지 않을 거면 그렇게 적는다.** 참고용 공개라고 한 줄 쓰면 밖에서 온 사람이
> 시간을 버리지 않는다. 받을 거면 포크용 경로를 따로 열어야 하는데, 그건 `WF-3`
> 를 고치는 일이라 그때 결정한다.
>
> 포크 PR 은 시크릿을 못 받아 **Jira·Slack 연동이 조용히 건너뛴다**(경고만).
>
> **이슈는 Jira 에 있는데 GitHub 이슈가 열려 있다.** 그대로 두면 밖에서 올린 이슈를
> 아무도 안 본다. 저장소 설정에서 끄거나, `ISSUE_TEMPLATE/config.yml` 로
> 빈 이슈를 막고 안내 링크만 남긴다. **Jira 는 인증이 걸려 있어 외부인이 못 쓴다**는
> 것도 그 안내에 적는다.

---

## 3. Exit Gate

| ID | 기준 | 검증 |
|---|---|---|
| G1.1 | `./gradlew build` CI 통과 | CI |
| G1.2 | 애플리케이션 기동 후 정상 종료 | T1.2.2 |
| G1.3 | 규약 위반 커밋 메시지가 **거부**된다 | T1.4.1 |
| G1.9 | 커버리지·뮤테이션 리포트가 CI 산출물로 생성 | T1.2.3 |
| G1.16 | 픽스처가 **프로덕션 클래스패스에 없다** — `testFixtures` 소스셋 | T1.2.4 |
| G1.17 | 커버리지·뮤테이션 집계에서 `testFixtures` 가 제외됨 | T1.2.3 |
| **G1.11** | **훅 자기검증 전건 통과** | T1.3.3 |
| G1.12 | journal 형식·색인 동기화가 CI 에서 검증 | T1.3.3 |
| G1.13 | 서드파티 액션이 전부 SHA 로 고정됨 | T1.3.7 |
| G1.14 | `main`·`develop` 에 필수 체크 + squash 비활성 | T1.3.8 |
| G1.15 | 루트 `README.md`·`LICENSE`·`CONTRIBUTING.md` 가 있다 | T1.5.1~T1.5.3 |

### 게이트 커밋

```
chore(gate): Phase 1 게이트 통과

빌드·기동 정상, 품질 임계 미달 시 빌드 실패 확인
훅 자기검증 전건 통과, 규약 위반 커밋 거부됨
외부 액션 SHA 고정, 브랜치 보호 규칙 적용

Refs: CY-39
```

---

## 4. 하지 않는 것

| 항목 | 이유 |
|---|---|
| Redis 의존성 | Phase 3. 미리 넣으면 Phase 2 의 순수성이 깨진다 |
| 도메인 클래스 | Phase 2 에서 TDD 로 |
| 라우트 정의 | Phase 5 |
| **관측·부하 하네스** | **Phase 6** — 지표를 낼 대상과 200 을 줄 서버가 그때 생긴다 |
| **장애 주입 하네스** | **Phase 8 직전** — 쓰기 직전에 만든다 |
| resilience4j | Phase 6 — 서킷·타임아웃·격벽 |

---

## 5. 롤백

되돌릴 대상이 없다 — 빈 프로젝트를 세우는 단계다.

---

## 지라 티켓

티켓에는 라벨을 붙이지 않는다 — 보드에서 방해만 된다. 계획서 ID 와의 대응은 [91-jira-map.md](91-jira-map.md) 9절에 있다.

**에픽** [`CY-18`](https://shseol.atlassian.net/browse/CY-18) 검증 기반 구축

| 계획서 | 티켓 | 이름 |
|---|---|---|
| `1.3` | [`CY-34`](https://shseol.atlassian.net/browse/CY-34) | **지속 통합 파이프라인** |
| `1.3.1` | [`CY-105`](https://shseol.atlassian.net/browse/CY-105) | 빌드·테스트·보안·부하·리포트를 재사용 워크플로로 쪼갠다 |
| `1.3.2` | [`CY-106`](https://shseol.atlassian.net/browse/CY-106) | PR 과 main 과 nightly 가 각기 다른 잡을 돈다 |
| `1.3.3` | [`CY-107`](https://shseol.atlassian.net/browse/CY-107) | 커밋 규약·문서 링크·훅 자기검증을 CI 가 검사한다 |
| `1.3.4` | [`CY-108`](https://shseol.atlassian.net/browse/CY-108) | 취약점 스캔과 시크릿 유출 검사 |
| `1.3.5` | [`CY-109`](https://shseol.atlassian.net/browse/CY-109) | 코드 리뷰 결과를 슬랙과 지라로 중계한다 |
| `1.3.6` | [`CY-110`](https://shseol.atlassian.net/browse/CY-110) | 지라 상태 전이와 컨플루언스 리포트 연동 |
| `1.3.7` | [`CY-111`](https://shseol.atlassian.net/browse/CY-111) | 외부 액션을 커밋 해시로 고정한다 |
| `1.3.8` | [`CY-112`](https://shseol.atlassian.net/browse/CY-112) | 브랜치 보호 규칙과 스쿼시 병합 비활성화 |
| 게이트 | [`CY-39`](https://shseol.atlassian.net/browse/CY-39) | 검증 기반 구축 종료 게이트 |

**이 페이즈만 에픽 단위로 딴다** (WF-3 예외) — `feature/CY-18-foundation`.
태스크가 전부 그래들 위에 얹혀 따로 PR 을 열 수 없고, 판정 단위도 페이즈다.
`1.2`·`1.4`·`1.5` 는 티켓이 없는데 에픽 브랜치면 `CY-18` 하나로 해결된다.

커밋 푸터는 지라 키 하나만 남긴다 — 계획서 ID 를 박지 않는다.

```
Refs: CY-42
```

# CI

워크플로를 최소 단위로 쪼개고 서로 참조해서 쓴다. 하나의 큰 파일에 다 넣으면
어느 단계가 왜 실패했는지 알기 어렵고 재사용도 안 된다.

---

## 구조

```
.github/
├── actions/                      스텝 단위로 재사용
│   ├── setup-gradle/             JDK 21 + Gradle 캐시. 프로젝트 유무 감지
│   ├── slack-notify/             Block Kit 메시지
│   └── verdict/                  잡 결과 목록 → 통과·실패 판정
│
├── workflows/
│   ├── _build.yml                밑줄로 시작하면 잡 단위 재사용
│   ├── _test.yml                    테스트 계층 하나를 돈다
│   ├── _verify-conventions.yml      훅, 커밋 메시지, 작업 로그, 문서 링크
│   ├── _security.yml                Trivy, gitleaks(CLI), dependency-review
│   ├── _load-test.yml               k6 시나리오 하나
│   ├── _report.yml                  Slack + Jira + Confluence
│   │
│   ├── pr.yml                    진입점
│   ├── main.yml
│   ├── nightly.yml
│   ├── coderabbit-relay.yml         리뷰 결과를 Slack 과 Jira 로
│   └── coderabbit-control.yml       라벨로 리뷰 켜고 끄기
│
├── PULL_REQUEST_TEMPLATE.md
├── dependabot.yml
└── CODEOWNERS
```

composite action 은 잡 안의 스텝으로 들어간다. 같은 러너에서 돌아 체크아웃과 캐시를
공유한다. 재사용 워크플로는 잡 자체가 되어 별도 러너에서 병렬로 돌고 실패가 격리된다.

테스트를 계층별로 나눈 이유는 실패했을 때 잡 이름만 보고 어디가 깨졌는지 알기
위해서다. 한 잡에 다 넣으면 로그를 파야 안다.

Jira 와 Confluence 연동은 [`ci-actions`](https://github.com/coupon-yaho/cy-ci-actions) 저장소의
TypeScript 액션을 쓴다. 재시도와 형식 변환처럼 테스트가 필요한 로직은 셸로 감당하기
어렵다. Slack 은 아직 여기 composite 로 남아 있는데, 웹훅에 JSON 을 던지는 게 전부라
테스트할 게 없어서다.

외부 액션은 API 를 부르는 곳에서만 쓴다. 이슈 키를 문자열에서 뽑는 정도는 인라인
셸로 둔다. PR 의 첫 관문에서 외부 저장소를 참조하면 그 저장소가 없는 순간 모든 PR 이
막힌다.

---

## 언제 무엇이 도는가

| 워크플로 | 언제 |
|---|---|
| `pr.yml` | PR 열림, 갱신 |
| `main.yml` | main push |
| `nightly.yml` | 평일 03:00 KST, 수동 실행 |
| `coderabbit-relay.yml` | CodeRabbit 이 코멘트나 리뷰를 남겼을 때 |
| `coderabbit-control.yml` | `skip-review` 라벨을 붙이거나 뗐을 때 |

뒤의 두 개는 `issue_comment` 와 `pull_request` 이벤트로 도는데, 이런 이벤트는
기본 브랜치에 있는 워크플로 파일로 실행된다. 고쳐도 main 에 병합되기 전까지는
반영되지 않는다.

### PR 과 main 이 다른 점

| | PR | main | nightly |
|---|---|---|---|
| 규범 검사 | O | O | — |
| 커밋 메시지 규약 | O | — | — |
| 작업 로그 요구 | O | — | — |
| 빌드 | 코드가 바뀌면 | O (산출물 업로드) | O |
| 단위·통합·컨텍스트 | O | O | O |
| 카오스 | O | O | O (60분) |
| 뮤테이션 | — | O | O |
| CVE 스캔 범위 | CRITICAL, HIGH (코드가 바뀌면) | + MEDIUM | + LOW |
| CVE 차단 등급 | CRITICAL, HIGH | + MEDIUM | **CRITICAL, HIGH 만** |
| 시크릿 검사 | O | O | O |
| 부하 | smoke | 한산한 쿠폰 | 네 가지 전부 |
| Jira 전이 | 검토 중 | 완료 | — |
| Confluence 리포트 | — | O | O |

카오스를 PR 에서도 도는 이유는 회복력이 이 프로젝트의 핵심이라서다. 병합한 다음에
발견하면 늦다.

뮤테이션은 PR 에서 뺐다. 느린데다 도메인이 안 바뀌면 결과도 안 바뀐다. PR 마다
40분을 기다리게 하면 아무도 PR 을 안 연다.

main 병합 커밋은 커밋 규약 검사에서 제외한다. 병합 커밋 메시지는 git 이 만든다.

nightly 는 `fail-fast: false` 로 모든 계층을 끝까지 돌린다. `continue-on-error` 를
쓰지 않는 이유는 그러면 스텝이 실패해도 잡 결과가 성공이 되어, 리포트가 늘
"이상 없음" 으로 나오기 때문이다.

**nightly 는 스캔과 차단을 분리한다.** LOW 까지 훑어 Security 탭에 올리되 차단은
`CRITICAL, HIGH` 에서만 한다 (`block-severity`). LOW 하나에 야간이 빨간불이 되면
**상시 빨간 CI 가 되고, 그러면 아무도 안 본다** — 관측과 차단은 다른 결정이다.

### 통과·실패 판정

세 워크플로 모두 `verdict` 잡에서 한 번 판정하고 리포트는 그 값을 쓴다.

| 잡 결과 | 판정 |
|---|---|
| `success` | 통과 |
| `skipped` | 통과 — 문서 PR 에서 빌드를 건너뛰는 것은 정상이다 |
| `failure` · `cancelled` | 실패 |

**취소를 통과로 세면 안 된다.** `contains(needs.*.result, 'failure')` 만 보면
취소가 통과로 집계되어 Jira 가 다음 상태로 넘어간다.

PR 은 `cancel-in-progress` 라 새 커밋을 밀 때마다 이전 실행이 취소된다. 그때는
**리포트를 아예 보내지 않는다** (`if: !cancelled()`). 밀린 실행은 실패가 아니라
검사하지 않은 것이라, 실패로 알리면 Slack 이 노이즈가 된다.

---

## 부하 테스트

`_load-test.yml` 은 k6 시나리오 하나를 돌린다. 직접 트리거되지 않고 세 곳에서 호출한다.

| 호출하는 곳 | 시나리오 | 왜 |
|---|---|---|
| `pr.yml` | `smoke` | 빠른 확인. 무거운 걸 PR 마다 돌리면 아무도 PR 을 안 연다 |
| `main.yml` | `idle-coupon` | 한산한 쿠폰이 큐 없이 통과하는지 실측 |
| `nightly.yml` | `idle-coupon`, `open-spike`, `mixed`, `abandonment` | 전부 |

`workflow_dispatch` 로 nightly 를 수동 실행할 때 `scenario` 를 지정하면 그 하나만 돈다.

### 무엇을 보고 도는가

시작하기 전에 두 파일이 있는지 본다.

| 파일 | 역할 |
|---|---|
| `test/load/<scenario>.js` | k6 시나리오 |
| `test/load/compose.yml` | 게이트웨이 + Redis + 백엔드 스텁을 띄운다 |

**둘 중 하나라도 없으면 건너뛴다.** 지금은 둘 다 없어서 부하 테스트가 한 번도
실행되지 않는다. 의도한 상태다. 코드가 없는데 실패시키면 CI 가 늘 빨간 상태가 되고,
그러면 아무도 CI 를 안 본다.

돌 때의 순서는 이렇다.

```
compose up --wait     게이트웨이 + Redis + 백엔드 스텁
k6 run <scenario>.js  게이트웨이를 때린다
evaluate-gate.sh      k6 결과를 페이즈 게이트 기준과 대조 (test/load/)
compose logs          실패했을 때 원인을 찾을 수 있게
```

판정은 k6 의 threshold 가 아니라 `test/load/evaluate-gate.sh` 가 한다. k6 threshold 는
"p95 가 500ms 미만" 같은 것만 볼 수 있는데, 계획서의 게이트 기준은 "한산한 쿠폰에
큐 등록 Redis 명령이 0건" 처럼 다른 차원이라서다. 이 스크립트도 아직 없고, 없으면
k6 종료 코드로 대신 판정한다.

### compose.yml 이 갖춰야 할 것

아직 만들지 않았지만 워크플로가 기대하는 계약은 정해져 있다.

- `--wait` 가 통과하도록 **모든 서비스에 healthcheck** 가 있어야 한다.
  없으면 게이트웨이가 뜨기 전에 k6 가 때려서 결과가 무의미해진다
- 백엔드 스텁은 **인스턴스별 지연과 동시 처리 한도를 설정으로** 받아야 한다.
  200/40/120 같은 가용량 차등을 만들 수 없으면 라우팅을 검증할 수 없다
- 게이트웨이가 `localhost` 의 고정 포트로 노출되어야 한다. k6 시나리오가 그 주소를 쓴다

---

## 시크릿

| 시크릿 | 없으면 |
|---|---|
| `SLACK_WEBHOOK_URL` | 알림만 안 감 |
| `ATLASSIAN_BASE_URL` `ATLASSIAN_USER_EMAIL` `ATLASSIAN_API_TOKEN` | Jira·Confluence 연동 안 됨 |
| `CONFLUENCE_SPACE_KEY` `CONFLUENCE_PARENT_ID` | Confluence 만 |

없어도 CI 는 안 깨진다. **다만 방식이 둘로 나뉜다.**

| | 없을 때 |
|---|---|
| `SLACK_WEBHOOK_URL` | 스텝이 돌면서 **경고만** 남기고 넘어간다 |
| Atlassian·Confluence 계열 | **스텝을 아예 건너뛴다** (`_report.yml` 의 `HAS_*` 가드) |

Confluence 액션은 **필수 입력이 비면 `fail-on-error` 와 무관하게 하드 실패**한다
— 계약 위반은 경고로 낮추지 않는 설계다. 그래서 빈 값으로 부르지 않고 건너뛴다.
`steps.*.if` 는 `secrets` 를 못 읽으므로 잡 레벨 `env` 로 존재 여부만 옮겨 본다.
값의 형식과 발급 방법, 계정에 필요한 권한은
[`ci-actions`](https://github.com/coupon-yaho/cy-ci-actions) 에 있다.

---

## CodeRabbit

GitHub App 이라 시크릿이 필요 없다. 저장소에 앱을 설치하면 루트의
[`.coderabbit.yaml`](../.coderabbit.yaml) 을 읽는다.

리뷰를 끄는 방법이 세 가지 있다. 리뷰가 필요 없는 곳에 리뷰가 붙으면 코드에 대한
진짜 지적이 그 안에 묻히기 때문이다.

| 방법 | 언제 쓰나 |
|---|---|
| `skip-review` 라벨 | PR 도중에도 켜고 끌 수 있다. 라벨을 떼면 재개된다 |
| 제목 키워드 | `[skip-review]`, `[wip]`, `docs:`, `chore(deps)` |
| 경로 필터 | `*.md`, 생성물, `waiting-legacy/` 는 아예 안 본다 |

문서를 뺀 이유는 CodeRabbit 이 한글 산문에 내놓는 게 오탈자와 문체 지적이지 설계
비평이 아니어서다. 계획 문서의 설계 검토는 사람과 `.claude/agents/` 의 몫이다.

라벨을 붙이면 `coderabbit-control.yml` 이 `@coderabbitai pause` 를 코멘트한다.
CodeRabbit 설정에는 "이 라벨이면 건너뛴다" 가 없어서 (`auto_review.labels` 는
허용 목록이라 방향이 반대다) 워크플로가 채팅 명령을 대신 친다. 라벨이 붙은 PR 은
Slack 중계도 하지 않는다.

라벨은 미리 만들어 둔다.

```bash
gh label create skip-review --color BFD4F2 --description "CodeRabbit 자동 리뷰 건너뛰기"
```

---

## Jira 와 Confluence

브랜치명에서 이슈 키를 뽑는다.

```
feature/CY-231-idle-cap-from-global-credit
        └──┬─┘
      여기를 읽는다
```

**봇이 연 PR 은 사람용 규칙 셋만 면제한다** — 브랜치명, 커밋 제목, 작업 로그.
봇이 만들 수 없는 형식이라 요구하면 갱신마다 빨간 체크가 쌓이고, **늘 빨간 것은
늘 초록인 것과 똑같이 아무 신호도 안 준다.**

| 봇 PR 에서 | |
|---|---|
| 브랜치명·커밋 제목·작업 로그 | **면제** |
| 빌드·테스트·시크릿·CVE·의존성 검토 | **그대로 돈다** |

**통째로 건너뛰지 않는다.** 의존성을 바꾸는 PR 이야말로 의존성 검토와 취약점
스캔이 가장 필요한 자리다. 그리고 필수 체크 중 재사용 워크플로 안에 있는 것은
건너뛰면 그 이름으로 보고를 안 해서, 빨강이 영구 대기로 바뀔 뿐 병합은 여전히
막힌다.

가르는 기준은 `user.login` 의 `[bot]` 접미사다 — 작업 로그 면제가 이미 쓰던 것과
같다. 기준이 둘이면 언젠가 갈라진다.

**봇도 `develop` 으로 연다.** 대상 브랜치를 안 정하면 기본 브랜치로 가는데,
사람은 전부 `develop` 으로 여는 마당에 봇만 릴리스 브랜치로 바로 들어간다.

> 워크플로는 **대상 브랜치에 있는 것**이 돈다. `develop` 에 넣은 면제가 `main` 을
> 겨냥한 봇 PR 에 안 닿아서, 고쳤는데 안 고쳐진 것처럼 보였다.

| 언제 | Jira |
|---|---|
| PR 열림 + CI 통과 | `검토 중` 으로 전이, PR 링크 코멘트 |
| PR CI 실패 | 코멘트만. 전이하지 않는다 |
| main 병합 성공 | `완료` 로 전이, 버전 코멘트 |
| CodeRabbit 지적 발생 | 코멘트. 승인일 때는 남기지 않는다 |

CI 가 실패했을 때 상태를 되돌리지 않는 이유는 보드가 앞뒤로 요동치면 보는 사람이
헷갈려서다. 전이 이름(`검토 중`, `완료`)은 **CY 프로젝트에서 실제 이름을 확인한 값**이다 — 프로젝트 로케일이 한국어라 영문 이름을 보내면 전이가 없다고 실패한다. 그 실패는 기본 설정에서 경고로 낮아져 **초록불인 채 아무 일도 안 한다.**
없으면 액션이 가능한 전이 목록을 에러에 담아 알려준다.

Confluence 는 제목으로 페이지를 찾는다. 같은 제목이면 새 페이지가 생기지 않고 버전만
올라간다. `main.yml` 은 `CI 리포트 — main <version>`, `nightly.yml` 은
`야간 검증 리포트` 라는 제목을 쓴다.

본문은 마크다운으로 주면 액션이 XHTML 로 바꾸고, `<h2>` 같은 태그로 시작하면 storage
형식으로 그대로 보낸다. **워크플로는 후자를 쓴다.** 그래서 커밋 메시지처럼 밖에서 온
값을 본문에 넣을 때는 이스케이프해야 한다 — `<` 하나에 문서가 깨지고, 그 실패는
경고로 낮아져 조용히 사라진다.

---

## 푸시 전에 할 일

~~`ci-actions` 푸시와 `v1` 태그~~ — 완료. 워크플로가
`coupon-yaho/cy-ci-actions/actions/*@v1` 을 참조한다.

**서드파티 액션을 SHA 로 고정한다.** 태그는 옮겨질 수 있다. dependabot 이 이후
갱신을 맡도록 설정해뒀지만 처음 고정은 손으로 해야 한다. dependabot 은 이미 SHA 인
것을 올려줄 뿐 태그를 SHA 로 바꿔주지 않는다.

**브랜치 보호 규칙을 건다.** `main` 과 `develop` 에 필수 체크를 지정하고 squash
merge 를 끈다. 설정으로 막지 않으면 언젠가 눌리고, 그러면 TDD 사이클 커밋이 사라져
규약 전체가 무의미해진다. GitHub 설정이라 코드로 남길 수 없다.

**CODEOWNERS 를 활성화한다.** 지금은 전부 주석 상태다. GitHub 은 존재하지 않는
팀이 하나라도 있으면 파일 전체를 무시하는데, 에러도 경고도 없이 리뷰 강제만
사라진다. 그래서 추측한 값으로 채우지 않고 비워뒀다. 팀 슬러그를 확정하고 주석을
푼다 (`cy-be` 는 저장소 이름이지 팀이 아니다).

`skip-review` 라벨 만들기와 Confluence 상위 페이지 만들기도 남아 있다.

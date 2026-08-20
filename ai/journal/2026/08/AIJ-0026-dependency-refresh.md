---
id: AIJ-0026
date: 2026-08-20
kind: decision
phase: 3
plan: []
jira: CY-265
commits: [7a5736a]
agent: claude-opus-5
confidence: high
promoted-to:
---

# 버전은 실측한다 — 검색 색인은 최신을 모른다

## 무엇을 했나

손으로 박은 버전 넷을 올렸다.

| 아티팩트 | 전 | 후 |
|---|---|---|
| jacoco | 0.8.13 | 0.8.15 |
| archunit | 1.3.2 | 1.5.0 |
| testcontainers-bom | 1.21.3 | 2.0.5 |
| pitest-junit5-plugin | 1.2.2 | 1.2.3 |

Spring Boot 4.1.0 · Spring Cloud 2025.1.2 · pitest 플러그인 1.15.0 은 이미 최신이라
그대로 뒀다.

## 검색 색인을 믿으면 안 된다

`search.maven.org/solrsearch` 는 jacoco 최신을 **0.8.13** 이라고 답했다. 그런데
`maven-metadata.xml` 에는 0.8.14·0.8.15 가 있다. 색인이 뒤처진 것이다.

**`maven-metadata.xml` 이 정답이다.** 저장소가 직접 들고 있는 목록이라 색인
갱신 주기와 무관하다.

```
https://repo1.maven.org/maven2/<group-path>/<artifact>/maven-metadata.xml
```

## BOM 이 먼저다

버전을 손으로 박는 것은 **BOM 이 없을 때만** 한다. 이 저장소는 Spring Cloud 와
Testcontainers 를 이미 `mavenBom` 으로 받는데, 그래서 Testcontainers 를 2.0.5 로
올릴 때 **한 줄만 고쳐도 63개 아티팩트가 정합적으로 따라왔다.**

## Testcontainers 2.x 는 아티팩트 이름이 바뀌었다

`org.testcontainers:junit-jupiter` → `org.testcontainers:testcontainers-junit-jupiter`.
옛 이름으로 두면 해석 단계에서 죽는다 — 조용히 넘어가지 않아서 다행이다.
build.gradle 에 이유를 적어 뒀다. 안 적으면 다음 사람이 "오타" 로 보고 되돌린다.

## 왜 별도 티켓으로 뺐나

이 실측은 Phase 3 작업(CY-230) 중에 나왔다. 그 PR 에 얹으면 **범위가 흐려져**
리뷰가 "카오스 시험을 보는 일" 과 "의존성을 보는 일" 로 갈린다. 메이저 승격이
섞이면 더 그렇다 — 되돌릴 때도 같이 되돌아간다.

## 검증

- 단위·통합·카오스 전 계층 통과 (clean 후)
- 뮤테이션 커버리지 미달 0 · 강도 97%

## 다음 사람에게

**버전을 올릴 땐 어디서 읽었는지도 같이 남겨라.** "최신으로 올렸다" 는 6개월
뒤에 아무 의미가 없고, 어느 출처를 봤는지가 다음 사람이 같은 실수를 안 하게
한다. 이번에는 검색 API 를 믿었다면 0.8.13 이 최신인 줄 알고 넘어갔을 것이다.

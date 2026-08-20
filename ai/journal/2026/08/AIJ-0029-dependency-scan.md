---
id: AIJ-0029
date: 2026-08-20
kind: decision
phase: 3
plan: []
jira: CY-293
commits: [10eed66, 031d1f0, 82bb16c]
agent: claude-opus-5
confidence: high
promoted-to:
---

# 초록불이 근거 없는 초록불이었다

## 무엇을 발견했나

취약점 스캔이 **Java 의존성을 하나도 안 보고 있었다.** CI 로그다.

```
보안 / CVE (Trivy)
  INFO  Number of language-specific files  num=0
  ::notice 차단 대상(CRITICAL,HIGH) 0건
```

Trivy 는 Gradle 을 **락파일로만** 읽는다. `build.gradle` 은 파싱 대상이 아니다.
이 저장소에 락파일이 없으니 볼 것이 없었고, 볼 것이 없으니 **0건이 나왔다.**

같은 이유로 의존성 그래프에도 Java 패키지가 0개였다 — `dependency-review` 도
Dependabot 알림도 함께 무의미했다.

## 켜자마자 HIGH 6건이 나왔다

| 패키지 | 현재 | CVE |
|---|---|---|
| netty-codec-http | 4.2.15 | CVE-2026-55831 · 55833 · 56745 |
| netty-codec-http2 | 4.2.15 | CVE-2026-56819 |
| netty-codec-http3 | 4.2.15 | CVE-2026-56816 |
| netty-codec-compression | 4.2.15 | CVE-2026-59901 |

전부 HTTP 코덱 계열이다. **WebFlux 게이트웨이는 그 코덱 위에서 돈다** —
"쓰지 않는 모듈이라 도달 불가" 로 넘길 수 있는 자리가 아니다.

`netty.version` 을 4.2.17.Final 로 덮어 0건이 됐다.

## 세 가지 판단

**락파일은 스캔 직전에 만들고 커밋하지 않는다.**
잠금이 목적이 아니라 Trivy 가 읽을 것을 주는 게 목적이다. 커밋하면 의존성을
바꿀 때마다 갱신 부담이 따라오고, 그 부담은 **스캔과 아무 상관이 없다.**

**잠금은 init 스크립트로 그 호출에만 켠다.**
`build.gradle` 에 `lockAllConfigurations()` 를 넣으면 락파일이 한 번 생긴 뒤
모든 빌드가 그것과 대조하게 된다. **취약점 스캔 사정이 일반 빌드에 얹히면
안 된다** — 개발자가 의존성 하나 바꿀 때마다 잠금 갱신을 강요당한다.

**판은 BOM 속성을 덮는다.**
`set('netty.version', ...)` 이지 `implementation 'io.netty:netty-codec-http:...'`
가 아니다. 후자는 BOM 이 관리하는 나머지 netty 모듈과 판이 갈린다 — 26개
모듈 중 하나만 올라간다.

## 왜 오래 살아남았나

**검사가 있었고, 초록이었고, 아무도 그 초록의 근거를 안 물었다.**
`plan/01-foundation.md` 의 "취약점 스캔과 시크릿 유출 검사" 항목은 시크릿
절반만 실제로 동작하고 있었다. gitleaks 는 잘 돌았고, 그래서 잡 전체가
초록이었다.

로그에 `num=0` 이 매번 찍히고 있었다. 읽는 사람이 없었을 뿐이다.

## 확신이 낮은 부분

- **`--write-locks` 가 실패해도 스캔은 계속 돈다** (`|| true`). 실패하면
  경고만 찍고 다시 Java 를 못 보게 된다 — 경고를 아무도 안 읽으면 같은 일이
  반복된다. 락파일 부재를 실패로 볼지 정해야 한다.
- **`dependency-resolution-task: dependencies`** 가 그래프에 무엇까지 담는지
  실측 안 했다. `main` 에 병합된 뒤 그래프에 Java 패키지가 실제로 잡히는지
  확인해야 한다.

## 검증

- 락파일 전 `num=0` → 후 `gradle.lockfile → gradle → 6건`
- 4.2.17 로 올린 뒤 CRITICAL·HIGH **0건**
- 단위·통합·카오스 전 계층 통과

## 다음 사람에게

**게이트가 초록이면 무엇을 보고 초록인지 한 번은 확인해라.** 이 검사는
"취약점이 없다" 가 아니라 "볼 것이 없다" 를 말하고 있었고, 둘은 로그
한 줄로만 구분된다. 검사를 새로 붙일 때는 **일부러 걸리는 것을 하나 넣어**
빨간불이 뜨는지 보는 편이 싸다.

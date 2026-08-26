---
id: AIJ-0058
date: 2026-08-24
kind: fix
phase: 1
plan: [01-foundation]
jira: CY-440
commits: []
agent: claude-opus-5
confidence: high
promoted-to:
---

# 시험이 하나도 안 뜨는데 이름이 안 남는다

## 증상

뮤테이션 플러그인을 올리자 테스트가 통째로 안 떴다.

```
TestSuiteExecutionException: Could not complete execution
  Caused by: NoSuchMethodError: CollectionUtils.toUnmodifiableList()
```

**어느 시험이 깨졌는지가 안 나온다.** 실패가 아니라 실행기 오류라 결과 파일에
시험 이름이 없다. CI 로그도 "테스트 실패" 한 줄뿐이었다.

## 어떻게 찾았나

로그만 보고 재실행을 반복했다. 그걸로는 안 나온다.

합쳐서 로컬에서 돌리고, 의존성 그래프를 직접 읽고서야 보였다.

```
junit-platform-commons  6.0.3
junit-platform-engine   6.0.3
junit-platform-launcher 1.14.4   ← 여기만 뒤처진다
```

## 원인

플러그인이 런처를 자기가 아는 판으로 끌어내린다. 나머지 플랫폼 모듈은 부트 BOM 이
관리하는 최신이라, **런처만 옛 판이고 공통 모듈은 새 판**이 된다. 실행기가
없는 메서드를 부른다.

## 판을 손으로 안 적는다

부트가 관리하는 판을 그대로 쓰게 했다. 숫자를 박아 두면 BOM 이 올릴 때 여기만
뒤처져 **같은 어긋남이 반대 방향으로** 다시 난다.

## 두 갱신이 서로를 필요로 했다

빌드 도구를 올리면 옛 뮤테이션 플러그인이 사라진 API 를 불러 아예 못 뜬다.

```
Failed to apply plugin: unknown property 'baseDir' for extension 'reporting'
```

플러그인을 올리면 이 런처 문제가 난다. 그래서 순서가 정해진다 —
**이 수정 → 플러그인 갱신 → 빌드 도구 갱신.** 가운데를 빼면 깨진다.

## 어떻게 검증했는가

| 확인한 것 | 결과 |
|---|---|
| 런처 판 | 강제 전 1.14.4, 강제 후 6.0.3 |
| 플러그인만 올림 | 고치기 전 실행기 오류, 고친 뒤 전체 게이트 통과 |
| 빌드 도구만 올림 | 플러그인 적용 실패 — 순서가 있다는 근거 |
| 둘 다 올림 + 이 수정 | 전체 게이트 통과 |

## 남은 것

**갈림을 막는 장치가 없다.** 다음에 다른 모듈이 같은 방식으로 끌려 내려가면
똑같이 시험 이름 없는 오류를 본다. 플랫폼 모듈의 판이 한 줄로 정렬되는지
보는 검사가 있으면 좋겠지만, 지금은 없다.

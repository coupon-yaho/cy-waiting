#!/usr/bin/env bash
# `gradlew` 를 부르는 잡이 JDK 와 Gradle 캐시를 준비했는지 본다.
#
# **준비 없이도 돈다는 것이 함정이다.** 러너에 JDK 가 깔려 있어 빌드는 초록으로
# 끝난다. 그래서 빠뜨려도 아무도 모른다 — 대신 (가) 우리가 고른 JDK 가 아니라
# 러너 이미지의 기본값으로 돌고, (나) Gradle 배포판과 의존성을 매번 처음부터
# 받는다. 앞엣것은 저장소를 안 건드려도 이미지가 바뀌면 깨지고, 뒤엣것은 그
# 내려받기 하나에 잡 전체가 걸린다.
#
# 실제로 보안 잡이 그 상태로 오래 돌았다 (CY-978).
set -uo pipefail

root=$(git rev-parse --show-toplevel 2>/dev/null) || root=.
python3 - "$root" <<'PY'
import pathlib
import re
import sys

try:
    import yaml
except ImportError:
    print("  PyYAML 이 없어 검사를 못 한다", file=sys.stderr)
    sys.exit(1)

root = pathlib.Path(sys.argv[1])
workflows = root / '.github' / 'workflows'
if not workflows.is_dir():
    sys.exit(0)

files = sorted(workflows.glob('*.yml')) + sorted(workflows.glob('*.yaml'))
if not files:
    print("  워크플로 디렉터리가 비었다", file=sys.stderr)
    sys.exit(1)

# `chmod +x ./gradlew` 는 실행이 아니다. 준비 액션 자신이 그것을 한다.
CALLS = re.compile(r'(^|[\s;&|(])(\./gradlew|[\w./-]*gradle-retry\.sh)\b')
SETUP = ('actions/setup-gradle', 'gradle/actions/setup-gradle')

bad = 0
for path in files:
    try:
        doc = yaml.safe_load(path.read_text(encoding='utf-8'))
    except yaml.YAMLError as e:
        print(f"  {path.name}: YAML 을 못 읽는다 — {str(e).splitlines()[0]}", file=sys.stderr)
        bad += 1
        continue
    for job, spec in (doc.get('jobs') or {}).items():
        steps = (spec or {}).get('steps') or []
        calls = False
        for step in steps:
            run = step.get('run') or ''
            for line in run.splitlines():
                stripped = line.strip()
                if stripped.startswith('chmod'):
                    continue
                if CALLS.search(stripped):
                    calls = True
                    break
            if calls:
                break
        if not calls:
            continue
        prepared = any(any(s in (step.get('uses') or '') for s in SETUP)
                       for step in steps)
        if not prepared:
            print(f"  {path.name}: 잡 '{job}' 이 gradlew 를 부르는데 "
                  f"setup-gradle 이 없다", file=sys.stderr)
            bad += 1

sys.exit(1 if bad else 0)
PY

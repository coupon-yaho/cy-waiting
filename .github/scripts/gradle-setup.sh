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
# **없는 것과 사라진 것을 가른다.** 디렉터리 자체가 없으면 검사 대상이 아니다 —
# 훅 자기검증의 픽스처 저장소가 그렇다. 있는데 비었으면 검사가 사라진 것이라
# 막는다. `workflow-shell.sh` 가 같은 이유로 같은 갈래를 쓴다.
if not workflows.is_dir():
    sys.exit(0)

files = sorted(workflows.glob('*.yml')) + sorted(workflows.glob('*.yaml'))
if not files:
    print("  워크플로 디렉터리가 비었다", file=sys.stderr)
    sys.exit(1)

# 호출의 모양은 여럿이다. `./gradlew` 만 찾으면 `bash gradlew` 와 절대경로가
# 통째로 빠진다 — 게이트가 통과 방향으로 틀리는 자리다.
CALLS = re.compile(r'(^|[\s;&|(=])([\w./${}-]*/)?gradlew\b'
                   r'|(^|[\s;&|(])[\w./${}-]*gradle-retry\.sh\b')
SETUP = ('/setup-gradle', 'gradle/actions/setup-gradle')
# `chmod +x ./gradlew` 는 실행이 아니다. 준비 액션 자신이 그것을 한다.
# **줄째로 건너뛰지 않는다** — `chmod +x ./gradlew && ./gradlew build` 가
# 통과 방향으로 빠진다.
CHMOD = re.compile(r'\bchmod\b[^;&|]*')


def calls_gradle(run):
    for line in run.splitlines():
        stripped = CHMOD.sub(' ', line.strip())
        if CALLS.search(stripped):
            return True
    return False


def prepares(step):
    uses = step.get('uses') or ''
    # `if:` 가 달린 준비는 안 돌 수 있다. 조건 없는 것만 준비로 센다.
    if step.get('if') is not None:
        return False
    return any(uses.endswith(s) or uses.split('@')[0].endswith(s) for s in SETUP)


bad = 0
for path in files:
    try:
        doc = yaml.safe_load(path.read_text(encoding='utf-8'))
    except yaml.YAMLError as e:
        print(f"  {path.name}: YAML 을 못 읽는다 — {str(e).splitlines()[0]}", file=sys.stderr)
        bad += 1
        continue
    if not isinstance(doc, dict):
        print(f"  {path.name}: 워크플로가 매핑이 아니다", file=sys.stderr)
        bad += 1
        continue
    jobs = doc.get('jobs')
    if jobs is not None and not isinstance(jobs, dict):
        print(f"  {path.name}: jobs 가 매핑이 아니다", file=sys.stderr)
        bad += 1
        continue
    for job, spec in (jobs or {}).items():
        steps = (spec or {}).get('steps') or []
        # **준비가 호출보다 앞에 있어야 한다.** 뒤에 있으면 그 잡은 준비 없이
        # 돈 것이고, 게이트가 막으려던 모양 그대로다.
        prepared = False
        for step in steps:
            if prepares(step):
                prepared = True
                continue
            if calls_gradle(step.get('run') or ''):
                if not prepared:
                    print(f"  {path.name}: 잡 '{job}' 이 준비 없이 gradlew 를 부른다",
                          file=sys.stderr)
                    bad += 1
                break

sys.exit(1 if bad else 0)
PY

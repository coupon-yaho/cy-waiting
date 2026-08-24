#!/usr/bin/env bash
# 워크플로의 `run:` 블록이 셸 문법에 맞는지 본다.
#
# **YAML 이 맞는 것과 셸이 도는 것은 다르다.** 따옴표 하나가 안 닫혀도 YAML 은
# 멀쩡하고, 그 스크립트는 러너에서 처음 실행될 때 죽는다. 그때는 검사 자체가
# 안 돈 것인데 로그만 보면 검사가 실패한 것처럼 읽힌다 — 실제로 그렇게 한 번
# 속았다.
#
# 워크플로 식(`${{ ... }}`)은 셸 문법이 아니므로 자리만 채워 검사한다.
set -uo pipefail

root=$(git rev-parse --show-toplevel 2>/dev/null) || root=.
python3 - "$root" <<'PY'
import os, re, subprocess, sys, tempfile, pathlib

try:
    import yaml
except ImportError:
    print("  PyYAML 이 없어 검사를 못 한다", file=sys.stderr)
    sys.exit(1)

root = pathlib.Path(sys.argv[1])
bad = 0

# **없는 것과 사라진 것을 가른다.** 워크플로 디렉터리 자체가 없으면 검사 대상이
# 아니다 — 픽스처 저장소가 그렇다. 있는데 비었으면 검사가 사라진 것이라 막는다.
workflows = root / '.github' / 'workflows'
if not workflows.is_dir():
    sys.exit(0)

files = sorted(workflows.glob('*.yml')) + sorted(workflows.glob('*.yaml'))
if not files:
    print("  워크플로 디렉터리가 비었다", file=sys.stderr)
    sys.exit(1)

for path in files:
    try:
        doc = yaml.safe_load(path.read_text(encoding='utf-8'))
    except yaml.YAMLError as e:
        print(f"  {path.name}: YAML 을 못 읽는다 — {str(e).splitlines()[0]}", file=sys.stderr)
        bad += 1
        continue
    # 셸은 스텝 → 잡 → 워크플로 순으로 정해진다. 기본값은 러너의 bash 다.
    top = ((doc.get('defaults') or {}).get('run') or {}).get('shell')
    for job, spec in (doc.get('jobs') or {}).items():
        job_shell = ((spec.get('defaults') or {}).get('run') or {}).get('shell') or top
        for i, step in enumerate(spec.get('steps') or []):
            run = step.get('run')
            if not run:
                continue
            shell = step.get('shell') or job_shell or 'bash'
            # **bash 가 아닌 것을 bash 로 읽지 않는다.** 파이썬 스크립트를
            # `bash -n` 에 넣으면 멀쩡한 코드가 문법 오류로 나온다.
            if shell.split()[0] not in ('bash', 'sh'):
                continue
            with tempfile.NamedTemporaryFile('w', suffix='.sh', delete=False) as f:
                f.write(re.sub(r'\$\{\{[^}]*\}\}', 'X', run))
                tmp = f.name
            r = subprocess.run(['bash', '-n', tmp], capture_output=True, text=True)
            os.unlink(tmp)
            if r.returncode:
                name = step.get('name', f'#{i}')
                first = (r.stderr.strip().splitlines() or [''])[0]
                print(f"  {path.name} · {job} · {name}: {first}", file=sys.stderr)
                bad += 1

sys.exit(1 if bad else 0)
PY

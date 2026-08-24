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
files = sorted(root.glob('.github/workflows/*.yml')) + sorted(root.glob('.github/workflows/*.yaml'))
if not files:
    print("  워크플로가 없다", file=sys.stderr)
    sys.exit(1)

for path in files:
    try:
        doc = yaml.safe_load(path.read_text(encoding='utf-8'))
    except yaml.YAMLError as e:
        print(f"  {path.name}: YAML 을 못 읽는다 — {str(e).splitlines()[0]}", file=sys.stderr)
        bad += 1
        continue
    for job, spec in (doc.get('jobs') or {}).items():
        for i, step in enumerate(spec.get('steps') or []):
            run = step.get('run')
            if not run:
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

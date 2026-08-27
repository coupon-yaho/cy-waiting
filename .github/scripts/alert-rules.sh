#!/usr/bin/env bash
# 알람 규칙이 실제로 도는 규칙인지 본다 (6.9.5).
#
# **문법 오류는 조용하다.** 프로메테우스는 못 읽는 규칙 파일을 건너뛰고 뜨므로,
# 알람이 하나도 안 걸린 채로 초록이다. 그 사실은 장애가 나서 아무도 안 불릴 때
# 드러난다.
#
# 그리고 **우리 지표를 보는지도 본다.** 이름이 바뀌면 규칙은 문법상 멀쩡한 채로
# 영영 안 울린다 — 대시보드와 같은 실패다 (LG-11).
set -uo pipefail

root=$(git rev-parse --show-toplevel 2>/dev/null) || root=.
cd "$root" || exit 1

readonly DIR=docker/observability/rules
readonly ALLOWED=docker/observability/dashboards/allowed-metrics.txt

[[ -d "$DIR" ]] || exit 0

fail=0

# promtool 이 있으면 그것이 본다. 없으면 최소한 파싱과 형태를 본다 —
# 없다고 건너뛰면 CI 마다 검사가 있다 없다 한다.
if command -v promtool >/dev/null 2>&1; then
    promtool check rules "$DIR"/*.yml || fail=1
else
    echo "::notice::promtool 이 없어 자체 검사로 대신한다"
fi

python3 - <<'PY' || fail=1
import pathlib, re, sys

try:
    import yaml
except ImportError:
    print("::error::PyYAML 이 없어 알람 규칙을 못 읽는다", file=sys.stderr)
    sys.exit(1)

DIR = pathlib.Path('docker/observability/rules')
ALLOWED = pathlib.Path('docker/observability/dashboards/allowed-metrics.txt')

known = set()
if ALLOWED.exists():
    known = {line.strip() for line in ALLOWED.read_text().splitlines()
             if line.strip() and not line.startswith('#')}

KEYWORDS = {
    'by', 'without', 'on', 'ignoring', 'group_left', 'group_right', 'offset',
    'and', 'or', 'unless', 'bool', 'rate', 'irate', 'increase', 'sum', 'avg',
    'min', 'max', 'count', 'topk', 'bottomk', 'quantile', 'histogram_quantile',
    'abs', 'ceil', 'floor', 'round', 'delta', 'deriv', 'time', 'absent',
    'sum_over_time', 'avg_over_time', 'max_over_time', 'min_over_time', 'vector',
}

bad = 0
rules = 0
for path in sorted(DIR.glob('*.yml')):
    doc = yaml.safe_load(path.read_text())
    for group in doc.get('groups', []):
        for rule in group.get('rules', []):
            rules += 1
            name = rule.get('alert') or rule.get('record')
            # **사람이 볼 문장이 있어야 한다.** 없으면 울려도 무엇을 하라는지
            # 아무 데도 안 적혀 있다.
            if rule.get('alert') and not rule.get('annotations', {}).get('summary'):
                print("::error file=%s::%s 에 summary 가 없다" % (path, name))
                bad = 1
            expr = re.sub(r'\{[^}]*\}', ' ', rule.get('expr', ''))
            expr = re.sub(r'\b(?:by|without|on|ignoring|group_left|group_right)'
                          r'\s*\([^)]*\)', ' ', expr)
            for match in re.finditer(r'\b[a-z_][a-z0-9_]*\b', expr):
                metric = match.group()
                if expr[match.end():match.end() + 1] == '(' or metric in KEYWORDS:
                    continue
                if metric.startswith('waiting_'):
                    dotted = metric.removesuffix('_total').replace('_', '.')
                    if not any(('"%s"' % dotted) in p.read_text()
                               for p in pathlib.Path('src/main/java').rglob('*.java')):
                        print("::error file=%s::%s 가 없는 지표를 본다: %s"
                              % (path, name, metric))
                        bad = 1
                elif metric not in known:
                    print("::error file=%s::%s 가 허용 목록에 없는 지표를 본다: %s"
                          % (path, name, metric))
                    bad = 1

if rules == 0:
    print("::error::알람 규칙을 하나도 못 찾았다 — 검사가 헛돈다")
    bad = 1
sys.exit(bad)
PY

exit $fail

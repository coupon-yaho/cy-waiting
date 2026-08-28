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

# **라벨 값도 코드에 있어야 한다.** 이름만 보면 outcome 을 리네임했을 때 그 항이
# 분자에서 조용히 빠지고, SLO 는 좋아지고 검사는 초록이다.
LABELS = set()
for src in pathlib.Path('src/main/java').rglob('*.java'):
    text = src.read_text()
    # 라벨 값으로 쓸 만한 문자열 리터럴을 통째로 모은다. 이름만 좁게 모으면
    # 새 지표가 생길 때마다 검사가 그 지표를 모른다.
    LABELS |= set(re.findall(r'"([a-z][a-z0-9-]*)"', text))
    LABELS |= set(re.findall(r'^\s+([A-Z][A-Z_]+)[,;(]', text, re.M))

# **기록 규칙이 만든 이름도 지표다.** 모르면 그것을 쓰는 알람이 "없는 지표를
# 본다" 로 걸리고, 그러면 소진율처럼 식이 긴 것을 한 곳에서 못 만든다.
recorded = set()
for path in sorted(DIR.glob('*.yml')):
    doc = yaml.safe_load(path.read_text())
    for group in doc.get('groups', []):
        for rule in group.get('rules', []):
            if rule.get('record'):
                # **콜론을 요구한다.** 없으면 지표 이름과 구분이 안 되고, 오타로
                # 만든 기록 규칙이 그 오타를 그대로 "있는 지표" 로 만든다.
                if ':' not in rule['record']:
                    print("::error file=%s::기록 규칙 이름에 콜론이 없다: %s"
                          % (path, rule['record']))
                    bad = 1
                recorded.add(rule['record'])

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
            raw = rule.get('expr', '')
            # `=` 와 `=~` 를 다 본다. 공백도 허용한다 — 한쪽만 보면 그쪽이
            # 아닌 표기로 쓴 오타가 조용히 빈 시계열이 된다.
            for label, op, group in re.findall(
                    r'(outcome|quality)\s*(=~|=)\s*"([^"]+)"', raw):
                for token in (group.split('|') if op == '=~' else [group]):
                    # 정규식 조각은 못 본다. 리터럴만 확인한다.
                    if re.fullmatch(r'[A-Za-z0-9_-]+', token) and token not in LABELS:
                        print("::error file=%s::%s 가 없는 %s 를 본다: %s"
                              % (path, name, label, token))
                        bad = 1
            expr = re.sub(r'\{[^}]*\}', ' ', raw)
            expr = re.sub(r'\b(?:by|without|on|ignoring|group_left|group_right)'
                          r'\s*\([^)]*\)', ' ', expr)
            # 콜론을 이름의 일부로 읽는다. 안 그러면 기록 규칙 이름이 조각으로
            # 갈려, 있는 지표가 없는 것으로 나온다.
            for match in re.finditer(r'\b[a-z_][a-z0-9_:]*\b', expr):
                metric = match.group()
                if expr[match.end():match.end() + 1] == '(' or metric in KEYWORDS:
                    continue
                if metric in recorded:
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

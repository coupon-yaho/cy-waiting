#!/usr/bin/env bash
# 대시보드가 참조하는 지표가 실제로 존재하는지 본다 (LG-11).
#
# **볼 곳 없는 지표를 만들지 않는 것과, 없는 지표를 보는 것은 같은 문제다.**
# 패널이 참조하는 이름이 코드에 없으면 그 패널은 영원히 빈 화면인데, 대시보드를
# 열어 보기 전까지 아무도 모른다. 지표 이름을 바꾸면 특히 그렇다.
set -uo pipefail

root=$(git rev-parse --show-toplevel 2>/dev/null) || root=.
cd "$root" || exit 1

readonly DIR=docker/observability/dashboards
readonly ALLOWED=docker/observability/dashboards/allowed-metrics.txt

# **없는 것과 사라진 것을 가른다.** 대시보드가 아직 없으면 검사 대상이 아니다.
[[ -d "$DIR" ]] || exit 0

fail=0
found=0

while IFS= read -r metric; do
    [[ -z "$metric" ]] && continue
    found=1
    # 코드의 이름은 점으로, 프로메테우스는 밑줄로 쓴다. 접미사(_total 등)도 뗀다.
    dotted=${metric%_total}
    dotted=${dotted//_/.}
    # **우리 지표는 코드에 있어야 한다.** 이름이 바뀌면 패널이 조용히 빈다.
    if [[ $metric == waiting_* ]]; then
        if ! grep -rqF "\"$dotted\"" src/main/java 2>/dev/null; then
            echo "::error file=$DIR::대시보드가 없는 지표를 본다: $metric (코드에 $dotted 없음)"
            fail=1
        fi
        continue
    fi
    # **남의 지표도 그냥 통과시키지 않는다.** 프레임워크가 내보내는 이름이라
    # 코드에 없을 뿐이고, 그쪽 이름도 버전이 올라가면 바뀐다. 허용 목록에 적어 둔
    # 것만 인정하고, 늘릴 때는 `/actuator/prometheus` 에서 직접 확인한다.
    if ! grep -v '^#' "$ALLOWED" 2>/dev/null | grep -qxF "$metric"; then
        echo "::error file=$DIR::허용 목록에 없는 지표를 본다: $metric ($ALLOWED 참고)"
        fail=1
    fi
done < <(
    python3 - <<'PY'
import json, pathlib, re

# PromQL 의 함수·연산자·집계 키워드. 지표 이름이 아니다.
KEYWORDS = {
    'by', 'without', 'on', 'ignoring', 'group_left', 'group_right', 'offset',
    'and', 'or', 'unless', 'bool', 'rate', 'irate', 'increase', 'sum', 'avg',
    'min', 'max', 'count', 'count_values', 'stddev', 'stdvar', 'topk',
    'bottomk', 'quantile', 'histogram_quantile', 'clamp_min', 'clamp_max',
    'abs', 'ceil', 'floor', 'round', 'delta', 'idelta', 'deriv', 'time',
    'absent', 'label_replace', 'label_join', 'sum_over_time', 'avg_over_time',
    'max_over_time', 'min_over_time', 'last_over_time', 'vector', 'scalar',
}

names = set()
for path in sorted(pathlib.Path('docker/observability/dashboards').glob('*.json')):
    doc = json.loads(path.read_text())
    for panel in doc.get('panels', []):
        # 로그 패널은 지표가 아니라 Loki 쿼리다. 이름 대조 대상이 아니다.
        if panel.get('type') == 'logs':
            continue
        for target in panel.get('targets', []):
            expr = target.get('expr', '')
            # 라벨 매처와 집계 묶음 안의 값은 지표 이름이 아니다. 먼저 걷어낸다.
            expr = re.sub(r'\{[^}]*\}', ' ', expr)
            expr = re.sub(r'\b(?:by|without|on|ignoring|group_left|group_right)\s*\([^)]*\)', ' ', expr)
            for match in re.finditer(r'\b[a-z_][a-z0-9_]*\b', expr):
                # 뒤에 `(` 가 오면 함수다.
                if expr[match.end():match.end() + 1] == '(':
                    continue
                if match.group() in KEYWORDS:
                    continue
                names.add(match.group())

print("\n".join(sorted(names)))
PY
)

# 하나도 못 찾으면 파싱이 어긋난 것이다. 통과시키지 않는다.
if [[ $found -eq 0 ]]; then
    echo "::error file=$DIR::대시보드에서 지표를 하나도 못 찾았다 — 검사가 헛돈다"
    fail=1
fi

exit $fail

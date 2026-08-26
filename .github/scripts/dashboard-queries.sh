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
    if ! grep -rqF "\"$dotted\"" src/main/java 2>/dev/null; then
        echo "::error file=$DIR::대시보드가 없는 지표를 본다: $metric (코드에 $dotted 없음)"
        fail=1
    fi
done < <(
    python3 - <<'PY'
import json, pathlib, re

names = set()
for path in pathlib.Path('docker/observability/dashboards').glob('*.json'):
    doc = json.loads(path.read_text())
    for panel in doc.get('panels', []):
        # 로그 패널은 지표가 아니라 Loki 쿼리다. 이름 대조 대상이 아니다.
        if panel.get('type') == 'logs':
            continue
        for target in panel.get('targets', []):
            expr = target.get('expr', '')
            names.update(re.findall(r'\bwaiting_[a-z_]+', expr))

print("\n".join(sorted(names)))
PY
)

# 하나도 못 찾으면 파싱이 어긋난 것이다. 통과시키지 않는다.
if [[ $found -eq 0 ]]; then
    echo "::error file=$DIR::대시보드에서 지표를 하나도 못 찾았다 — 검사가 헛돈다"
    fail=1
fi

exit $fail

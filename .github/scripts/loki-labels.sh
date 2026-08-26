#!/usr/bin/env bash
# Loki 로 나가는 라벨이 허용 목록 안인지 본다 (LG-4).
#
# **Loki 는 라벨마다 스트림을 만든다.** 고카디널리티 라벨 하나가 인덱스를
# 무너뜨린다. 쿠폰 식별자처럼 밖에서 오는 값은 가짓수에 상한이 없으므로,
# 그것을 라벨로 쓰는 순간 스트림이 요청 수만큼 늘어난다.
#
# 사람이 리뷰로 막을 수 없다. 라벨 하나 추가는 한 줄이고, 그 한 줄이 무엇을
# 뜻하는지는 운영에서 인덱스가 죽고 나서야 드러난다.
set -uo pipefail

root=$(git rev-parse --show-toplevel 2>/dev/null) || root=.
cd "$root" || exit 1

readonly CONFIG=docker/observability/promtail.yml

# **여기 없는 라벨은 못 쓴다.** 늘리려면 그 값의 가짓수에 상한이 있는지 먼저
# 따져야 하고, 그 판단을 이 목록이 강제한다.
readonly ALLOWED="app env instance level logger coupon_version"

# **없는 것과 사라진 것을 가른다.** 설정 자체가 없으면 검사 대상이 아니다.
[[ -f "$CONFIG" ]] || exit 0

fail=0
found=0

while IFS= read -r label; do
    [[ -z "$label" ]] && continue
    found=1
    if ! grep -qw -- "$label" <<<"$ALLOWED"; then
        echo "::error file=$CONFIG::허용하지 않은 Loki 라벨이다: $label"
        echo "::error::Loki 는 라벨마다 스트림을 만든다. 가짓수에 상한이 없는 값을" \
             "라벨로 쓰면 인덱스가 무너진다 (LG-4). 허용: $ALLOWED"
        fail=1
    fi
done < <(
    python3 - <<'PY'
import pathlib, sys

try:
    import yaml
except ImportError:
    print("::error::PyYAML 이 없어 라벨을 못 읽는다", file=sys.stderr)
    sys.exit(1)

doc = yaml.safe_load(pathlib.Path('docker/observability/promtail.yml').read_text())


def walk(node, out):
    """`labels` 와 `static_configs` 아래의 라벨 이름을 모은다."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key in ('labels', 'source_labels') and isinstance(value, dict):
                out.update(value.keys())
            elif key == 'labels' and isinstance(value, list):
                out.update(str(v) for v in value)
            else:
                walk(value, out)
    elif isinstance(node, list):
        for child in node:
            walk(child, out)


names = set()
walk(doc, names)
# `__`로 시작하는 것은 Promtail 의 내부 지시자다. 스트림 라벨이 아니라
# 수집 대상을 정하는 값이라 Loki 인덱스에 안 들어간다.
print("\n".join(sorted(n for n in names if not n.startswith('__'))))
PY
)

# 라벨을 하나도 못 찾으면 파싱이 어긋난 것이다. 통과시키지 않는다.
if [[ $found -eq 0 ]]; then
    echo "::error file=$CONFIG::Loki 라벨을 하나도 못 찾았다 — 검사가 헛돈다"
    fail=1
fi

exit $fail

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
    """스트림 라벨이 되는 이름을 전부 모은다.

    **나가는 자리만 본다.** `source_labels` 는 읽는 자리라 스트림에 안 남고,
    `target_label` 과 파이프라인의 `labels` 는 남는다. 읽는 자리만 보면
    리라벨로 만든 라벨이 통째로 검사를 지나간다.
    """
    if isinstance(node, dict):
        for key, value in node.items():
            if key == 'labels' and isinstance(value, dict):
                # 파이프라인의 `labels` 는 `{라벨: 추출키}` 다. 값이 비면
                # 라벨 이름이 곧 추출키다.
                out.update(value.keys())
            elif key == 'labels' and isinstance(value, list):
                out.update(str(v) for v in value)
            elif key == 'target_label' and isinstance(value, str):
                out.add(value)
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


# **정규식이 실제 로그 형식을 먹는지 본다.** Promtail 의 regex 스테이지는 안 맞으면
# 조용히 통과시킨다 — 라벨이 한 번도 안 붙는데 아무 데도 안 드러나고, ERROR 만 보는
# 패널이 영원히 빈 채로 남는다. 형식은 Boot 의 기본 파일 패턴(ISO-8601)이다.
python3 - <<'PY' || fail=1
import pathlib, re, sys

try:
    import yaml
except ImportError:
    print("::error::PyYAML 이 없어 정규식을 못 잰다", file=sys.stderr)
    sys.exit(1)

doc = yaml.safe_load(pathlib.Path('docker/observability/promtail.yml').read_text())
stages = [s for c in doc.get('scrape_configs', [])
          for s in c.get('pipeline_stages', []) if 'regex' in s]
if not stages:
    sys.exit(0)

SAMPLES = {
    '2026-08-25T10:00:00.123+09:00  INFO 1 --- [nio-2] c.k.w.Foo : 떴다': 'INFO',
    '2026-08-25T10:00:00.123+09:00 ERROR 1 --- [nio-2] c.k.w.Foo : 터졌다': 'ERROR',
    '2026-08-25T10:00:00.123+09:00  WARN 1 --- [nio-2] c.k.w.Foo : 서킷 열림': 'WARN',
}

bad = 0
for stage in stages:
    expr = stage['regex']['expression']
    if 'level' not in expr:
        continue
    for line, want in SAMPLES.items():
        match = re.search(expr, line)
        got = match.group('level') if match else None
        if got != want:
            print("::error file=docker/observability/promtail.yml::"
                  "레벨 정규식이 로그 형식을 못 먹는다: %r 에서 %r 을 기대했는데 %r"
                  % (line[:40], want, got))
            bad = 1
sys.exit(bad)
PY

exit $fail

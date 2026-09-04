#!/usr/bin/env bash
# 판정이 모든 잡을 기다리는지 본다.
#
# **잡을 늘리고 판정에서 빠뜨리면 그 잡이 깨져도 워크플로가 초록이다.** 실제로
# 그렇게 돼 있었다 — 빌드가 깨진 밤에 아래 잡들이 통째로 skipped 가 되고
# 보안만 남아 "이상 없음" 이 나갔다.
#
# 리포트도 같이 본다. 판정만 넣고 리포트를 빠뜨리면 전체 결과는 뒤집히는데
# 어느 항목이 깨졌는지를 리포트만 보고는 못 짚는다. 리포트가 존재하는 이유가
# 그것이다.
set -uo pipefail

# 검사할 뿌리를 인자로 받는다. 자기검증이 무너뜨린 워크플로를 만들어 넘겨야
# 이 검사가 실제로 무는지를 잴 수 있다 — 저장소만 볼 수 있으면 통과하는 모습만
# 본다. 인자가 없으면 저장소를 본다.
root=${1:-}
[ -n "$root" ] || root=$(git rev-parse --show-toplevel 2>/dev/null) || root=.
python3 - "$root" <<'PY'
import pathlib, sys

try:
    import yaml
except ImportError:
    print("  PyYAML 이 없어 검사를 못 한다", file=sys.stderr)
    sys.exit(1)

# **빼는 잡은 이유와 함께 여기 적는다.** 검사 밖에 두면 왜 빠졌는지가
# 아무 데도 안 남고, 다음 사람이 결함으로 읽거나 반대로 아무거나 뺀다.
EXEMPT = {
    # 실패해도 파이프라인을 막지 않는다. 판정이 아니라 가시성이다.
    "main.yml": {"journal-index"},
}
# 결과가 아니라 **출력**을 내는 잡. 리포트는 그 출력(지라 키·버전)을 이미
# 싣고 있고, 판정은 이 잡을 기다린다 — 리포트 본문에만 안 실린다.
OUTPUT_ONLY = {"context"}

root = pathlib.Path(sys.argv[1])
# 판정을 두는 진입점만 본다. 재사용 워크플로(_*.yml)는 잡 하나짜리라 판정이 없다.
entries = sorted(p for p in (root / ".github/workflows").glob("*.yml")
                 if not p.name.startswith("_"))

def needs_of(job):
    raw = job.get("needs", [])
    return [raw] if isinstance(raw, str) else list(raw)

bad = []
for path in entries:
    doc = yaml.safe_load(path.read_text()) or {}
    jobs = doc.get("jobs") or {}
    if "verdict" not in jobs:
        continue
    # 판정과 리포트 자신은 뺀다. 나머지는 전부 판정이 기다려야 한다.
    watchers = {"verdict", "report"}
    expected = set(jobs) - watchers - EXEMPT.get(path.name, set())
    # **리포트는 판정도 기다려야 한다.** 리포트 본문이 `needs.verdict.outputs`
    # 로 전체 결과를 싣기 때문이다 — 안 기다리면 그 값이 빈 문자열이 되고,
    # 리포트는 아무 말 없이 "실패" 로 나간다. 판정 자신은 자기를 안 기다린다.
    report_expected = expected | {"verdict"}
    waited = set(needs_of(jobs["verdict"]))
    for missing in sorted(expected - waited):
        bad.append(f"{path.name}: 판정이 '{missing}' 을 안 기다린다")

    # **없으면 넘어가지 않는다.** 리포트 잡을 통째로 지우면 아래 검사가 전부
    # 안 돌고 통과가 난다 — 이 검사가 막으려던 바로 그 상태다.
    report = jobs.get("report")
    if report is None:
        bad.append(f"{path.name}: 판정은 있는데 리포트 잡이 없다")
        continue
    reported = set(needs_of(report))
    for missing in sorted(report_expected - reported):
        bad.append(f"{path.name}: 리포트가 '{missing}' 을 안 기다린다")

    # 리포트가 각 잡의 결과를 실제로 싣는지까지 본다. `needs` 에만 넣고
    # 본문에서 빠뜨리면 읽는 사람이 그 항목의 결과를 못 찾는다.
    #
    # **두 형태를 따로 본다.** 한쪽에만 넣고 지나가는 것이 실제로 난 실수다 —
    # 합쳐서 보면 Slack 에는 있고 Confluence 에는 없는 상태가 통과한다.
    with_block = report.get("with") or {}

    # 슬랙 필드는 늘 있어야 한다. 컨플루언스는 선택이지만, **제목을 걸어 놓고
    # 본문이 없으면** 빈 문서가 나간다 — 그건 선택이 아니라 결함이다.
    shapes = [("fields", "슬랙 필드")]
    if with_block.get("confluence-title"):
        shapes.append(("confluence-body", "컨플루언스 표"))

    for shape, label in shapes:
        body = with_block.get(shape)
        if not body:
            # 비어 있으면 넘어가지 않는다. 넘어가면 전부 지운 것이 통과한다.
            bad.append(f"{path.name}: 리포트에 {label}가 없다")
            continue
        for job in sorted(expected - OUTPUT_ONLY):
            if f"needs.{job}.result" not in body:
                bad.append(f"{path.name}: {label}에 '{job}' 의 결과가 없다")

for line in bad:
    print(f"  {line}")
sys.exit(1 if bad else 0)
PY

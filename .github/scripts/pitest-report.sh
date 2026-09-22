#!/usr/bin/env bash
# 뮤턴트 위치 요약. 뮤테이션 가드가 실패할 때와 끝날 때 부른다.
#
# **수만으로는 어디를 볼지 모른다.** 리포트 보관이 끝나면 "모자라다" 한 줄만
# 남는다. 판정 안 선 것과 살아남은 것을 클래스·메서드·줄로 로그와 잡 요약에 쓴다.
#
# 종료: 0 요약을 썼거나 리포트가 없다 / 2 리포트를 못 읽었다
set -uo pipefail

report=${1:-build/reports/pitest/mutations.xml}

if [ ! -f "$report" ]; then
    echo "뮤턴트 위치 요약: 리포트가 없다 — $report"
    exit 0
fi

python3 - "$report" "${GITHUB_STEP_SUMMARY:-}" <<'PY'
import collections
import sys
import xml.etree.ElementTree as ET

report, summary_path = sys.argv[1], sys.argv[2]
LIMIT = 30
UNRESOLVED = {'RUN_ERROR', 'MEMORY_ERROR', 'NOT_STARTED', 'STARTED'}

try:
    mutants = ET.parse(report).getroot().findall('mutation')
except ET.ParseError as e:
    print(f"뮤턴트 위치 요약: 리포트를 못 읽었다 — {e}")
    sys.exit(2)


def where(m):
    cls = (m.findtext('mutatedClass') or '?').rsplit('.', 1)[-1]
    return f"{cls}.{m.findtext('mutatedMethod') or '?'}:{m.findtext('lineNumber') or '?'}"


def order(m):
    # 클래스 안에서는 줄 순서다. 문자열로 정렬하면 10 이 2 앞에 온다.
    number = m.findtext('lineNumber') or ''
    return (m.findtext('mutatedClass') or '', int(number) if number.isdigit() else 0,
            m.findtext('mutatedMethod') or '')


def line(m):
    return f"{where(m)} {m.get('status')} — {m.findtext('description') or ''}".rstrip(' —')


killed = [m for m in mutants if m.get('detected') == 'true']
unresolved = [m for m in mutants if m.get('status') in UNRESOLVED]
survived = [m for m in mutants
            if m.get('detected') != 'true' and m.get('status') not in UNRESOLVED]

out = [f"죽인 뮤턴트 {len(killed)}개 / 전체 {len(mutants)}개"]


def section(title, items):
    if not items:
        return
    out.append(f"{title} {len(items)}개")
    by_class = collections.Counter(where(m).split('.', 1)[0] for m in items)
    out.append("  클래스별: " + ", ".join(f"{c} {n}" for c, n in by_class.most_common(10)))
    for m in sorted(items, key=order)[:LIMIT]:
        out.append(f"  {line(m)}")
    if len(items) > LIMIT:
        out.append(f"  외 {len(items) - LIMIT}개")


section("판정 안 선 뮤턴트", unresolved)
section("살아남은 뮤턴트", survived)

text = "\n".join(out)
print(text)
if summary_path:
    with open(summary_path, 'a', encoding='utf-8') as f:
        f.write("### 뮤턴트 위치\n\n```\n" + text + "\n```\n")
PY

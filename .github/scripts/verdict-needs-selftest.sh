#!/usr/bin/env bash
# 판정 배선 검사의 자기검증 (TS-9).
#
# **무는지를 안 재면 무는 척만 하는 검사가 남는다.** 배선 검사를 넣자마자
# 그것이 났다 — 리포트 잡을 통째로 지우면 아래 검사가 전부 안 돌고 통과가
# 났고, 리포트가 판정을 안 기다려도 통과가 났다. 둘 다 사람이 찾았다.
#
# 그래서 온전한 워크플로 하나를 만들어 두고 **한 사례에 한 자리만** 무너뜨린다.
# 여러 자리를 같이 건드리면 무엇이 물었는지 알 수 없다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

CHECK=$PWD/.github/scripts/verdict-needs.sh
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

python3 - "$work" <<'PY'
import pathlib
import sys

work = pathlib.Path(sys.argv[1])

# `context` 는 결과가 아니라 출력을 내는 잡이라 리포트 본문에서 빼도 된다.
# 온전한 사례가 그것을 겸해서 든다 — 본문에 안 실린 채로 통과해야 한다.
JOBS = """name: 보기
on: push
jobs:
  context:
    runs-on: ubuntu-latest
    steps:
      - run: "true"
  build:
    runs-on: ubuntu-latest
    steps:
      - run: "true"
  test:
    runs-on: ubuntu-latest
    steps:
      - run: "true"
"""

ALL = "context, build, test"
FIELDS = "빌드 ${{ needs.build.result }} 시험 ${{ needs.test.result }}"
TABLE = "| 빌드 | ${{ needs.build.result }} | 시험 | ${{ needs.test.result }} |"


def verdict(needs=ALL):
    return (
        "  verdict:\n"
        f"    needs: [{needs}]\n"
        "    runs-on: ubuntu-latest\n"
        "    steps:\n"
        '      - run: "true"\n'
    )


def report(needs=ALL + ", verdict", fields=FIELDS, title=None, body=None):
    out = (
        "  report:\n"
        f"    needs: [{needs}]\n"
        "    uses: ./.github/workflows/_report.yml\n"
        "    with:\n"
    )
    if fields is not None:
        out += f'      fields: "{fields}"\n'
    if title is not None:
        out += f'      confluence-title: "{title}"\n'
    if body is not None:
        out += f'      confluence-body: "{body}"\n'
    return out


def case(name, text, filename="entry.yml"):
    path = work / name / ".github/workflows" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


case("ok", JOBS + verdict() + report())
case("verdict-misses-job", JOBS + verdict("context, build") + report())
case("no-report", JOBS + verdict())
case("report-misses-job", JOBS + verdict() + report("context, build, verdict"))
case("report-misses-verdict", JOBS + verdict() + report(ALL))
case("no-fields", JOBS + verdict() + report(fields=None))
case("empty-fields", JOBS + verdict() + report(fields=""))
case("fields-miss-result", JOBS + verdict() + report(fields="빌드 ${{ needs.build.result }}"))
case("title-without-body", JOBS + verdict() + report(title="야간"))
case("table-miss-result", JOBS + verdict() + report(
    title="야간", body="| 빌드 | ${{ needs.build.result }} |"))
case("table-ok", JOBS + verdict() + report(title="야간", body=TABLE))

# 판정이 없는 워크플로는 검사 대상이 아니다. 배포처럼 판정을 안 두는 진입점이
# 있고, 그것까지 물면 검사를 끄게 된다.
case("no-verdict", JOBS)

# 재사용 워크플로는 잡 하나짜리라 판정이 없다. 이름으로 거르는 것이 실제로
# 도는지 본다 — 안 걸러지면 `_report.yml` 자신이 위반으로 잡힌다.
case("reusable", JOBS + verdict("context"), filename="_inner.yml")

# 빼기로 한 잡은 파일 이름으로 걸린다. 이름이 안 맞으면 면제가 안 붙는다.
EXEMPT_JOB = """  journal-index:
    runs-on: ubuntu-latest
    steps:
      - run: "true"
"""
case("exempt", JOBS + EXEMPT_JOB + verdict() + report(), filename="main.yml")
case("exempt-other-name", JOBS + EXEMPT_JOB + verdict() + report())
PY

fail=0
# **문구까지 본다.** 종료 코드만 보면 엉뚱한 이유로 떨어진 것이 통과로 읽힌다.
# 통과를 기대하는 사례는 문구가 없다 — 그때는 아무 말도 안 나와야 맞다.
check() {
    local name=$1 dir=$2 want_rc=$3 want_word=${4:-}
    local out rc ok=1
    out=$("$CHECK" "$work/$dir" 2>&1); rc=$?
    [ "$rc" -eq "$want_rc" ] || ok=0
    if [ -n "$want_word" ]; then
        printf '%s' "$out" | grep -qF "$want_word" || ok=0
    elif [ -n "$out" ]; then
        ok=0
    fi
    if [ "$ok" -eq 1 ]; then
        echo "  ✓ $name"
    else
        echo "  ✗ $name — 종료 $rc (기대 $want_rc), 기대 문구 '$want_word'"
        printf '%s\n' "$out" | sed 's/^/      /'
        fail=1
    fi
}

echo "판정 배선 자기검증"

check "온전한 배선은 통과"            ok                    0
check "판정이 잡을 안 기다리면 문다"   verdict-misses-job    1 "판정이 'test' 을 안 기다린다"
check "리포트 잡이 없으면 문다"        no-report             1 "판정은 있는데 리포트 잡이 없다"
check "리포트가 잡을 안 기다리면 문다" report-misses-job     1 "리포트가 'test' 을 안 기다린다"
check "리포트가 판정을 안 기다리면 문다" report-misses-verdict 1 "리포트가 'verdict' 을 안 기다린다"
check "슬랙 필드가 없으면 문다"        no-fields             1 "리포트에 슬랙 필드가 없다"
check "슬랙 필드가 비면 문다"          empty-fields          1 "리포트에 슬랙 필드가 없다"
check "필드에 결과가 빠지면 문다"      fields-miss-result    1 "슬랙 필드에 'test' 의 결과가 없다"
check "제목만 걸고 본문이 없으면 문다"  title-without-body    1 "리포트에 컨플루언스 표가 없다"
check "표에 결과가 빠지면 문다"        table-miss-result     1 "컨플루언스 표에 'test' 의 결과가 없다"
check "표까지 온전하면 통과"           table-ok              0
check "판정이 없는 워크플로는 건너뛴다" no-verdict            0
check "재사용 워크플로는 안 본다"       reusable              0
check "빼기로 한 잡은 안 문다"          exempt                0
check "면제는 파일 이름에만 붙는다"     exempt-other-name     1 "판정이 'journal-index' 을 안 기다린다"

[ "$fail" -eq 0 ] || exit 1
echo "  판정 배선 자기검증 통과"

#!/usr/bin/env bash
# 뮤턴트 위치 요약의 자기검증 (TS-9).
#
# **실패 로그만으로 어디를 볼지 알아야 한다.** 리포트 보관이 끝나면 수만 남는데,
# 수로는 어느 클래스의 어느 줄인지 못 찾는다. 요약이 그 위치를 실제로 내는지,
# 리포트가 없거나 깨졌을 때 그 사실을 말하는지를 잰다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

REPORT=$PWD/.github/scripts/pitest-report.sh
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# PIT 이 쓰는 모양 그대로다. 속성은 홑따옴표로 온다.
mutant() {
    local detected=$1 status=$2 cls=$3 method=$4 line=$5
    printf "<mutation detected='%s' status='%s' numberOfTestsRun='1'>" "$detected" "$status"
    printf '<sourceFile>%s.java</sourceFile><mutatedClass>com.kafkick.%s</mutatedClass>' "$cls" "$cls"
    printf '<mutatedMethod>%s</mutatedMethod><methodDescription>()V</methodDescription>' "$method"
    printf '<lineNumber>%s</lineNumber>' "$line"
    printf '<mutator>org.pitest.mutationtest.engine.gregor.mutators.NegateConditionalsMutator</mutator>'
    printf '<description>negated conditional</description></mutation>\n'
}

mkdir -p "$work/mixed" "$work/many" "$work/broken" "$work/none"
{
    echo '<?xml version="1.0" encoding="UTF-8"?><mutations>'
    mutant true KILLED Alpha keep 3
    mutant true TIMED_OUT Alpha loop 4
    mutant false SURVIVED Alpha decide 10
    mutant false NO_COVERAGE Alpha idle 20
    mutant false RUN_ERROR Beta crash 5
    mutant false MEMORY_ERROR Beta hog 6
    echo '</mutations>'
} > "$work/mixed/mutations.xml"
{
    echo '<?xml version="1.0" encoding="UTF-8"?><mutations>'
    for i in $(seq 1 40); do mutant false SURVIVED Gamma "m$i" "$i"; done
    echo '</mutations>'
} > "$work/many/mutations.xml"
echo '<mutations><mutation' > "$work/broken/mutations.xml"

fail=0

# 사례 하나: 종료 코드와 꼭 들어야 할 문구들.
check() {
    local name=$1 dir=$2 want_rc=$3
    shift 3
    local out rc ok=1
    out=$(GITHUB_STEP_SUMMARY="$work/$dir/summary.md" "$REPORT" "$work/$dir/mutations.xml" 2>&1)
    rc=$?
    [ "$rc" -eq "$want_rc" ] || ok=0
    local word
    for word in "$@"; do
        case "$out" in *"$word"*) ;; *) ok=0 ;; esac
    done
    if [ "$ok" -eq 1 ]; then
        echo "  ✓ $name"
    else
        echo "  ✗ $name — 종료 $rc (기대 $want_rc)"
        printf '%s\n' "$out" | sed 's/^/      /'
        fail=1
    fi
}

echo "뮤턴트 위치 요약 자기검증"

check "판정 안 선 것의 위치를 낸다"    mixed  0 "판정 안 선 뮤턴트 2개" "Beta.crash:5 RUN_ERROR" "Beta.hog:6 MEMORY_ERROR"
check "살아남은 것의 위치를 낸다"      mixed  0 "살아남은 뮤턴트 2개" "Alpha.decide:10 SURVIVED" "Alpha.idle:20 NO_COVERAGE"
check "죽인 것과 시간 초과는 안 낸다"  mixed  0 "죽인 뮤턴트 2개 / 전체 6개"
check "클래스별로 모아 센다"           mixed  0 "Alpha 2" "Beta 2"
check "목록은 상한까지만 싣는다"       many   0 "Gamma.m30:30" "외 10개"
check "리포트가 없으면 말한다"         none   0 "리포트가 없다"
check "리포트가 깨지면 문다"           broken 2 "리포트를 못 읽었다"

# **잡 요약에도 남아야 한다.** 로그는 접히고 요약은 실행 화면 첫 장에 뜬다.
if grep -q "Beta.crash:5" "$work/mixed/summary.md" 2>/dev/null; then
    echo "  ✓ 잡 요약에 같은 목록을 쓴다"
else
    echo "  ✗ 잡 요약에 목록이 없다"
    fail=1
fi
# 목록 밖 사례가 새지 않는지 — 상한이 요약에도 걸린다.
if grep -q "Gamma.m31:31" "$work/many/summary.md" 2>/dev/null; then
    echo "  ✗ 잡 요약이 상한을 넘겼다"
    fail=1
else
    echo "  ✓ 잡 요약도 상한까지만 쓴다"
fi

[ "$fail" -eq 0 ] || exit 1
echo "  뮤턴트 위치 요약 자기검증 통과"

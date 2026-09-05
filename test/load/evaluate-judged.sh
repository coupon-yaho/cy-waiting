#!/usr/bin/env bash
# 판정한 요청 비율 (G10.10 의 기준).
#
# **`5xx < 0.1%` 로는 못 잰다.** 이 제품은 끊는 것도 줄 세우는 것도 정상
# 동작이라, 2xx 비율로 재면 보호 장치가 동작할수록 게이트가 빨개진다. 뒤집어도
# 못 쓴다 — 전원을 큐에 넣으면 무너져도 5xx 가 0 이다. Phase 6 이 O-7 로 이미
# 밝힌 것이고, 기준은 거기서 정한 **재료를 갖고 판정한 요청의 비율**이다.
#
# 실패로 세는 것과 안 세는 것은 `plan/90-decisions.md` 2.19 절의 표가 정한다.
# 여기서 다시 정하지 않는다 — 둘이 갈리는 순간 어느 쪽도 못 믿는다.
#
# **회차 앞뒤를 받아 차분한다.** 계수가 누적이라 절대값으로 읽으면 예열과 앞
# 회차가 분모에 섞인다. 예열 120 만 건이 신선으로 쌓인 상태면 실측 회차가
# 99.0% 여도 합쳐서 99.9% 가 나와 미달이 충족으로 뒤집힌다. 표본이 하나뿐이면
# 판정하지 않는다.
set -uo pipefail

# 종료 코드를 가른다 — 2 는 계기를 고치라는 뜻이고 1 은 제품을 고치라는 뜻이다.
# 같은 코드로 내면 "못 쟀다" 가 "미달" 로 읽힌다.
UNMEASURABLE=2

# **인자 누락도 판정 불가다.** `${1:?}` 로 두면 bash 가 1 로 끝내, 러너가 표본을
# 하나만 넘긴 배선 실수가 "미달"(제품 결함)로 읽힌다.
if [ $# -lt 2 ]; then
    echo "::error title=판정 비율::지표 표본 파일 둘이 필요하다 — 회차 이전과 이후"
    exit "$UNMEASURABLE"
fi
before=$1
after=$2

# **기준도 확인한다.** 그대로 awk 에 넘기면 `-1` 이 모든 회차를 통과시키고, 오타
# 하나가 "미달" 로 나가 계기 문제가 제품 문제로 읽힌다. 하한을 확인하면서 정작
# 판정을 가르는 수를 안 보고 있었다.
target=${JUDGED_TARGET_PCT:-99.9}
if ! printf '%s' "$target" | grep -Eq '^[0-9]+(\.[0-9]+)?$' \
        || awk -v t="$target" 'BEGIN{ exit (t > 0 && t <= 100) ? 1 : 0 }'; then
    echo "::error title=판정 비율::기준이 0 초과 100 이하의 백분율이 아니다: '$target'"
    exit "$UNMEASURABLE"
fi

for f in "$before" "$after"; do
    if [ ! -s "$f" ]; then
        echo "::error title=판정 비율::표본이 비었다 — 지표를 못 긁었다: $f"
        exit "$UNMEASURABLE"
    fi
done

# **여러 줄이 잡히면 못 읽는다.** 라벨 조합이 갈라져 시계열이 둘이 되면 마지막
# 값만 쓰는 것도, 이어 붙이는 것도 틀린다. 무엇이 갈렸는지를 먼저 봐야 한다.
#
# 노드가 여럿인 회차(10.7.3)는 노드마다 표본을 따로 뜨고 이 자를 노드별로 부른다.
# 한 파일에 여러 노드를 담아 합산하지 않는다 — 합산하면 한 노드만 망가진 회차와
# 전부 조금씩 나쁜 회차가 같은 수로 나온다.
read_metric() {
    awk -v q="$2" '
        $0 ~ "^waiting_judgement_total\\{" && $0 ~ ("quality=\"" q "\"") {
            v = $NF; n++
        }
        END { if (n > 1) print "MULTI"; else if (n == 1) print v }
    ' "$1"
}

# 프로메테우스는 큰 수를 지수 표기로 낸다 — 1e7 부터다. 100K 를 100 초만 지속해도
# 누적이 거기 닿으므로, 그 표기를 못 읽으면 목표 규모에서만 판정기가 죽는다.
numeric='^[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$'

value_of() {
    local file=$1 quality=$2 label=$3 raw
    raw=$(read_metric "$file" "$quality")
    if [ "$raw" = MULTI ]; then
        echo "::error title=판정 비율::${label} 의 ${quality} 시계열이 둘 이상이다 — 라벨이 갈렸다" >&2
        return 1
    fi
    # 열화 계수는 없을 수 있다 — 한 번도 안 났으면 그 시계열이 안 생긴다.
    # 신선이 없는 것은 다르다. 아래에서 따로 막는다.
    [ -z "$raw" ] && { printf '0'; return 0; }
    if ! printf '%s' "$raw" | grep -Eq "$numeric"; then
        echo "::error title=판정 비율::${label} 의 ${quality} 계수가 숫자가 아니다: '$raw'" >&2
        return 1
    fi
    printf '%s' "$raw"
}

# **신선이 이후 표본에 없으면 아무것도 안 잰 것이다.** 0 으로 읽으면 부하가
# 안 닿은 회차가 조용히 지나간다.
if [ -z "$(read_metric "$after" fresh)" ]; then
    echo "::error title=판정 비율::신선 판정 계수가 없다 — 지표를 먼저 본다"
    exit "$UNMEASURABLE"
fi

fresh_before=$(value_of "$before" fresh 이전) || exit "$UNMEASURABLE"
fresh_after=$(value_of "$after" fresh 이후) || exit "$UNMEASURABLE"
deg_before=$(value_of "$before" degraded 이전) || exit "$UNMEASURABLE"
deg_after=$(value_of "$after" degraded 이후) || exit "$UNMEASURABLE"

delta() { awk -v a="$1" -v b="$2" 'BEGIN{ printf "%.0f", b - a }'; }
fresh=$(delta "$fresh_before" "$fresh_after")
degraded=$(delta "$deg_before" "$deg_after")

# **줄어들 수 없는 값이다.** 줄었으면 회차 중에 게이트웨이가 다시 떴다는 뜻이라,
# 차분이 그 앞의 트래픽을 통째로 빼먹는다.
if [ "$fresh" -lt 0 ] || [ "$degraded" -lt 0 ]; then
    echo "::error title=판정 비율::계수가 줄었다 (신선 ${fresh} · 열화 ${degraded}) — 회차 중에 게이트웨이가 다시 떴다"
    exit "$UNMEASURABLE"
fi

total=$((fresh + degraded))
# 기대 건수를 알면 그 비례로 하한을 잡는다. 절대값 하나로는 목표가 커질수록
# 사실상 아무것도 안 막는다 — 600 만을 기대하는 회차에서 1,000 은 0.017% 다.
#
# **하한을 숫자로 확인한다.** 비숫자가 들어오면 산술이 죽고 하한이 빈 채로 비교에
# 들어가, 부하가 안 닿은 회차를 막으라고 둔 가드가 그 자리에서 사라진다.
if [ -n "${EXPECT_TOTAL:-}" ]; then
    if ! printf '%s' "$EXPECT_TOTAL" | grep -Eq '^[0-9]+$'; then
        echo "::error title=판정 비율::EXPECT_TOTAL 이 숫자가 아니다: '$EXPECT_TOTAL'"
        exit "$UNMEASURABLE"
    fi
    min_total=$((EXPECT_TOTAL / 2))
else
    min_total=${MIN_TOTAL:-1000}
    if ! printf '%s' "$min_total" | grep -Eq '^[0-9]+$'; then
        echo "::error title=판정 비율::MIN_TOTAL 이 숫자가 아니다: '$min_total'"
        exit "$UNMEASURABLE"
    fi
fi
if [ "$total" -lt "$min_total" ]; then
    echo "::error title=판정 비율::판정이 ${total} 건뿐이다 (최소 ${min_total}) — 부하가 안 닿았다"
    exit "$UNMEASURABLE"
fi

# **보여 주는 수와 판정하는 수를 가른다.** 넷째 자리에서 반올림한 문자열로
# 비교하면 99.89998% 가 "99.9000" 이 되어 기준을 통과한다. 판정은 계수에서
# 곧장 낸다.
pct=$(awk -v f="$fresh" -v t="$total" 'BEGIN{ printf "%.4f", f / t * 100 }')
printf '  %-24s %s\n' "재료를 갖고 판정" "$fresh"
printf '  %-24s %s\n' "재료 없이 판정" "$degraded"
printf '  %-24s %s%%\n' "판정한 요청 비율" "$pct"
printf '  %-24s %s%%\n' "기준" "$target"

# 기준을 정확히 맞춘 회차는 충족이다 — SLO 목표는 "그 수 이상" 을 뜻한다 (O-7).
if awk -v f="$fresh" -v n="$total" -v t="$target" \
        'BEGIN{ exit (f / n * 100 >= t) ? 0 : 1 }'; then
    echo "판정: 충족 — 기준 이상이다"
    exit 0
fi
echo "판정: 미달 — 게이트웨이가 제 일을 못 한 요청이 기준을 넘었다"
exit 1

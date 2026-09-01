#!/usr/bin/env bash
# 문서 앵커 검사. CI(_verify-conventions.yml)와 **같은 스크립트**를 쓴다.
#
# 파일만 확인하면 `](other.md#anchor)` 의 앵커가 사라져도 통과한다. 링크는
# 열리는데 엉뚱한 위치로 가므로 읽는 사람만 손해다.
#
# **앵커는 `<a id="...">` 만 센다.** 제목에서 슬러그를 유추하지 않는다 — 유추
# 규칙이 렌더러마다 달라서, 맞춰 봐야 어느 한쪽에서 깨진다. 걸어야 하면 그
# 자리에 `<a id="...">` 를 박는다.
#
# **대상은 추적 중인 파일이다.** 훑어서 모으면 CI 는 깨끗한 체크아웃을 보는데
# 로컬은 `build/` 산출물과 아직 안 올린 초안까지 본다 — 같은 스크립트를 부르고도
# 결과가 갈린다. `doc-links.sh` 와 같은 목록을 쓴다.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

# 목록을 환경으로 넘긴다 — 표준입력은 heredoc 이 이미 쓰고 있다.
TRACKED_MD=$(git ls-files '*.md')
export TRACKED_MD

python3 - <<'EOF'
import os, pathlib, re, sys
root = pathlib.Path('.').resolve()
md = [pathlib.Path(p) for p in os.environ['TRACKED_MD'].splitlines() if p]
anchors = {f: set(re.findall(r'<a id="([^"]+)"', f.read_text())) for f in md}
bad = 0
for f in md:
    body = re.sub(r'(?ms)^```.*?^```', '', f.read_text())
    body = re.sub(r'`[^`]*`', '', body)
    for m in re.finditer(r'\]\(([^)#]*)#([^)\s]+)\)', body):
        tgt, anc = m.group(1), m.group(2)
        if tgt.startswith(('http', 'mailto')):
            continue
        t = f if not tgt else (f.parent / tgt)
        try:
            t = pathlib.Path(t.resolve().relative_to(root))
        except ValueError:
            continue
        if t in anchors and anc not in anchors[t]:
            print(f'::error file={f}::없는 앵커 — {tgt or "(자기)"}#{anc}')
            bad += 1
sys.exit(1 if bad else 0)
EOF

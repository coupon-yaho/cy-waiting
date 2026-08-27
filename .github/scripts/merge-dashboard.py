#!/usr/bin/env python3
"""대시보드 병합 충돌을 푼다.

**패널은 줄이 아니라 항목이다.** 텍스트로 합치면 자리(gridPos)와 id 가 겹쳐
그라파나에서 패널이 서로를 가린다 — 실제로 그렇게 겹친 적이 있다.

제목을 신원으로 보고 양쪽을 합친 뒤 자리를 다시 깐다. 같은 제목이면 이쪽
(HEAD) 것을 쓴다 — 이 브랜치가 그 패널을 고치고 있다는 뜻이다.
"""
import json
import subprocess
import sys

PATH = 'docker/observability/dashboards/waiting.json'


def stage(n):
    out = subprocess.run(['git', 'show', f':{n}:{PATH}'], capture_output=True, text=True)
    return json.loads(out.stdout) if out.returncode == 0 else None


def main():
    base, ours, theirs = stage(1), stage(2), stage(3)
    if ours is None or theirs is None:
        print(f'{PATH} 에 충돌이 없다', file=sys.stderr)
        return 1
    # **패널 밖의 변경을 조용히 지우지 않는다.** 양쪽이 같은 최상위 필드를 다르게
    # 고쳤으면 사람이 봐야 한다 — 여기서 한쪽을 고르면 그 판단이 안 남는다.
    for key in set(ours) | set(theirs):
        if key == 'panels':
            continue
        mine, yours = ours.get(key), theirs.get(key)
        if mine == yours:
            continue
        was = (base or {}).get(key)
        if mine != was and yours != was:
            print(f"'{key}' 를 양쪽이 다르게 고쳤다 — 손으로 풀어라", file=sys.stderr)
            return 1
    merged = {}
    order = []
    for panel in theirs['panels'] + ours['panels']:
        title = panel['title']
        if title not in merged:
            order.append(title)
        merged[title] = panel
    panels = []
    for i, title in enumerate(order):
        panel = merged[title]
        panel['id'] = i + 1
        panel['gridPos'] = {'h': 8, 'w': 12, 'x': (i % 2) * 12, 'y': (i // 2) * 8}
        panels.append(panel)
    doc = dict(theirs)
    # 한쪽만 고친 최상위 필드는 그 값을 살린다.
    for key in set(ours) | set(theirs):
        if key == 'panels':
            continue
        if ours.get(key) != (base or {}).get(key):
            doc[key] = ours.get(key)
    doc['panels'] = panels
    with open(PATH, 'w') as f:
        f.write(json.dumps(doc, ensure_ascii=False, indent=2) + '\n')
    print('패널 %d개: %s' % (len(panels), ', '.join(order)))
    return 0


if __name__ == '__main__':
    sys.exit(main())

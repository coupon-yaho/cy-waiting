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
    ours, theirs = stage(2), stage(3)
    if ours is None or theirs is None:
        print(f'{PATH} 에 충돌이 없다', file=sys.stderr)
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
    doc['panels'] = panels
    with open(PATH, 'w') as f:
        f.write(json.dumps(doc, ensure_ascii=False, indent=2) + '\n')
    print('패널 %d개: %s' % (len(panels), ', '.join(order)))
    return 0


if __name__ == '__main__':
    sys.exit(main())

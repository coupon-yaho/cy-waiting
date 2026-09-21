"""명령에서 커밋 제목을 뽑는다. `check-commit-msg.sh` 가 부른다.

셸로 자르면 인용 안의 `#`·`|`·`;` 를 문법으로 읽어 제목이 잘리고 세그먼트가 갈린다.
표준 라이브러리의 셸 렉서를 써서 그것을 피한다.

표준 출력에 제목을 한 줄씩 낸다. 검사할 커밋이 없으면 아무것도 안 낸다.
두 표식이 따로 있다 — `__UNPARSED__` 는 인용 짝이 안 맞는 것이고,
`__NO_MESSAGE__` 는 메시지를 안 실은 커밋이다.
"""

import os
import re
import shlex
import sys

HEREDOC = re.compile(r"<<(-?)\s*(['\"]?)([A-Za-z_][A-Za-z0-9_]*)\2")
SEPARATORS = {";", "|", "&&", "||", "&"}
GLOBAL_WITH_VALUE = {"-c", "-C", "--git-dir", "--work-tree", "--namespace"}
# 메시지를 안 싣는 형태. 값이 붙어 와도 같은 뜻이다.
NO_MESSAGE = ("--no-edit", "--amend", "--fixup", "--squash",
              "--reuse-message", "-C", "-c", "--file", "-F")


def split_heredocs(cmd):
    """본문을 걷어내고 명령 줄만 남긴다. 본문은 여는 순서대로 따로 모은다.

    본문은 명령이 아니라 자료다. 안 걷으면 본문의 낱말이 명령으로 읽힌다.
    """
    # **줄 이음을 먼저 편다.** 역슬래시로 이어 쓰면 힙독 연산자가 둘째 물리적 줄에
    # 오는데, 물리적 줄만 보면 그 본문을 못 찾아 검사가 통째로 지나간다.
    lines = cmd.replace("\\\n", " ").split("\n")
    bodies = []
    code = []
    i = 0
    while i < len(lines):
        line = lines[i]
        code.append(line)
        i += 1
        found = HEREDOC.search(line)
        if not found:
            continue
        dash, _, delim = found.groups()
        body = []
        while i < len(lines):
            candidate = lines[i]
            i += 1
            # `<<-` 는 셸이 선행 탭을 뗀다. 구분자도 들여쓸 수 있다.
            trimmed = candidate.lstrip("\t") if dash else candidate
            if trimmed.strip() == delim:
                break
            body.append(trimmed)
        bodies.append(body)
    return code, bodies


def subject_of(body):
    """git 훅과 같게 고른다 — 빈 줄과 주석 줄을 건너뛴 첫 줄이 제목이다."""
    for raw in body:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        return line
    return ""


def subcommand(toks):
    """`-c 키=값`·`-C 경로` 뒤의 낱말은 부명령이 아니다. 건너뛰고 찾는다."""
    j = 1
    while j < len(toks):
        tok = toks[j]
        if tok in GLOBAL_WITH_VALUE:
            j += 2
            continue
        if tok.startswith("-"):
            j += 1
            continue
        return tok, j
    return "", len(toks)


def message_of(rest):
    """제목과 표준 입력 여부를 돌려준다. 붙어 온 값도 같이 다룬다."""
    skip = False
    for k, tok in enumerate(rest):
        if skip:
            skip = False
            continue
        if tok in ("-m", "--message"):
            return (rest[k + 1] if k + 1 < len(rest) else ""), False
        if tok.startswith("--message="):
            return tok[len("--message="):], False
        if tok in ("-F-", "--file=-"):
            return None, True
        if tok in ("-F", "--file"):
            return None, k + 1 < len(rest) and rest[k + 1] == "-"
        # **파일 플래그가 먼저다.** `-Fmsg.txt` 는 m 을 품지만 메시지 묶음이 아니다 —
        # 묶음으로 읽으면 파일 이름의 조각이 제목이 된다.
        if tok.startswith("-F") and not tok.startswith("--"):
            return None, tok == "-F-"
        # `-am` 처럼 묶인 짧은 플래그. 값이 붙어 오면 그것이 메시지다.
        if not tok.startswith("--") and re.match(r"^-[A-Za-z]*m", tok):
            attached = tok[tok.index("m") + 1:]
            if attached:
                return attached, False
            return (rest[k + 1] if k + 1 < len(rest) else ""), False
    return None, False


def main():
    cmd = os.environ.get("COMMIT_CMD", "")
    code, bodies = split_heredocs(cmd)
    try:
        tokens = shlex.split(" ".join(code), comments=True)
    except ValueError:
        # 짝이 안 맞는 인용. 무엇을 실행할지 모르므로 막는 쪽으로 넘긴다.
        print("__UNPARSED__")
        return

    segments = [[]]
    for tok in tokens:
        if tok in SEPARATORS:
            segments.append([])
        else:
            segments[-1].append(tok)

    heredoc_at = 0
    for segment in segments:
        toks = list(segment)
        while toks and re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", toks[0]):
            toks.pop(0)
        # **경로를 뗀다.** 붙여 부르면 이름 앞이 슬래시라 낱말로는 안 걸린다.
        if not toks or os.path.basename(toks[0]) != "git":
            continue
        sub, at = subcommand(toks)
        if sub != "commit":
            continue

        rest = toks[at + 1:]
        message, from_stdin = message_of(rest)
        if message is not None:
            print(message.split("\n")[0].strip())
            continue
        if from_stdin:
            body = bodies[heredoc_at] if heredoc_at < len(bodies) else []
            heredoc_at += 1
            # 힙독이 아니면 본문이 명령 밖이라 여기서는 못 본다.
            found = subject_of(body)
            if found:
                print(found)
            continue
        if any(tok == opt or tok.startswith(opt)
               for tok in rest for opt in NO_MESSAGE):
            continue
        print("__NO_MESSAGE__")


if __name__ == "__main__":
    main()
    sys.exit(0)

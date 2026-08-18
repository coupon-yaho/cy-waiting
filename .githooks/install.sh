#!/usr/bin/env bash
# git 훅 설치. 저장소를 클론한 뒤 한 번 실행한다.
#
#   ./.githooks/install.sh
#
# `.git/hooks/` 에 복사하지 않고 core.hooksPath 를 쓴다 — 복사하면 훅을
# 고칠 때마다 각자 다시 깔아야 하고, 누가 안 깔았는지 알 수 없다.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
root=$(git -C "$here" rev-parse --show-toplevel)

chmod +x "$here"/commit-msg "$here"/lib/*.sh
git -C "$root" config core.hooksPath .githooks

echo "설치됨 — core.hooksPath = .githooks"
echo
echo "확인:"
echo "  git -C '$root' config core.hooksPath"
echo
echo "해제하려면:"
echo "  git -C '$root' config --unset core.hooksPath"

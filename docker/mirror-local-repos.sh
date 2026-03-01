#!/usr/bin/env bash
set -euo pipefail

SOURCE_ROOT="${SOURCE_ROOT:-$HOME/code-external}"
TARGET_ROOT="${GITALY_REPOSITORIES_DIR:-/Volumes/External/gitaly-workspace}"

if [ "$#" -gt 0 ]; then
  repos=("$@")
else
  repos=(
    "hepquant-config"
    "detoxu-config"
    "biofidelity-config"
    "dxflow-examples"
  )
fi

mkdir -p "$TARGET_ROOT"

echo "Source root: $SOURCE_ROOT"
echo "Target root: $TARGET_ROOT"
echo

for repo in "${repos[@]}"; do
  src="$SOURCE_ROOT/$repo"
  dst="$TARGET_ROOT/${repo}.git"

  if [ ! -d "$src" ]; then
    echo "Skipping $repo: source repo not found at $src"
    continue
  fi

  if [ -d "$dst" ]; then
    echo "Syncing existing mirror: $dst"
    git -C "$dst" remote set-url origin "$src"
    git -C "$dst" fetch --prune origin '+refs/*:refs/*'
  else
    echo "Creating mirror: $dst"
    git clone --mirror "$src" "$dst"
  fi
done

echo
echo "Done."

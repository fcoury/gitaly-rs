#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST_DIR="${1:-${ROOT_DIR}/dist/bin}"
PROFILE="${PROFILE:-release}"
TARGET_DIR="${TARGET_DIR:-${ROOT_DIR}/target}"
BUILD_PROFILE_DIR="${PROFILE}"

BINARIES=(
  gitaly
  gitaly-rs
  gitaly-gateway
  gitaly-hooks
  gitaly-ssh
  gitaly-backup
  gitaly-gpg
  gitaly-lfs-smudge
  gitaly-blackbox
)

echo "Building workspace binaries (profile: ${PROFILE})..."
if [[ "${PROFILE}" == "release" ]]; then
  cargo build --workspace --bins --release
elif [[ "${PROFILE}" == "debug" ]]; then
  cargo build --workspace --bins
  BUILD_PROFILE_DIR="debug"
else
  cargo build --workspace --bins --profile "${PROFILE}"
fi

mkdir -p "${DEST_DIR}"

for binary in "${BINARIES[@]}"; do
  source_path="${TARGET_DIR}/${BUILD_PROFILE_DIR}/${binary}"
  if [[ ! -x "${source_path}" ]]; then
    echo "missing binary: ${source_path}" >&2
    exit 1
  fi

  install -m 0755 "${source_path}" "${DEST_DIR}/${binary}"
done

echo "Staged ${#BINARIES[@]} binaries into ${DEST_DIR}"

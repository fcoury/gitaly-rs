#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARTIFACT_DIR="${ROOT_DIR}/artifacts/stress-gate"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
mkdir -p "${ARTIFACT_DIR}"

declare -A MIN_RPS
MIN_RPS[stress_repository_exists]="${STRESS_MIN_RPS_REPOSITORY_EXISTS:-100}"
MIN_RPS[stress_repository_metadata]="${STRESS_MIN_RPS_REPOSITORY_METADATA:-80}"
MIN_RPS[stress_server_info]="${STRESS_MIN_RPS_SERVER_INFO:-120}"

run_bench() {
  local bench="$1"
  local min_rps="$2"
  local log_file="${ARTIFACT_DIR}/${TIMESTAMP}-${bench}.log"

  echo "==> running ${bench} (min_rps=${min_rps})"
  (
    cd "${ROOT_DIR}"
    cargo bench --bench "${bench}" -- --nocapture
  ) | tee "${log_file}"

  local line
  line="$(grep -E "stress_gate bench=${bench} " "${log_file}" | tail -n1 || true)"
  if [[ -z "${line}" ]]; then
    echo "stress gate parse failure for ${bench}: no stress_gate output found" >&2
    return 1
  fi

  local requests="" elapsed_ms="" rps=""
  for token in ${line}; do
    case "${token}" in
      requests=*) requests="${token#requests=}" ;;
      elapsed_ms=*) elapsed_ms="${token#elapsed_ms=}" ;;
      rps=*) rps="${token#rps=}" ;;
    esac
  done

  if [[ -z "${requests}" || -z "${elapsed_ms}" || -z "${rps}" ]]; then
    echo "stress gate parse failure for ${bench}: ${line}" >&2
    return 1
  fi

  if awk -v value="${rps}" -v minimum="${min_rps}" 'BEGIN { exit(value + 0 >= minimum + 0 ? 0 : 1) }'; then
    echo "PASS ${bench}: requests=${requests} elapsed_ms=${elapsed_ms} rps=${rps}"
  else
    echo "FAIL ${bench}: requests=${requests} elapsed_ms=${elapsed_ms} rps=${rps} < min_rps=${min_rps}" >&2
    return 1
  fi
}

for bench in stress_repository_exists stress_repository_metadata stress_server_info; do
  run_bench "${bench}" "${MIN_RPS[${bench}]}"
done

echo "stress suite passed; logs written to ${ARTIFACT_DIR}"

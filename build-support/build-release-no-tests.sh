#!/usr/bin/env bash
#
# Reproduce the successful local Kudu build:
#   - Ninja generator
#   - Release build
#   - NO_TESTS=1
#   - ccache compiler launcher when available
#
# Useful overrides:
#   BUILD_DIR=/path/to/build/dir JOBS=8 ./build-support/build-release-no-tests.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${ROOT}/build/ninja-no-tests}"
JOBS="${JOBS:-$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 8)}"
MIN_FREE_GB="${MIN_FREE_GB:-5}"

TP_COMMON_BIN="${ROOT}/thirdparty/installed/common/bin"
if [[ -x "${TP_COMMON_BIN}/cmake" ]]; then
  export PATH="${TP_COMMON_BIN}:${PATH}"
fi

log() {
  printf '[kudu-build] %s\n' "$*"
}

die() {
  printf '[kudu-build] ERROR: %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

available_gb() {
  df -Pk "$1" | awk 'NR == 2 { printf "%d\n", $4 / 1024 / 1024 }'
}

cleanup_disk_junk() {
  log "low disk space detected; cleaning safe local build leftovers"

  rm -rf "${BUILD_DIR}/CMakeFiles/CMakeTmp" 2>/dev/null || true

  if command -v ccache >/dev/null 2>&1; then
    ccache --cleanup >/dev/null 2>&1 || true
  fi

  # Remove only stale temporary directories owned by the current user and older
  # than one day. Keep this conservative: do not touch source, thirdparty, or
  # existing build outputs.
  find /tmp -mindepth 1 -maxdepth 1 -user "$(id -un)" -mtime +1 \
    \( -name 'cmake-*' -o -name 'kudu-build-*' \) \
    -exec rm -rf {} + 2>/dev/null || true
}

ensure_space() {
  local path="$1"
  local free_gb
  free_gb="$(available_gb "$path")"
  if (( free_gb < MIN_FREE_GB )); then
    cleanup_disk_junk
    free_gb="$(available_gb "$path")"
  fi
  (( free_gb >= MIN_FREE_GB )) || die "not enough free space on ${path}: ${free_gb}G available, need at least ${MIN_FREE_GB}G"
}

require_cmd cmake
require_cmd ninja

if command -v ccache >/dev/null 2>&1; then
  CMAKE_LAUNCHER_ARGS=(
    -DCMAKE_C_COMPILER_LAUNCHER=ccache
    -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
  )
else
  CMAKE_LAUNCHER_ARGS=()
  log "ccache not found; continuing without compiler launcher"
fi

ensure_space "${ROOT}"
ensure_space /tmp

log "repo: ${ROOT}"
log "build dir: ${BUILD_DIR}"
log "jobs: ${JOBS}"
log "cmake: $(command -v cmake)"
log "ninja: $(command -v ninja)"

cmake -S "${ROOT}" -B "${BUILD_DIR}" -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DNO_TESTS=1 \
  "${CMAKE_LAUNCHER_ARGS[@]}"

cmake --build "${BUILD_DIR}" --parallel "${JOBS}"

log "build completed successfully"
log "main binaries:"
ls -lh \
  "${BUILD_DIR}/bin/kudu" \
  "${BUILD_DIR}/bin/kudu-master" \
  "${BUILD_DIR}/bin/kudu-tserver" \
  "${BUILD_DIR}/lib/exported/libkudu_client.so"

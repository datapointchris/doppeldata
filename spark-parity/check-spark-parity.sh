#!/usr/bin/env bash
# Checks doppeldata.spark against doppeldata run locally, in each Spark image named, one container at a time. Each generates
# 10M rows of the eight-columns example through generate and write, with 7 and with 40 partitions, checks spark_holds
# against how the image carries every column of every-column-kind.profile.yaml, and generates and writes the
# orders-customers bundle.
#
#   spark-parity/check-spark-parity.sh [--scenario FILE] WHEEL [IMAGE...]
#
# WHEEL is the library's manylinux2014 x86_64 wheel. With no IMAGE, it checks each image spark-images.txt lists. uv is
# mounted into each container, which inside-spark-image.sh uses to give an image with no Python 3.11 one. The container
# runs as the caller, so what uv caches in the mounted cache directory stays the caller's. With --scenario, every run
# applies FILE, such as eight-columns.scenario.yaml beside this script.
set -euo pipefail

readonly ROWS=10000000 SEED=42 FILE_ROWS=1000000 BUNDLE_FILE_ROWS=100000
readonly CONTAINER_MEMORY=7g DRIVER_MEMORY=3g SPARK_MASTER='local[8]'

main() {
  local scenario=() mounted=()
  if [[ ${1:-} == --scenario ]] && (($# >= 2)); then
    scenario=(--scenario "$(realpath "$2")")
    mounted=(-v "${scenario[1]}:/work/scenario.yaml:ro")
    shift 2
  fi
  if (($# < 1)); then
    echo "usage: $(basename "$0") [--scenario FILE] WHEEL [IMAGE...]" >&2
    exit 2
  fi
  local wheel here cache image fixture images=() status=0
  wheel=$(realpath "$1")
  shift
  here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
  source "$here/run-spark-image-as-caller.sh"
  (($# > 0)) || mapfile -t images <"$here/spark-images.txt"
  (($# > 0)) || set -- "${images[@]}"
  fixture=$here/../crates/core/tests/every-column-kind.profile.yaml
  # Global, because errexit drops main's locals before the EXIT trap runs.
  work=$(mktemp -d)
  trap 'rm -rf "$work"' EXIT
  cache=${XDG_CACHE_HOME:-$HOME/.cache}/doppeldata-spark-parity
  mkdir -p "$cache"
  uv run --project "$here/.." doppeldata data generate eight-columns --rows "$ROWS" --seed "$SEED" \
    --file-rows "$FILE_ROWS" --out "$work/local" "${scenario[@]}" >&2
  uv run --project "$here/.." doppeldata data generate orders-customers --seed "$SEED" --file-rows "$BUNDLE_FILE_ROWS" \
    --out "$work/bundle" >&2
  for image in "$@"; do
    echo "== $image" >&2
    run_spark_image_as_caller "$work" "$image" --memory "$CONTAINER_MEMORY" \
      -e UV_CACHE_DIR=/uv/cache -e UV_PYTHON_INSTALL_DIR=/uv/python -v "$(command -v uv):/opt/uv:ro" -v "$cache:/uv" -v "$here:/parity:ro" -v "$work/local:/work/local:ro" \
      -v "$work/bundle:/work/bundle:ro" -v "$fixture:/work/every-column-kind.profile.yaml:ro" "${mounted[@]}" \
      -v "$wheel:/wheel/$(basename "$wheel"):ro" --entrypoint bash "$image" \
      /parity/inside-spark-image.sh "/wheel/$(basename "$wheel")" "$SPARK_MASTER" "$DRIVER_MEMORY" \
      /parity/compare_spark_with_local.py /work/local/_manifest.json /tmp/out /work/every-column-kind.profile.yaml \
      /work/bundle/_manifest.json "${mounted:+/work/scenario.yaml}" || status=1
  done
  exit "$status"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi

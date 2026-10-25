#!/usr/bin/env bash
# Checks doppeldata.spark against doppeldata run locally, in each Spark image named, one container at a time. Each generates
# 10M rows of the eight-columns example through generate and write, with 7 and with 40 partitions.
#
#   spark-parity/check-spark-parity.sh WHEEL IMAGE...
#
# WHEEL is the library's manylinux2014 x86_64 wheel. uv is mounted into each container, which inside-spark-image.sh uses to
# give an image with no Python 3.11 one.
set -euo pipefail

readonly ROWS=10000000 SEED=42 FILE_ROWS=1000000
readonly CONTAINER_MEMORY=7g DRIVER_MEMORY=3g SPARK_MASTER='local[8]'

main() {
  if (($# < 2)); then
    echo "usage: $(basename "$0") WHEEL IMAGE..." >&2
    exit 2
  fi
  local wheel here work cache image status=0
  wheel=$(realpath "$1")
  shift
  here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
  work=$(mktemp -d)
  trap 'rm -rf "$work"' EXIT
  cache=${XDG_CACHE_HOME:-$HOME/.cache}/doppeldata-spark-parity
  mkdir -p "$cache"
  uv run --project "$here/.." doppeldata data generate eight-columns --rows "$ROWS" --seed "$SEED" \
    --file-rows "$FILE_ROWS" --out "$work/local" >&2
  for image in "$@"; do
    echo "== $image" >&2
    # Hadoop's login refuses a uid with no name, so the container runs as this user under the image's own passwd plus one line.
    {
      docker run --rm --entrypoint cat "$image" /etc/passwd
      echo "parity:x:$(id -u):$(id -g):parity:/tmp:/bin/bash"
    } >"$work/passwd"
    docker run --rm --memory "$CONTAINER_MEMORY" --user "$(id -u):$(id -g)" -e HOME=/tmp \
      -e UV_CACHE_DIR=/uv/cache -e UV_PYTHON_INSTALL_DIR=/uv/python -v "$work/passwd:/etc/passwd:ro" \
      -v "$(command -v uv):/opt/uv:ro" -v "$cache:/uv" -v "$here:/parity:ro" -v "$work/local:/work/local:ro" \
      -v "$wheel:/wheel/$(basename "$wheel"):ro" --entrypoint bash "$image" \
      /parity/inside-spark-image.sh "/wheel/$(basename "$wheel")" "$SPARK_MASTER" "$DRIVER_MEMORY" || status=1
  done
  exit "$status"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi

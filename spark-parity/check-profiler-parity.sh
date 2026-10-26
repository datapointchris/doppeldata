#!/usr/bin/env bash
# Checks the Spark profiler against the Polars one, in each Spark image named, one container at a time. DATA is profiled on
# the host through Polars and in each image through Spark. Both profiles must pass `profile check`, and
# compare_spark_profile_with_polars.py holds the Spark one to the Polars one.
#
#   spark-parity/check-profiler-parity.sh WHEEL DATA [IMAGE...]
#
# WHEEL is the library's manylinux2014 x86_64 wheel. DATA is a CSV, TSV or Parquet file, or a directory of one table's
# files. POLICY names the privacy policy, `default` when unset. KEEP names a directory each profile is copied into. With no
# IMAGE, it checks each image spark-images.txt lists.
set -euo pipefail

readonly CONTAINER_MEMORY=8g DRIVER_MEMORY=4g SPARK_MASTER='local[8]'
# What the profiler reads text with beside the wheel, each at the version the host's Polars profile read with, so both
# record the same readers. Then what it lists S3 and fits families with, at what the image's Python takes.
readonly READING_DISTRIBUTIONS=(duckdb clevercsv charset-normalizer)
readonly OTHER_PACKAGES='boto3 scipy'

main() {
  if (($# < 2)); then
    echo "usage: $(basename "$0") WHEEL DATA [IMAGE...]" >&2
    exit 2
  fi
  local wheel data name here cache image packages images=() policy=${POLICY:-default} status=0
  wheel=$(realpath "$1")
  data=$(realpath "$2")
  name=$(basename "$data")
  shift 2
  here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
  source "$here/run-spark-image-as-caller.sh"
  (($# > 0)) || mapfile -t images <"$here/spark-images.txt"
  (($# > 0)) || set -- "${images[@]}"
  # Global, because errexit drops main's locals before the EXIT trap runs.
  work=$(mktemp -d)
  trap 'rm -rf "$work"' EXIT
  cache=${XDG_CACHE_HOME:-$HOME/.cache}/doppeldata-spark-parity
  packages="$(uv run --project "$here/.." python -c \
    'import importlib.metadata as m, sys; print(" ".join(f"{d}=={m.version(d)}" for d in sys.argv[1:]))' "${READING_DISTRIBUTIONS[@]}") $OTHER_PACKAGES"
  mkdir -p "$cache" ${KEEP:+"$KEEP"}
  uv run --project "$here/.." doppeldata profile create "$data" --policy "$policy" --out "$work/polars.yaml" >&2
  uv run --project "$here/.." doppeldata profile check "$work/polars.yaml" >&2 || status=1
  [[ -z "${KEEP:-}" ]] || cp "$work/polars.yaml" "$KEEP/${name%.*}.polars.yaml"
  for image in "$@"; do
    echo "== $image" >&2
    rm -f "$work/spark.yaml"
    run_spark_image_as_caller "$work" "$image" --memory "$CONTAINER_MEMORY" \
      -e UV_CACHE_DIR=/uv/cache -e UV_PYTHON_INSTALL_DIR=/uv/python -e EXTRA_PACKAGES="$packages" \
      -v "$(command -v uv):/opt/uv:ro" -v "$cache:/uv" -v "$here:/parity:ro" \
      -v "$data:/data/$name:ro" -v "$work:/work" -v "$wheel:/wheel/$(basename "$wheel"):ro" --entrypoint bash "$image" \
      /parity/inside-spark-image.sh "/wheel/$(basename "$wheel")" "$SPARK_MASTER" "$DRIVER_MEMORY" \
      /parity/profile_on_spark.py "/data/$name" /work/spark.yaml "$policy" || {
      status=1
      continue
    }
    uv run --project "$here/.." doppeldata profile check "$work/spark.yaml" >&2 || status=1
    [[ -z "${KEEP:-}" ]] || cp "$work/spark.yaml" "$KEEP/${name%.*}.$(basename "${image//:/-}").yaml"
    uv run --project "$here/.." python "$here/compare_spark_profile_with_polars.py" "$work/polars.yaml" "$work/spark.yaml" "$data" \
      || status=1
  done
  exit "$status"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi

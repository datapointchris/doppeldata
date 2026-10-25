#!/usr/bin/env bash
# Checks the Spark profiler against the Polars one, in each Spark image named, one container at a time. DATA is profiled on
# the host through Polars and in each image through Spark. Both profiles must pass `profile check`, and
# compare_spark_profile_with_polars.py holds the Spark one to the Polars one.
#
#   spark-parity/check-profiler-parity.sh WHEEL DATA [IMAGE...]
#
# WHEEL is the library's manylinux2014 x86_64 wheel. DATA is a CSV, TSV or Parquet file, or a directory of one table's
# files. POLICY names the privacy policy, `default` when unset. With no IMAGE, it checks DEFAULT_IMAGES.
set -euo pipefail

readonly CONTAINER_MEMORY=8g DRIVER_MEMORY=4g SPARK_MASTER='local[8]'
readonly DEFAULT_IMAGES=(
  apache/spark:3.5.9-python3
  apache/spark:4.1.3-python3
  public.ecr.aws/glue/aws-glue-libs:5
)
# What the profiler reads text and fits families with beside the wheel, at the versions its results were measured with.
readonly PROFILING_PACKAGES='duckdb==2.0.0 clevercsv==0.8.5 charset-normalizer boto3 scipy'

main() {
  if (($# < 2)); then
    echo "usage: $(basename "$0") WHEEL DATA [IMAGE...]" >&2
    exit 2
  fi
  local wheel data name here work cache image policy=${POLICY:-default} status=0
  wheel=$(realpath "$1")
  data=$(realpath "$2")
  name=$(basename "$data")
  shift 2
  (($# > 0)) || set -- "${DEFAULT_IMAGES[@]}"
  here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
  work=$(mktemp -d)
  trap 'rm -rf "$work"' EXIT
  cache=${XDG_CACHE_HOME:-$HOME/.cache}/doppeldata-spark-parity
  mkdir -p "$cache"
  uv run --project "$here/.." doppeldata profile create "$data" --policy "$policy" --out "$work/polars.yaml" >&2
  uv run --project "$here/.." doppeldata profile check "$work/polars.yaml" >&2 || status=1
  for image in "$@"; do
    echo "== $image" >&2
    rm -f "$work/spark.yaml"
    # Hadoop's login refuses a uid with no name, so the container runs as this user under the image's own passwd plus one line.
    {
      docker run --rm --entrypoint cat "$image" /etc/passwd
      echo "parity:x:$(id -u):$(id -g):parity:/tmp:/bin/bash"
    } >"$work/passwd"
    docker run --rm --memory "$CONTAINER_MEMORY" --user "$(id -u):$(id -g)" -e HOME=/tmp \
      -e UV_CACHE_DIR=/uv/cache -e UV_PYTHON_INSTALL_DIR=/uv/python -e EXTRA_PACKAGES="$PROFILING_PACKAGES" \
      -v "$work/passwd:/etc/passwd:ro" -v "$(command -v uv):/opt/uv:ro" -v "$cache:/uv" -v "$here:/parity:ro" \
      -v "$data:/data/$name:ro" -v "$work:/work" -v "$wheel:/wheel/$(basename "$wheel"):ro" --entrypoint bash "$image" \
      /parity/inside-spark-image.sh "/wheel/$(basename "$wheel")" "$SPARK_MASTER" "$DRIVER_MEMORY" \
      /parity/profile_on_spark.py "/data/$name" /work/spark.yaml "$policy" || {
      status=1
      continue
    }
    uv run --project "$here/.." doppeldata profile check "$work/spark.yaml" >&2 || status=1
    uv run --project "$here/.." python "$here/compare_spark_profile_with_polars.py" "$work/polars.yaml" "$work/spark.yaml" "$data" \
      || status=1
  done
  exit "$status"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi

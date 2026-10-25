#!/usr/bin/env bash
# Writes the orders example under the bad-csv example scenario and once for each fault kind alone, then prints what DuckDB
# and Spark make of each kind, one line per kind and reader. DuckDB reads on the host, through the workspace. Spark reads
# in each image named, one container at a time, with the image's own Python, because the reader needs only PySpark.
#
#   malformed-file-readers/read-every-fault-kind.sh [IMAGE...]
#
# With no IMAGE, it reads with each image spark-parity/spark-images.txt lists.
set -euo pipefail

readonly CONTAINER_MEMORY=4g DRIVER_MEMORY=2g SPARK_MASTER='local[4]'

main() {
  local here image images=() status=0
  here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
  source "$here/../spark-parity/run-spark-image-as-caller.sh"
  (($# > 0)) || mapfile -t images <"$here/../spark-parity/spark-images.txt"
  (($# > 0)) || set -- "${images[@]}"
  # Global, because errexit drops main's locals before the EXIT trap runs.
  work=$(mktemp -d)
  trap 'rm -rf "$work"' EXIT
  uv run --project "$here/.." python "$here/write_every_fault_kind.py" "$work/data"
  uv run --project "$here/.." python "$here/read_with_duckdb.py" "$work/data" || status=1
  for image in "$@"; do
    echo "== $image" >&2
    run_spark_image_as_caller "$work" "$image" --memory "$CONTAINER_MEMORY" -v "$here:/readers:ro" \
      -v "$work/data:/work/data:ro" --entrypoint bash "$image" \
      /readers/read-in-spark-image.sh "$SPARK_MASTER" "$DRIVER_MEMORY" /work/data || status=1
  done
  exit "$status"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi

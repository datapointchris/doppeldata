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
  (($# > 0)) || mapfile -t images <"$here/../spark-parity/spark-images.txt"
  (($# > 0)) || set -- "${images[@]}"
  # Global, because errexit drops main's locals before the EXIT trap runs.
  work=$(mktemp -d)
  trap 'rm -rf "$work"' EXIT
  uv run --project "$here/.." python "$here/write_every_fault_kind.py" "$work/data"
  uv run --project "$here/.." python "$here/read_with_duckdb.py" "$work/data" || status=1
  for image in "$@"; do
    echo "== $image" >&2
    # Hadoop's login refuses a uid with no name, so the container runs as this user under the image's own passwd plus one line.
    {
      docker run --rm --entrypoint cat "$image" /etc/passwd
      echo "reader:x:$(id -u):$(id -g):reader:/tmp:/bin/bash"
    } >"$work/passwd"
    docker run --rm --memory "$CONTAINER_MEMORY" --user "$(id -u):$(id -g)" -e HOME=/tmp -v "$work/passwd:/etc/passwd:ro" \
      -v "$here:/readers:ro" -v "$work/data:/work/data:ro" --entrypoint bash "$image" -c \
      '"${SPARK_HOME:?}/bin/spark-submit" --master "$1" --driver-memory "$2" --driver-java-options -Duser.home=/tmp \
        --conf spark.jars.ivy=/tmp/.ivy2 --conf spark.sql.session.timeZone=UTC --conf spark.ui.enabled=false \
        /readers/read_with_spark.py /work/data' _ "$SPARK_MASTER" "$DRIVER_MEMORY" || status=1
  done
  exit "$status"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi

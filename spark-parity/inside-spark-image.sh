#!/usr/bin/env bash
# Runs inside a Spark image for a parity check: a Python 3.11 with pyarrow, pandas and the wheel, then SCRIPT through
# spark-submit. PySpark's mapInArrow needs pandas. An image whose python3.11 has pyarrow keeps its own, as the Glue images
# do. Any other gets the PINNED versions from uv. EXTRA_PACKAGES names any other requirement the script has.
#
#   inside-spark-image.sh WHEEL SPARK_MASTER DRIVER_MEMORY SCRIPT [ARG...]
set -euo pipefail

# The pyarrow and pandas the Apache images' results in docs/generating-inside-spark.md were measured with.
readonly PINNED=(pyarrow==26.0.0 pandas==3.1.0)

main() {
  local wheel=$1 master=$2 driver_memory=$3 script=$4 python=/tmp/venv/bin/python
  shift 4
  local extra=()
  read -r -a extra <<<"${EXTRA_PACKAGES:-}"
  if python3.11 -c 'import pyarrow' 2>/dev/null; then
    /opt/uv venv --quiet --python "$(command -v python3.11)" --system-site-packages /tmp/venv
    /opt/uv pip install --quiet --python "$python" "$wheel" "${extra[@]}"
  else
    /opt/uv venv --quiet --python 3.11 /tmp/venv
    /opt/uv pip install --quiet --python "$python" "$wheel" "${PINNED[@]}" "${extra[@]}"
  fi
  "$python" -c 'import platform, pyarrow; print("Python", platform.python_version(), "pyarrow", pyarrow.__version__)' >&2
  "${SPARK_HOME:?}/bin/spark-submit" --version 2>&1 | grep -m1 -o 'version [0-9.]*' >&2
  # The Glue image's launcher resets PYSPARK_PYTHON, and these settings outrank it.
  "${SPARK_HOME}/bin/spark-submit" --master "$master" --driver-memory "$driver_memory" \
    --conf "spark.pyspark.python=$python" --conf "spark.pyspark.driver.python=$python" \
    --driver-java-options -Duser.home=/tmp --conf spark.jars.ivy=/tmp/.ivy2 \
    --conf spark.sql.session.timeZone=UTC --conf spark.ui.enabled=false \
    "$script" "$@"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi

#!/usr/bin/env bash
# Runs read_with_spark.py beside this script through the image's own spark-submit, inside a Spark image.
#
#   read-in-spark-image.sh MASTER DRIVER_MEMORY DATA
set -euo pipefail

exec "${SPARK_HOME:?}/bin/spark-submit" --master "$1" --driver-memory "$2" --driver-java-options -Duser.home=/tmp \
  --conf spark.jars.ivy=/tmp/.ivy2 --conf spark.sql.session.timeZone=UTC --conf spark.ui.enabled=false \
  "$(dirname "${BASH_SOURCE[0]}")/read_with_spark.py" "$3"

# shellcheck shell=bash
# Sourced by each script that runs a Spark image. Hadoop's login refuses a uid with no name, so the container runs as the
# caller, under the image's own passwd plus one line naming the caller.
#
#   run_spark_image_as_caller WORK IMAGE DOCKER-RUN-ARG...
#
# It writes WORK/passwd, then calls docker run with the DOCKER-RUN-ARGs, which name IMAGE again where docker run takes it.

run_spark_image_as_caller() {
  local work=$1 image=$2
  shift 2
  {
    docker run --rm --entrypoint cat "$image" /etc/passwd
    echo "caller:x:$(id -u):$(id -g):caller:/tmp:/bin/bash"
  } >"$work/passwd"
  docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp -v "$work/passwd:/etc/passwd:ro" "$@"
}

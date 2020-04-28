#!/bin/bash

SCRIPT_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source "${SCRIPT_HOME}/target/assemblyname.sh"

SPARK_HOME="${SPARK_HOME:-/opt/spark}"

"${SPARK_HOME}/bin/spark-submit" \
  --class "org.yupana.examples.spark.queryrunner.QueryRunner" \
  --master "local" \
  --properties-file "${SCRIPT_HOME}/query-runner-app.conf" \
  "$JARFILE" \
  "$@"

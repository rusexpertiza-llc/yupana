#!/bin/sh

SCRIPT_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source "${SCRIPT_HOME}/target/scala-2.12/assemblyname.sh"

SPARK_HOME="${SPARK_HOME:-/opt/spark}"

"${SPARK_HOME}/bin/spark-submit" \
  --class "org.yupana.examples.spark.etl.ETL" \
  --master "local[4]" \
  --properties-file "${SCRIPT_HOME}/etl-app.conf" \
  "$JARFILE" \
  "$@"

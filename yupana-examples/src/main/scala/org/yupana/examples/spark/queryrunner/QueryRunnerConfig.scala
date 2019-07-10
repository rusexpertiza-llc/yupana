package org.yupana.examples.spark.queryrunner

import org.apache.spark.SparkConf
import org.yupana.spark.Config

class QueryRunnerConfig(sparkConf: SparkConf) extends Config(sparkConf) {
  val query: String = sparkConf.get("query-runner.query")
  val outPath: String = sparkConf.get("query-runner.output", "/tmp/query-runner-result")
}

package org.yupana.spark

import org.apache.spark.SparkConf

class EtlConfig(sparkConf: SparkConf) extends Config(sparkConf) {
  val loadInvertedIndex: Boolean = sparkConf.getBoolean("tsd.etl.load-inverted-index", defaultValue = true)
}

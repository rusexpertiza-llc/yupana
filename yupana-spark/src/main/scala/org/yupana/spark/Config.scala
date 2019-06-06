package org.yupana.spark

import java.util.Properties

import org.apache.spark.SparkConf

class Config(@transient val sparkConf: SparkConf) extends Serializable {

  val hbaseZookeeper: String = sparkConf.get("hbase.zookeeper")
  val hbaseTimeout: Int = sparkConf.getInt("analytics.tsdb.rollup-job.hbase.timeout", 900000) // 15 minutes
  val hbaseNamespace: String = sparkConf.getOption("tsdb.hbase.namespace").getOrElse("default")

  val addHdfsToConfiguration: Boolean = sparkConf.getBoolean("analytics.jobs.add-hdfs-to-configuration", defaultValue = false)

  val extractBatchSize: Int = sparkConf.getInt("analytics.tsdb.extract-batch-size", 10000)

  val properties: Properties = propsWithPrefix("")

  protected def propsWithPrefix(prefix: String): Properties = sparkConf.getAllWithPrefix(prefix)
    .foldLeft(new Properties) { case (_props, (k, v)) => _props.put(prefix + k, v); _props }
}

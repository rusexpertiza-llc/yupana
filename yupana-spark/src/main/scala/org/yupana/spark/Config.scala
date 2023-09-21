/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yupana.spark

import org.apache.hadoop.hbase.io.compress.Compression.Algorithm

import org.apache.spark.SparkConf
import org.yupana.core.TsdbConfig

class Config(@transient val sparkConf: SparkConf) extends TsdbConfig with Serializable {

  val settings: SparkConfSettings = SparkConfSettings(sparkConf)

  val hbaseZookeeper: String = sparkConf.get("hbase.zookeeper")
  val hbaseTimeout: Int = sparkConf.getInt("analytics.tsdb.rollup-job.hbase.timeout", 900000) // 15 minutes
  val hbaseNamespace: String = sparkConf.getOption("tsdb.hbase.namespace").getOrElse("default")

  val addHdfsToConfiguration: Boolean =
    sparkConf.getBoolean("analytics.jobs.add-hdfs-to-configuration", defaultValue = false)

  override val extractBatchSize: Int = sparkConf.getInt("analytics.tsdb.extract-batch-size", 10000)

  override val putBatchSize: Int = sparkConf.getInt("analytics.tsdb.put-batch-size", 1000)

  val rowKeyBatchSize: Int = sparkConf.getInt("analytics.tsdb.row-key-batch-size", 50000)

  override val collectMetrics: Boolean = sparkConf.getBoolean("analytics.tsdb.collect-metrics", defaultValue = true)

  override val metricsUpdateInterval: Int = sparkConf.getInt("analytics.tsdb.metrics-update-interval", 30000)

  override val putEnabled: Boolean = false

  override val maxRegions: Int = sparkConf.getInt("spark.hbase.regions.initial.max", 50)

  override val compression: String = sparkConf.getOption("tsdb.hbase.compression").getOrElse(Algorithm.SNAPPY.getName)

  override val reduceLimit: Int = Int.MaxValue

  override val needCheckSchema: Boolean = true

  val minHBaseScanPartitions: Int = sparkConf.getInt("analytics.tsdb.spark.min-hbase-scan-partitions", 50)
}

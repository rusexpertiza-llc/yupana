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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ Mutation, Result => HResult }
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{ TableMapReduceUtil, TableOutputFormat }
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.{ Job, OutputFormat }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.yupana.api.query.DataPoint
import org.yupana.api.schema.{ Dimension, Schema }
import org.yupana.core.MapReducible
import org.yupana.core.dao.{ DictionaryProvider, TSDao }
import org.yupana.core.model.UpdateInterval
import org.yupana.core.utils.metric.MetricQueryCollector
import org.yupana.hbase._

class TsDaoHBaseSpark(
    @transient val sparkContext: SparkContext,
    override val schema: Schema,
    config: Config,
    override val dictionaryProvider: DictionaryProvider
) extends TSDaoHBaseBase[RDD]
    with TSDao[RDD, Long]
    with Serializable {

  override def mapReduceEngine(metricQueryCollector: MetricQueryCollector): MapReducible[RDD] = {
    new RddMapReducible(sparkContext, metricQueryCollector)
  }

  override def executeScans(
      queryContext: InternalQueryContext,
      from: Long,
      to: Long,
      rangeScanDims: Iterator[Map[Dimension, Seq[_]]]
  ): RDD[HResult] = {
    if (rangeScanDims.nonEmpty) {
      val rdds = rangeScanDims.map { dimIds =>
        new HBaseScanRDD(sparkContext, config, queryContext, from, to, dimIds)
      }
      sparkContext.union(rdds.toSeq)
    } else {
      sparkContext.emptyRDD[HResult]
    }
  }

  override def put(dataPoints: RDD[DataPoint], username: String): RDD[UpdateInterval] = {
    val hbaseConf = TsDaoHBaseSpark.hbaseConfiguration(config)

    val table = dataPoints.first().table

    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, HBaseUtils.tableNameString(config.hbaseNamespace, table))
    hbaseConf.setClass(
      "mapreduce.job.outputformat.class",
      classOf[TableOutputFormat[String]],
      classOf[OutputFormat[String, Mutation]]
    )
    hbaseConf.set("mapreduce.output.fileoutputformat.outputdir", "/tmp")

    val job: Job = Job.getInstance(hbaseConf, "TsdbRollup-write")
    TableMapReduceUtil.initCredentials(job)

    val jconf = new JobConf(job.getConfiguration)
    SparkConfUtils.addCredentials(jconf)

    val puts = dataPoints
      .mapPartitions { partition =>
        partition.grouped(10000).flatMap { dataPoints =>
          HBaseUtils.createPuts(dataPoints, dictionaryProvider).head._2
        }
      }
      .cache()
    puts.map(p => new ImmutableBytesWritable() -> p).saveAsNewAPIHadoopDataset(job.getConfiguration)
    puts.mapPartitions(prt =>
      prt.grouped(10000).flatMap(pts => ChangelogDaoHBase.createUpdatesIntervals(table, username, pts))
    )
  }
}

object TsDaoHBaseSpark {
  def hbaseConfiguration(config: Config): Configuration = {
    val configuration = new Configuration()
    configuration.set("hbase.zookeeper.quorum", config.hbaseZookeeper)
    configuration.set("hbase.client.scanner.timeout.period", config.hbaseTimeout.toString)
    if (config.addHdfsToConfiguration) {
      HdfsFileUtils.addHdfsPathToConfiguration(configuration, config.properties)
    }
    configuration
  }
}

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
import org.apache.hadoop.hbase.client.{ Connection, ConnectionFactory, Result => HResult }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.yupana.api.query.DataPoint
import org.yupana.api.schema.{ Dimension, Schema, Table }
import org.yupana.core.MapReducible
import org.yupana.core.dao.{ DictionaryProvider, TSDao }
import org.yupana.core.model.{ BatchDataset, UpdateInterval }
import org.yupana.core.utils.metric.MetricQueryCollector
import org.yupana.hbase.HBaseUtils.{ doPutBatch, doPutBatchDataset }
import org.yupana.hbase._

class TsDaoHBaseSpark(
    @transient val sparkContext: SparkContext,
    override val schema: Schema,
    config: Config,
    override val dictionaryProvider: DictionaryProvider
) extends TSDaoHBaseBase[RDD]
    with TSDao[RDD, Long]
    with Serializable {

  override val putBatchSize: Int = config.putBatchSize
  override val extractBatchSize: Int = config.extractBatchSize

  private val sparkListener = new ProgressListener[HBaseScanPartition]
  sparkContext.addSparkListener(sparkListener)

  override def mapReduceEngine(metricQueryCollector: MetricQueryCollector): MapReducible[RDD] = {
    new RddMapReducible(sparkContext, metricQueryCollector)
  }

  override def executeScans(
      queryContext: InternalQueryContext,
      intervals: Seq[(Long, Long)],
      rangeScanDims: Iterator[Map[Dimension, Seq[_]]]
  ): RDD[HResult] = {
    val progressFile = queryContext.hints.collectFirst { case ProgressHint(fileName) => fileName }
    if (rangeScanDims.nonEmpty) {
      val rdds = rangeScanDims.zipWithIndex.flatMap {
        case (dimIds, index) =>
          intervals.map {
            case (from, to) =>
              val listener = progressFile match {
                case Some(f) =>
                  new RddProgressListenerImpl[HBaseScanPartition](
                    s"${f}_${from}-${to}_${index}",
                    new HBaseScanPartition.HBaseScanPartitionStorable(from, to, queryContext, dimIds),
                    config.settings
                  )
                case None => new DummyProgressListener[HBaseScanPartition]
              }
              sparkListener.addListener(listener)
              new HBaseScanRDD(sparkContext, config, queryContext, from, to, dimIds, listener)
          }
      }
      sparkContext.union(rdds.toSeq)
    } else {
      sparkContext.emptyRDD[HResult]
    }
  }

  override def put(
      mr: MapReducible[RDD],
      dataPoints: RDD[DataPoint],
      username: String
  ): RDD[UpdateInterval] = {
    mr.batchFlatMap(dataPoints, putBatchSize) { dataPointsBatch =>
      doPutBatch(connection, dictionaryProvider, config.hbaseNamespace, username, dataPointsBatch)
    }

  }

  override def putDataset(
      mr: MapReducible[RDD],
      tables: Seq[Table],
      dataset: RDD[BatchDataset],
      username: String
  ): RDD[UpdateInterval] = {
    mr.flatMap(dataset) { batch =>
      tables.flatMap(table =>
        doPutBatchDataset(connection, dictionaryProvider, config.hbaseNamespace, username, batch, table)
      )
    }
  }

  override def putBatch(table: Table, batch: BatchDataset, username: String): Seq[UpdateInterval] = {
    doPutBatchDataset(connection, dictionaryProvider, config.hbaseNamespace, username, batch, table)
  }

  @transient lazy val connection: Connection = {
    TsDaoHBaseSpark.executorHBaseConnection match {
      case None =>
        val c = ConnectionFactory.createConnection(TsDaoHBaseSpark.hbaseConfiguration(config))
        TsDaoHBaseSpark.executorHBaseConnection = Some(c)
        c
      case Some(c) => c
    }
  }

}

object TsDaoHBaseSpark {
  def hbaseConfiguration(config: Config): Configuration = {
    val configuration = new Configuration()
    configuration.set("hbase.zookeeper.quorum", config.hbaseZookeeper)
    configuration.set("hbase.client.scanner.timeout.period", config.hbaseTimeout.toString)
    if (config.addHdfsToConfiguration) {
      HdfsFileUtils.addHdfsPathToConfiguration(configuration, config.settings)
    }
    configuration
  }

  var executorHBaseConnection: Option[Connection] = None
}

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

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Partition, SparkContext, TaskContext }
import org.yupana.api.schema.Dimension
import org.yupana.hbase.{ HBaseUtils, InternalQueryContext, TSDOutputRow }

case class HBaseScanPartition(
    override val index: Int,
    startKey: Array[Byte],
    endKey: Array[Byte],
    fromTime: Long,
    toTime: Long,
    queryContext: InternalQueryContext,
    rangeScanDimsIds: Map[Dimension, Seq[Long]]
) extends Partition

class HBaseScanRDD(
    sc: SparkContext,
    config: Config,
    queryContext: InternalQueryContext,
    fromTime: Long,
    toTime: Long,
    rangeScanDimsIds: Map[Dimension, Seq[Long]]
) extends RDD[TSDOutputRow[Long]](sc, Nil) {

  override protected def getPartitions: Array[Partition] = {
    println(s"getPartitions: $fromTime - $toTime")
    val regionLocator = connection().getRegionLocator(hTableName())
    val keys = regionLocator.getStartEndKeys

    val regions = keys.getFirst.zip(keys.getSecond)

    val baseTimeList = HBaseUtils.baseTimeList(fromTime, toTime, queryContext.table)

    val partitions = regions
    /*.filter {
        case (startKey, endKey) =>
          baseTimeList.exists { time =>
            val t1 = Bytes.toBytes(time)
            val t2 = Bytes.toBytes(time + 1)

            (Bytes.compareTo(t1, endKey) <= 0 || endKey.isEmpty) && (Bytes.compareTo(t2, startKey) >= 0 || startKey.isEmpty)
          }
      }*/
    .zipWithIndex
      .map {
        case ((startKey, endKey), index) =>
          HBaseScanPartition(index, startKey, endKey, fromTime, toTime, queryContext, rangeScanDimsIds)
      }

    partitions.asInstanceOf[Array[Partition]]
  }

  override def compute(split: Partition, context: TaskContext): Iterator[TSDOutputRow[Long]] = {
    val partition = split.asInstanceOf[HBaseScanPartition]
    println(s"compute: ${partition.fromTime} - ${partition.toTime}")

    val scan = queryContext.metricsCollector.createScans.measure(1) {
      val filter =
        HBaseUtils.multiRowRangeFilter(
          partition.queryContext.table,
          fromTime,
          toTime,
          partition.rangeScanDimsIds
        )

      HBaseUtils.createScan(
        partition.queryContext,
        filter,
        Seq.empty,
        fromTime,
        toTime,
        Some(partition.startKey)/*,
        Some(partition.endKey)*/
      )
    }

    HBaseUtils.executeScan(connection(), config.hbaseNamespace, scan, partition.queryContext, config.extractBatchSize)
  }

  private def connection() = {
    val hbaseConfig = TsDaoHBaseSpark.hbaseConfiguration(config)
    ConnectionFactory.createConnection(hbaseConfig)
  }

  private def hTableName() = {
    HBaseUtils.tableName(config.hbaseNamespace, queryContext.table)
  }
}

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
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Partition, SparkContext, TaskContext }
import org.yupana.api.schema.Dimension
import org.yupana.hbase.{ HBaseUtils, InternalQueryContext, TSDOutputRow }

case class HBaseScanPartition(
    override val index: Int,
    fromTime: Long,
    toTime: Long,
    queryContext: InternalQueryContext,
    rangeScanDimsIds: Map[Dimension, Seq[Long]],
    regionRanges: Seq[(Array[Byte], Array[Byte])]
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
    val regionLocator = connection().getRegionLocator(hTableName())
    val keys = regionLocator.getStartEndKeys

    val regions = keys.getFirst.zip(keys.getSecond)
    println(s"regions: ${regions.length}")

    val baseTimeList = HBaseUtils.baseTimeList(fromTime, toTime, queryContext.table)

    val ranges = HBaseUtils
      .getRanges(queryContext.table, fromTime, toTime, rangeScanDimsIds)
      .map(range => (range.getStartRow, range.getStopRow))

    val regionsFilteredByTime = regions
      .filter {
        case (startKey, endKey) =>
          baseTimeList.exists { time =>
            val t1 = Bytes.toBytes(time)
            val t2 = Bytes.toBytes(time + 1)

            (Bytes.compareTo(t1, endKey) <= 0 || endKey.isEmpty) && (Bytes.compareTo(t2, startKey) >= 0 || startKey.isEmpty)
          }
      }

    println(s"regionsFilteredByTime: ${regionsFilteredByTime.length}")

    val regionsWithRanges = regionsFilteredByTime
      .map {
        case (regionStartKey, regionEndKey) =>
          val regionRanges = HBaseUtils.getIntersectedIntervals(regionStartKey, regionEndKey, ranges)
          if (regionRanges.nonEmpty) {
            println(
              s"${regionStartKey.mkString("[", ",", "]")}   -   ${regionEndKey.mkString("[", ",", "]")}:    ${regionRanges.size}"
            )
          }
          regionRanges
      }
      .filter(_.nonEmpty)
    println(s"regionsWithRanges: ${regionsWithRanges.length}")

    val partitions = regionsWithRanges.zipWithIndex
      .map {
        case (regionRanges, index) =>
          HBaseScanPartition(index, fromTime, toTime, queryContext, rangeScanDimsIds, regionRanges)
      }

    partitions.asInstanceOf[Array[Partition]]
  }

  override def compute(split: Partition, context: TaskContext): Iterator[TSDOutputRow[Long]] = {
    val partition = split.asInstanceOf[HBaseScanPartition]

    val scan = queryContext.metricsCollector.createScans.measure(1) {

      val rowRanges = partition.regionRanges.map {
        case (startRow, stopRow) =>
          new RowRange(startRow, true, stopRow, false)
      }

      val filter = HBaseUtils.multiRowRangeFilter(rowRanges)

      HBaseUtils.createScan(
        partition.queryContext,
        filter,
        Seq.empty,
        fromTime,
        toTime
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

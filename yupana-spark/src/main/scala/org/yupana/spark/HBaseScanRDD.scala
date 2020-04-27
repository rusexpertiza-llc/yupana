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
) extends Partition {
  override def toString: String =
    s"HBaseScanPartition(index: $index, " +
      s"startKey: ${startKey.mkString("[", ",", "]")}," +
      s"endKey: ${endKey.mkString("[", ",", "]")}," +
      s"fromTime: $fromTime," +
      s"toTime: $toTime," +
      s"rangeScanDimsIds: $rangeScanDimsIds)"
}

class HBaseScanRDD(
    sc: SparkContext,
    config: Config,
    queryContext: InternalQueryContext,
    fromTime: Long,
    toTime: Long,
    rangeScanDimsIds: Map[Dimension, Seq[Long]]
) extends RDD[TSDOutputRow[Long]](sc, Nil) {

  def asBytes(a: Array[Int]): Array[Byte] = a.map(_.toByte)

  override protected def getPartitions: Array[Partition] = {
    println(s"getPartitions: $fromTime - $toTime")
    val regionLocator = connection().getRegionLocator(hTableName())
    val keys = regionLocator.getStartEndKeys

    val regions = keys.getFirst.zip(keys.getSecond)

    val baseTimeList = HBaseUtils.baseTimeList(fromTime, toTime, queryContext.table)

    val timeFilteredRegions = regions
    /*val timeFilteredRegions = Array(
      (
        asBytes(
          Array(0, 0, 1, 112, 34, 24, -112, 0, 0, 0, 0, 0, 0, 4, 78, 23, 3, -15, -28, -113, 0, 5, 91, 85, 0, 0, 0, 0, 0,
            0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 10)
        ),
        asBytes(
          Array(0, 0, 1, 112, 34, 24, -112, 0, 0, 0, 0, 0, 0, 5, -31, -83, 2, -80, 44, 78, 0, 6, -102, 93, 0, 0, 0, 0,
            0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2)
        )
      ),
      (
        asBytes(
          Array(0, 0, 1, 112, -68, -105, 88, 0, 0, 0, 0, 0, 0, 5, -111, -67, 11, -97, -112, -110, 0, 3, 32, 68, 0, 0, 0,
            0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 20)
        ),
        asBytes(
          Array(0, 0, 1, 112, -68, -105, 88, 0, 0, 0, 0, 0, 0, 7, 104, -79, 13, -34, -68, 119, 40, -117, -24, 41, 0, 0,
            0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 12)
        )
      )
    )*/
    /*val timeFilteredRegions = regions
    .filter {
        case (startKey, endKey) =>
          baseTimeList.exists { time =>
            val t1 = Bytes.toBytes(time)
            val t2 = Bytes.toBytes(time + 1)

            (Bytes.compareTo(t1, endKey) <= 0 || endKey.isEmpty) && (Bytes.compareTo(t2, startKey) >= 0 || startKey.isEmpty)
          }
      }*/
    println(s"timeFilteredRegions:")
    timeFilteredRegions.foreach {
      case (regionStart, regionEnd) =>
        println(s"${regionStart.mkString("[", ",", "]")}    -     ${regionEnd.mkString("[", ",", "]")}")
    }

    val partitions = timeFilteredRegions.zipWithIndex
      .map {
        case ((startKey, endKey), index) =>
          HBaseScanPartition(index, startKey, endKey, fromTime, toTime, queryContext, rangeScanDimsIds)
      }

    println("partitions:")
    partitions.foreach(println)

    /*val partitions = Array(
      HBaseScanPartition(0, Array.empty, Array.empty, fromTime, toTime, queryContext, rangeScanDimsIds)
    )*/

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
        Some(partition.startKey),
        Some(partition.endKey)
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

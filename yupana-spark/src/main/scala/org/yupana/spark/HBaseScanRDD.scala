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

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ Connection, ConnectionFactory, Result => HBaseResult }
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Partition, SparkContext, TaskContext }
import org.yupana.api.schema.Dimension
import org.yupana.core.utils.CloseableIterator
import org.yupana.hbase.{ HBaseUtils, InternalQueryContext }

import scala.annotation.tailrec
import scala.util.Using

class HBaseScanRDD(
    sc: SparkContext,
    config: SparkHBaseTsdbConfig,
    queryContext: InternalQueryContext,
    fromTime: Long,
    toTime: Long,
    rangeScanDimsIds: Map[Dimension, Seq[_]],
    listener: RddProgressListener[HBaseScanPartition]
) extends RDD[HBaseResult](sc, Nil) {

  override protected def getPartitions: Array[Partition] = {
    val regions = Using.resource(createConnection()) { connection =>
      val tableName = hTableName()

      Using.resource(connection.getRegionLocator(tableName)) { regionLocator =>
        val keys = regionLocator.getStartEndKeys
        val firstKey = HBaseUtils.getFirstKey(connection, tableName)
        val lastKey = Bytes.unsignedCopyAndIncrement(HBaseUtils.getLastKey(connection, tableName))

        keys.getFirst()(0) = firstKey
        keys.getSecond()(keys.getSecond.length - 1) = lastKey

        keys.getFirst.zip(keys.getSecond)
      }
    }

    val baseTimeList = HBaseUtils.baseTimeList(fromTime, toTime, queryContext.table)

    val regionsRequested = regions.filter {
      case (startKey, endKey) =>
        baseTimeList.exists { time =>
          val t1 = Bytes.toBytes(time)
          val t2 = Bytes.toBytes(time + 1)

          (Bytes
            .compareTo(t1, endKey) <= 0 || endKey.isEmpty) && (Bytes.compareTo(t2, startKey) >= 0 || startKey.isEmpty)
        }
    }

    val partitions = HBaseScanRDD
      .splitRanges(config.minHBaseScanPartitions, regionsRequested)
      .zipWithIndex
      .map {
        case ((startKey, endKey), index) =>
          HBaseScanPartition(index, startKey, endKey, fromTime, toTime, queryContext, rangeScanDimsIds)
      }

    listener
      .transformPartitions(partitions.toSeq)
      .toArray[Partition]
  }

  override def compute(split: Partition, context: TaskContext): Iterator[HBaseResult] = {
    val partition = split.asInstanceOf[HBaseScanPartition]
    val scan = queryContext.metricsCollector.createScans.measure(1) {
      val filter =
        HBaseUtils.multiRowRangeFilter(
          partition.queryContext.table,
          Seq(partition.fromTime -> partition.toTime),
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

    scan match {
      case Some(s) =>
        val connection = createConnection()
        CloseableIterator(
          HBaseUtils.executeScan(connection, config.hbaseNamespace, s, partition.queryContext, config.extractBatchSize),
          connection.close()
        )

      case None => Iterator.empty
    }
  }

  private def createConnection(): Connection = {
    val hbaseConfig = TsDaoHBaseSpark.hbaseConfiguration(config)
    ConnectionFactory.createConnection(hbaseConfig)
  }

  private def hTableName(): TableName = {
    HBaseUtils.tableName(config.hbaseNamespace, queryContext.table)
  }
}

object HBaseScanRDD {

  private def bisect(range: (Array[Byte], Array[Byte])): Array[(Array[Byte], Array[Byte])] = {
    Bytes
      .split(range._1, range._2, 1)
      .sliding(2, 1)
      .map { a => (a(0), a(1)) }
      .toArray
  }

  @tailrec
  def splitRanges(parts: Int, rs: Array[(Array[Byte], Array[Byte])]): Array[(Array[Byte], Array[Byte])] = {
    if (rs.length >= parts) rs
    else {
      val bisectedRanges = rs.flatMap(bisect)
      splitRanges(parts, bisectedRanges)
    }
  }
}

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

package org.yupana.hbase

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.{ Connection, Get, Put, Result, Scan }
import org.apache.hadoop.hbase.util.Bytes
import org.yupana.api.query.DataPoint
import org.yupana.api.schema.{ Dimension, Table }
import org.yupana.core.MapReducible
import org.yupana.core.dao.{ DictionaryProvider, TSDao }
import org.yupana.core.utils.metric.MetricQueryCollector
import org.yupana.hbase.HBaseUtils._

import scala.collection.JavaConverters._

class TSDaoHBase(
    connection: Connection,
    namespace: String,
    override val dictionaryProvider: DictionaryProvider,
    putsBatchSize: Int = 1000
) extends TSDaoHBaseBase[Iterator]
    with TSDao[Iterator, Long] {

  override def mapReduceEngine(metricQueryCollector: MetricQueryCollector): MapReducible[Iterator] =
    MapReducible.iteratorMR

  override def executeScans(
      queryContext: InternalQueryContext,
      from: IdType,
      to: IdType,
      rangeScanDims: Iterator[Map[Dimension, Seq[IdType]]]
  ): Iterator[Result] = {

    if (rangeScanDims.nonEmpty) {
      rangeScanDims.flatMap { dimIds =>
        val scan = queryContext.metricsCollector.createScans.measure(1) {
          val filter = multiRowRangeFilter(queryContext.table, from, to, dimIds)
          createScan(queryContext, filter, Seq.empty, from, to)
        }
        executeScan(connection, namespace, scan, queryContext, EXTRACT_BATCH_SIZE)
      }
    } else {
      Iterator.empty
    }
  }

  override def put(dataPoints: Seq[DataPoint]): Unit = {
    logger.trace(s"Put ${dataPoints.size} dataPoints to tsdb")
    logger.trace(s" -- DETAIL DATAPOINTS: \r\n ${dataPoints.mkString("\r\n")}")

    createPutOperations(dataPoints, dictionaryProvider).foreach {
      case (table, puts) =>
        val hbaseTable = connection.getTable(tableName(namespace, table))
        puts
          .grouped(putsBatchSize)
          .foreach(putsBatch => hbaseTable.put(putsBatch.asJava))
        logger.trace(s" -- DETAIL PUTS IN TABLE ${table.name}: \r\n ${puts.mkString("\r\n")}")
    }
  }

  override def getRollupStatuses(fromTime: Long, toTime: Long, table: Table): Seq[(Long, String)] = {
    checkRollupStatusFamilyExistsElseCreate(connection, namespace, table)
    val hbaseTable = connection.getTable(tableName(namespace, table))
    val scan = new Scan()
      .addColumn(rollupStatusFamily, rollupStatusField)
      .setStartRow(Bytes.toBytes(fromTime))
      .setStopRow(Bytes.toBytes(toTime))
    val scanner = hbaseTable.getScanner(scan)
    val statuses = scanner.asScala.toIterator.flatMap { result =>
      val time = Bytes.toLong(result.getRow)
      val value = Option(result.getValue(rollupStatusFamily, rollupStatusField)).map(Bytes.toString)
      value.map(v => time -> v)
    }.toSeq
    statuses
  }

  override def putRollupStatuses(statuses: Seq[(Long, String)], table: Table): Unit = {
    checkRollupStatusFamilyExistsElseCreate(connection, namespace, table)
    val hbaseTable = connection.getTable(tableName(namespace, table))
    val puts = statuses.map(status =>
      new Put(Bytes.toBytes(status._1))
        .addColumn(rollupStatusFamily, rollupStatusField, Bytes.toBytes(status._2))
    )
    hbaseTable.put(puts.asJava)
  }

  override def checkAndPutRollupStatus(
      time: Long,
      oldStatus: Option[String],
      newStatus: String,
      table: Table
  ): Boolean = {
    checkRollupStatusFamilyExistsElseCreate(connection, namespace, table)
    val hbaseTable = connection.getTable(tableName(namespace, table))
    hbaseTable.checkAndPut(
      Bytes.toBytes(time),
      rollupStatusFamily,
      rollupStatusField,
      oldStatus.map(_.getBytes).orNull,
      new Put(Bytes.toBytes(time)).addColumn(rollupStatusFamily, rollupStatusField, Bytes.toBytes(newStatus))
    )
  }

  override def getRollupSpecialField(fieldName: String, table: Table): Option[Long] = {
    checkRollupStatusFamilyExistsElseCreate(connection, namespace, table)
    val hbaseTable = connection.getTable(tableName(namespace, table))
    val get = new Get(rollupSpecialKey).addColumn(rollupStatusFamily, fieldName.getBytes)
    val res = hbaseTable.get(get)
    val cell = Option(res.getColumnLatestCell(rollupStatusFamily, fieldName.getBytes))
    cell.map(c => Bytes.toLong(CellUtil.cloneValue(c)))
  }

  override def putRollupSpecialField(fieldName: String, value: Long, table: Table): Unit = {
    checkRollupStatusFamilyExistsElseCreate(connection, namespace, table)
    val hbaseTable = connection.getTable(tableName(namespace, table))
    val put: Put = new Put(rollupSpecialKey).addColumn(rollupStatusFamily, fieldName.getBytes, Bytes.toBytes(value))
    hbaseTable.put(put)
  }

}

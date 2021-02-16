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

import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{ Table => HTable, _ }
import org.apache.hadoop.hbase.filter.{ CompareFilter, Filter, FilterList, SingleColumnValueFilter }
import org.apache.hadoop.hbase.util.Bytes
import org.joda.time.Interval
import org.yupana.api.schema.Table
import org.yupana.api.utils.ResourceUtils.using
import org.yupana.core.dao.RollupMetaDao
import org.yupana.core.model.UpdateInterval
import org.yupana.core.model.UpdateInterval._
import org.yupana.hbase.HBaseUtils._
import org.yupana.hbase.RollupMetaDaoHBase._

import scala.collection.JavaConverters._

object RollupMetaDaoHBase {
  val TABLE_NAME: String = "ts_updates_intervals"
  val FAMILY: Array[Byte] = Bytes.toBytes("f")
  val INVALIDATED_FLAG_QUALIFIER: Array[Byte] = Bytes.toBytes(invalidatedFlagColumn)
  val ROLLUP_TIME_QUALIFIER: Array[Byte] = Bytes.toBytes(rollupTimeColumn)
  val FROM_QUALIFIER: Array[Byte] = Bytes.toBytes(fromColumn)
  val TO_QUALIFIER: Array[Byte] = Bytes.toBytes(toColumn)
  val TABLE_QUALIFIER: Array[Byte] = Bytes.toBytes(tableColumn)

  def getTableName(namespace: String): TableName = TableName.valueOf(namespace, TABLE_NAME)
}

class RollupMetaDaoHBase(connection: Connection, namespace: String) extends RollupMetaDao with StrictLogging {

  override def putUpdatesIntervals(tableName: String, intervals: Seq[UpdateInterval]): Unit = withTables {
    using(getTable) { table =>
      val puts = intervals.map { period =>
        val put = new Put(Bytes.toBytes(period.from))
        put
          .addColumn(FAMILY, FROM_QUALIFIER, Bytes.toBytes(period.from))
          .addColumn(FAMILY, TO_QUALIFIER, Bytes.toBytes(period.to))
          .addColumn(FAMILY, INVALIDATED_FLAG_QUALIFIER, Bytes.toBytes(period.rollupTime.isEmpty))
          .addColumn(FAMILY, TABLE_QUALIFIER, Bytes.toBytes(tableName))
        period.rollupTime.foreach { rollupTime =>
          put.addColumn(FAMILY, ROLLUP_TIME_QUALIFIER, Bytes.toBytes(rollupTime))
        }
        put
      }
      table.put(puts.asJava)
    }
  }

  override def getUpdatesIntervals(tableName: String, rollupIntervalOpt: Option[Interval]): Iterable[UpdateInterval] =
    withTables {
      val updatesIntervals = using(getTable) { table =>
        val scan = new Scan().addFamily(FAMILY)
        val tableFilter = new SingleColumnValueFilter(
          FAMILY,
          TABLE_QUALIFIER,
          CompareFilter.CompareOp.EQUAL,
          Bytes.toBytes(tableName)
        )
        val filterList = rollupIntervalOpt match {
          case Some(rollupInterval) =>
            val filterFrom = new SingleColumnValueFilter(
              FAMILY,
              ROLLUP_TIME_QUALIFIER,
              CompareFilter.CompareOp.GREATER_OR_EQUAL,
              Bytes.toBytes(rollupInterval.getStartMillis)
            )

            val filterTo = new SingleColumnValueFilter(
              FAMILY,
              ROLLUP_TIME_QUALIFIER,
              CompareFilter.CompareOp.LESS_OR_EQUAL,
              Bytes.toBytes(rollupInterval.getEndMillis)
            )

            new FilterList(
              List[Filter](tableFilter, filterFrom, filterTo).asJava
            )
          case None =>
            val invalidatedFilter = new SingleColumnValueFilter(
              FAMILY,
              INVALIDATED_FLAG_QUALIFIER,
              CompareFilter.CompareOp.EQUAL,
              Bytes.toBytes(true)
            )
            new FilterList(
              List[Filter](tableFilter, invalidatedFilter).asJava
            )
        }
        scan.setFilter(filterList)
        table.getScanner(scan).asScala.map(toUpdateInterval)
      }
      updatesIntervals
    }

  private def toUpdateInterval(result: Result): UpdateInterval = {
    val rollupTimeValue =
      if (result.containsColumn(FAMILY, ROLLUP_TIME_QUALIFIER))
        Some(Bytes.toLong(result.getValue(FAMILY, ROLLUP_TIME_QUALIFIER)))
      else
        None
    UpdateInterval(
      from = Bytes.toLong(result.getRow),
      to = Bytes.toLong(result.getValue(FAMILY, TO_QUALIFIER)),
      rollupTime = rollupTimeValue
    )
  }

  override def getRollupStatuses(fromTime: Long, toTime: Long, table: Table): Seq[(Long, String)] = {
    checkRollupStatusFamilyExistsElseCreate(connection, namespace, table)
    using(connection.getTable(tableName(namespace, table))) { hbaseTable =>
      val scan = new Scan()
        .addColumn(rollupStatusFamily, rollupStatusField)
        .setStartRow(Bytes.toBytes(fromTime))
        .setStopRow(Bytes.toBytes(toTime))
      using(hbaseTable.getScanner(scan)) { scanner =>
        scanner.asScala.toIterator.flatMap { result =>
          val time = Bytes.toLong(result.getRow)
          val value = Option(result.getValue(rollupStatusFamily, rollupStatusField)).map(Bytes.toString)
          value.map(v => time -> v)
        }.toSeq
      }
    }
  }

  override def putRollupStatuses(statuses: Seq[(Long, String)], table: Table): Unit = {
    if (statuses.nonEmpty) {
      checkRollupStatusFamilyExistsElseCreate(connection, namespace, table)
      using(connection.getTable(tableName(namespace, table))) { hbaseTable =>
        val puts = statuses.map(status =>
          new Put(Bytes.toBytes(status._1))
            .addColumn(rollupStatusFamily, rollupStatusField, Bytes.toBytes(status._2))
        )
        hbaseTable.put(puts.asJava)
      }
    }
  }

  override def checkAndPutRollupStatus(
      time: Long,
      oldStatus: Option[String],
      newStatus: String,
      table: Table
  ): Boolean = {
    checkRollupStatusFamilyExistsElseCreate(connection, namespace, table)
    using(connection.getTable(tableName(namespace, table))) { hbaseTable =>
      hbaseTable.checkAndPut(
        Bytes.toBytes(time),
        rollupStatusFamily,
        rollupStatusField,
        oldStatus.map(_.getBytes).orNull,
        new Put(Bytes.toBytes(time)).addColumn(rollupStatusFamily, rollupStatusField, Bytes.toBytes(newStatus))
      )
    }
  }

  override def getRollupSpecialField(fieldName: String, table: Table): Option[Long] = {
    checkRollupStatusFamilyExistsElseCreate(connection, namespace, table)
    using(connection.getTable(tableName(namespace, table))) { hbaseTable =>
      val get = new Get(rollupSpecialKey).addColumn(rollupStatusFamily, fieldName.getBytes)
      val res = hbaseTable.get(get)
      val cell = Option(res.getColumnLatestCell(rollupStatusFamily, fieldName.getBytes))
      cell.map(c => Bytes.toLong(CellUtil.cloneValue(c)))
    }
  }

  override def putRollupSpecialField(fieldName: String, value: Long, table: Table): Unit = {
    checkRollupStatusFamilyExistsElseCreate(connection, namespace, table)
    using(connection.getTable(tableName(namespace, table))) { hbaseTable =>
      val put: Put = new Put(rollupSpecialKey).addColumn(rollupStatusFamily, fieldName.getBytes, Bytes.toBytes(value))
      hbaseTable.put(put)
    }
  }

  def withTables[T](block: => T): T = {
    checkTablesExistsElseCreate()
    block
  }

  private def getTable: HTable = connection.getTable(getTableName(namespace))

  private def checkTablesExistsElseCreate(): Unit = {
    try {
      val tableName = getTableName(namespace)
      using(connection.getAdmin) { admin =>
        if (!admin.tableExists(tableName)) {
          val desc = new HTableDescriptor(tableName)
            .addFamily(new HColumnDescriptor(FAMILY))
          admin.createTable(desc)
        }
      }
    } catch {
      case _: TableExistsException =>
    }
  }
}

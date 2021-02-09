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
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.client.{ Table => HTable }
import org.apache.hadoop.hbase.filter.{ CompareFilter, Filter, FilterList, SingleColumnValueFilter }
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ CellUtil, HColumnDescriptor, HTableDescriptor, TableExistsException, TableName }
import org.joda.time.{ DateTime, Interval, LocalDateTime }
import org.yupana.api.schema.Table
import org.yupana.api.utils.ResourceUtils.using
import org.yupana.core.dao.RollupMetaDao
import org.yupana.core.model.InvalidPeriod
import org.yupana.core.model.InvalidPeriod._
import org.yupana.hbase.HBaseUtils.{
  checkRollupStatusFamilyExistsElseCreate,
  rollupSpecialKey,
  rollupStatusFamily,
  rollupStatusField,
  tableName
}
import org.yupana.hbase.RollupMetaDaoHBase._

import scala.collection.JavaConverters._

object RollupMetaDaoHBase {
  val TABLE_NAME: String = "ts_invalid_periods"
  val FAMILY: Array[Byte] = Bytes.toBytes("f")
  val ROLLUP_TIME_QUALIFIER: Array[Byte] = Bytes.toBytes(rollupTimeColumn)
  val FROM_QUALIFIER: Array[Byte] = Bytes.toBytes(fromColumn)
  val TO_QUALIFIER: Array[Byte] = Bytes.toBytes(toColumn)

  def getTableName(namespace: String): TableName = TableName.valueOf(namespace, TABLE_NAME)
}

class RollupMetaDaoHBase(connection: Connection, namespace: String) extends RollupMetaDao with StrictLogging {

  override def putInvalidPeriods(intervals: Seq[Interval]): Unit = withTables {
    val rollupTime = DateTime.now()
    using(getTable) { table =>
      val puts = intervals.map { interval =>
        val put = new Put(Bytes.toBytes(System.nanoTime()))
        put.addColumn(FAMILY, ROLLUP_TIME_QUALIFIER, Bytes.toBytes(rollupTime.getMillis))
        put.addColumn(FAMILY, FROM_QUALIFIER, Bytes.toBytes(interval.getStartMillis))
        put.addColumn(FAMILY, TO_QUALIFIER, Bytes.toBytes(interval.getEndMillis))
        put
      }
      table.put(puts.asJava)
    }
  }

  override def getInvalidPeriods(rollupDateFrom: LocalDateTime, rollupDateTo: LocalDateTime): Iterable[InvalidPeriod] =
    withTables {
      val invalidPeriods = using(getTable) { table =>
        val scan = new Scan().addFamily(FAMILY)
        val filterList = new FilterList(
          List[Filter](
            new SingleColumnValueFilter(
              FAMILY,
              ROLLUP_TIME_QUALIFIER,
              CompareFilter.CompareOp.GREATER_OR_EQUAL,
              Bytes.toBytes(rollupDateFrom.toDateTime.getMillis)
            ),
            new SingleColumnValueFilter(
              FAMILY,
              ROLLUP_TIME_QUALIFIER,
              CompareFilter.CompareOp.LESS_OR_EQUAL,
              Bytes.toBytes(rollupDateTo.toDateTime.getMillis)
            )
          ).asJava
        )
        scan.setFilter(filterList)
        table.getScanner(scan).asScala.map(toInvalidPeriod)
      }
      invalidPeriods
    }

  private def toInvalidPeriod(result: Result): InvalidPeriod = {
    InvalidPeriod(
      rollupTime = new DateTime(Bytes.toLong(result.getValue(FAMILY, ROLLUP_TIME_QUALIFIER))),
      from = new DateTime(Bytes.toLong(result.getValue(FAMILY, FROM_QUALIFIER))),
      to = new DateTime(Bytes.toLong(result.getValue(FAMILY, TO_QUALIFIER)))
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

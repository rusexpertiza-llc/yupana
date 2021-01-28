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
import org.apache.hadoop.hbase.filter.{ CompareFilter, Filter, FilterList, SingleColumnValueFilter }
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ HColumnDescriptor, HTableDescriptor, TableExistsException, TableName }
import org.joda.time.{ DateTime, Interval, LocalDateTime }
import org.yupana.api.utils.ResourceUtils.using
import org.yupana.core.dao.InvalidPeriodsDao
import org.yupana.core.model.InvalidPeriod
import org.yupana.core.model.InvalidPeriod._
import org.yupana.hbase.InvalidPeriodsDaoHBase._

import scala.collection.JavaConverters._

object InvalidPeriodsDaoHBase {
  val TABLE_NAME: String = "ts_invalid_periods"
  val FAMILY: Array[Byte] = Bytes.toBytes("f")
  val ROLLUP_TIME_QUALIFIER: Array[Byte] = Bytes.toBytes(rollupTimeColumn)
  val FROM_QUALIFIER: Array[Byte] = Bytes.toBytes(fromColumn)
  val TO_QUALIFIER: Array[Byte] = Bytes.toBytes(toColumn)

  def getTableName(namespace: String): TableName = TableName.valueOf(namespace, TABLE_NAME)
}

class InvalidPeriodsDaoHBase(connection: Connection, namespace: String) extends InvalidPeriodsDao with StrictLogging {

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

  def withTables[T](block: => T): T = {
    checkTablesExistsElseCreate()
    block
  }

  private def getTable: Table = connection.getTable(getTableName(namespace))

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

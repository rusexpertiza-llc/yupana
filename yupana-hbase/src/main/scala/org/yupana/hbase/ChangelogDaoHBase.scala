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

import java.nio.charset.StandardCharsets
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{ Table => HTable, _ }
import org.apache.hadoop.hbase.filter.{ FilterList, SingleColumnValueFilter }
import org.apache.hadoop.hbase.util.Bytes
import org.joda.time.DateTime
import org.yupana.api.utils.ResourceUtils.using
import org.yupana.core.dao.ChangelogDao
import org.yupana.core.model.UpdateInterval
import org.yupana.core.model.UpdateInterval._
import org.yupana.hbase.ChangelogDaoHBase._

import scala.jdk.CollectionConverters._

object ChangelogDaoHBase {

  val TABLE_NAME: String = "ts_updates_intervals"
  val FAMILY: Array[Byte] = Bytes.toBytes("f")
  val UPDATED_AT_QUALIFIER: Array[Byte] = Bytes.toBytes(updatedAtColumn)
  val UPDATED_BY_QUALIFIER: Array[Byte] = Bytes.toBytes(updatedByColumn)
  val FROM_QUALIFIER: Array[Byte] = Bytes.toBytes(fromColumn)
  val TO_QUALIFIER: Array[Byte] = Bytes.toBytes(toColumn)
  val TABLE_QUALIFIER: Array[Byte] = Bytes.toBytes(tableColumn)

  val UPDATER_UNKNOWN = "UNKNOWN"

  def getTableName(namespace: String): TableName = TableName.valueOf(namespace, TABLE_NAME)

  def createChangelogPut(updateInterval: UpdateInterval): Put = {
    val rowKey = Bytes.toBytes(updateInterval.table) ++
      Bytes.toBytes(updateInterval.from.getMillis) ++
      Bytes.toBytes(updateInterval.to.getMillis)
    val put = new Put(rowKey)
    put.addColumn(FAMILY, FROM_QUALIFIER, Bytes.toBytes(updateInterval.from.getMillis))
    put.addColumn(FAMILY, TO_QUALIFIER, Bytes.toBytes(updateInterval.to.getMillis))
    put.addColumn(FAMILY, TABLE_QUALIFIER, Bytes.toBytes(updateInterval.table))
    put.addColumn(FAMILY, UPDATED_AT_QUALIFIER, Bytes.toBytes(updateInterval.updatedAt.getMillis))
    put.addColumn(FAMILY, UPDATED_BY_QUALIFIER, updateInterval.updatedBy.getBytes(StandardCharsets.UTF_8))
    put
  }
}

class ChangelogDaoHBase(connection: Connection, namespace: String) extends ChangelogDao with StrictLogging {

  override def putUpdatesIntervals(intervals: Seq[UpdateInterval]): Unit = withTables {
    using(getTable) { table =>
      val puts = intervals.map(ChangelogDaoHBase.createChangelogPut)
      table.put(puts.asJava)
    }
  }

  override def getUpdatesIntervals(
      tableName: Option[String],
      updatedAfter: Option[Long],
      updatedBefore: Option[Long],
      updatedBy: Option[String]
  ): Iterable[UpdateInterval] =
    withTables {
      val updatesIntervals = using(getTable) { table =>
        val scan = new Scan().addFamily(FAMILY)
        val filterList = new FilterList()

        tableName.foreach(t =>
          filterList.addFilter(
            new SingleColumnValueFilter(
              FAMILY,
              TABLE_QUALIFIER,
              CompareOperator.EQUAL,
              Bytes.toBytes(t)
            )
          )
        )

        updatedAfter.foreach(ua =>
          filterList.addFilter(
            new SingleColumnValueFilter(
              FAMILY,
              UPDATED_AT_QUALIFIER,
              CompareOperator.GREATER_OR_EQUAL,
              Bytes.toBytes(ua)
            )
          )
        )

        updatedBefore.foreach(ub =>
          filterList.addFilter(
            new SingleColumnValueFilter(
              FAMILY,
              UPDATED_AT_QUALIFIER,
              CompareOperator.LESS_OR_EQUAL,
              Bytes.toBytes(ub)
            )
          )
        )

        updatedBy.foreach(ub =>
          filterList.addFilter(
            new SingleColumnValueFilter(
              FAMILY,
              UPDATED_BY_QUALIFIER,
              CompareOperator.EQUAL,
              ub.getBytes(StandardCharsets.UTF_8)
            )
          )
        )

        scan.setFilter(filterList)
        table.getScanner(scan).asScala.map(toUpdateInterval)
      }
      updatesIntervals
    }

  private def toUpdateInterval(result: Result): UpdateInterval = {
    val byBytes = result.getValue(FAMILY, UPDATED_BY_QUALIFIER)
    val by = if (byBytes != null) new String(byBytes, StandardCharsets.UTF_8) else ChangelogDaoHBase.UPDATER_UNKNOWN
    UpdateInterval(
      table = new String(result.getValue(FAMILY, TABLE_QUALIFIER), StandardCharsets.UTF_8),
      from = new DateTime(Bytes.toLong(result.getValue(FAMILY, FROM_QUALIFIER))),
      to = new DateTime(Bytes.toLong(result.getValue(FAMILY, TO_QUALIFIER))),
      updatedAt = new DateTime(Bytes.toLong(result.getValue(FAMILY, UPDATED_AT_QUALIFIER))),
      updatedBy = by
    )
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
          val desc = TableDescriptorBuilder
            .newBuilder(tableName)
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
            .build()
          admin.createTable(desc)
        }
      }
    } catch {
      case _: TableExistsException =>
    }
  }
}

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
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.FilterList.Operator
import org.apache.hadoop.hbase.filter.{ Filter, FilterList, SingleColumnValueFilter }
import org.apache.hadoop.hbase.util.Bytes
import org.yupana.core.dao.ChangelogDao
import org.yupana.core.model.UpdateInterval
import org.yupana.core.model.UpdateInterval._
import org.yupana.hbase.ChangelogDaoHBase._

import java.time.{ Instant, OffsetDateTime, ZoneOffset }
import scala.jdk.CollectionConverters._
import scala.util.Using

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
    val fromBytes = Bytes.toBytes(updateInterval.from.toInstant.toEpochMilli)
    val toBytes = Bytes.toBytes(updateInterval.to.toInstant.toEpochMilli)
    val rowKey = Bytes.toBytes(updateInterval.table) ++ fromBytes ++ toBytes

    val put = new Put(rowKey)
    put.addColumn(FAMILY, FROM_QUALIFIER, fromBytes)
    put.addColumn(FAMILY, TO_QUALIFIER, toBytes)
    put.addColumn(FAMILY, TABLE_QUALIFIER, Bytes.toBytes(updateInterval.table))
    put.addColumn(FAMILY, UPDATED_AT_QUALIFIER, Bytes.toBytes(updateInterval.updatedAt.toInstant.toEpochMilli))
    put.addColumn(FAMILY, UPDATED_BY_QUALIFIER, updateInterval.updatedBy.getBytes(StandardCharsets.UTF_8))
    put
  }
}

class ChangelogDaoHBase(connection: Connection, namespace: String) extends ChangelogDao with StrictLogging {

  override def putUpdatesIntervals(intervals: Seq[UpdateInterval]): Unit = withTables {
    Using.resource(getTable) { table =>
      val puts = intervals.map(ChangelogDaoHBase.createChangelogPut)
      table.put(puts.asJava)
    }
  }

  private def buildFilterList(
      tableName: Option[String],
      updatedAfter: Option[Long],
      updatedBefore: Option[Long],
      recalculatedAfter: Option[Long],
      recalculatedBefore: Option[Long],
      updatedBy: Option[String]
  ): FilterList = {

    def dateRangeFilter(
        fromColumn: Array[Byte],
        toColumn: Array[Byte],
        fromOpt: Option[Long],
        toOpt: Option[Long]
    ): Option[Filter] = {
      (fromOpt, toOpt) match {
        case (Some(from), Some(to)) =>
          Some(
            new FilterList(
              Operator.MUST_PASS_ONE,
              List(
                new FilterList(
                  Operator.MUST_PASS_ALL,
                  List(
                    new SingleColumnValueFilter(
                      FAMILY,
                      fromColumn,
                      CompareOperator.GREATER_OR_EQUAL,
                      Bytes.toBytes(from)
                    ),
                    new SingleColumnValueFilter(
                      FAMILY,
                      fromColumn,
                      CompareOperator.LESS_OR_EQUAL,
                      Bytes.toBytes(to)
                    )
                  ).asInstanceOf[List[Filter]].asJava
                ),
                new FilterList(
                  Operator.MUST_PASS_ALL,
                  List(
                    new SingleColumnValueFilter(
                      FAMILY,
                      fromColumn,
                      CompareOperator.LESS_OR_EQUAL,
                      Bytes.toBytes(from)
                    ),
                    new SingleColumnValueFilter(
                      FAMILY,
                      toColumn,
                      CompareOperator.GREATER_OR_EQUAL,
                      Bytes.toBytes(from)
                    )
                  ).asInstanceOf[List[Filter]].asJava
                )
              ).asInstanceOf[List[Filter]].asJava
            )
          )
        case (Some(from), _) =>
          Some(
            new SingleColumnValueFilter(
              FAMILY,
              fromColumn,
              CompareOperator.GREATER_OR_EQUAL,
              Bytes.toBytes(from)
            )
          )
        case _ =>
          None
      }
    }

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

    dateRangeFilter(UPDATED_AT_QUALIFIER, UPDATED_AT_QUALIFIER, updatedAfter, updatedBefore).foreach(
      filterList.addFilter
    )
    dateRangeFilter(FROM_QUALIFIER, TO_QUALIFIER, recalculatedAfter, recalculatedBefore).foreach(filterList.addFilter)

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
    filterList
  }

  override def getUpdatesIntervals(
      tableName: Option[String],
      updatedAfter: Option[Long],
      updatedBefore: Option[Long],
      recalculatedAfter: Option[Long],
      recalculatedBefore: Option[Long],
      updatedBy: Option[String]
  ): Iterable[UpdateInterval] =
    withTables {
      val updatesIntervals = Using.resource(getTable) { table =>
        val scan = new Scan().addFamily(FAMILY)
        val filterList =
          buildFilterList(tableName, updatedAfter, updatedBefore, recalculatedAfter, recalculatedBefore, updatedBy)
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
      from = OffsetDateTime.ofInstant(
        Instant.ofEpochMilli(Bytes.toLong(result.getValue(FAMILY, FROM_QUALIFIER))),
        ZoneOffset.UTC
      ),
      to = OffsetDateTime.ofInstant(
        Instant.ofEpochMilli(Bytes.toLong(result.getValue(FAMILY, TO_QUALIFIER))),
        ZoneOffset.UTC
      ),
      updatedAt = OffsetDateTime.ofInstant(
        Instant.ofEpochMilli(Bytes.toLong(result.getValue(FAMILY, UPDATED_AT_QUALIFIER))),
        ZoneOffset.UTC
      ),
      updatedBy = by
    )
  }

  def withTables[T](block: => T): T = {
    HBaseUtils.checkTableExistsElseCreate(connection, getTableName(namespace), Seq(FAMILY))
    block
  }

  private def getTable: Table = connection.getTable(getTableName(namespace))
}

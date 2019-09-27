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
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ HColumnDescriptor, HTableDescriptor, TableExistsException, TableName }
import org.joda.time.LocalDateTime
import org.yupana.api.query.Query
import org.yupana.core.dao.{ QueryMetricsFilter, TsdbQueryMetricsDao }
import org.yupana.core.model.QueryStates.{ Cancelled, QueryState }
import org.yupana.core.model.{ MetricData, QueryStates, TsdbQueryMetrics }
import org.yupana.hbase.TsdbQueryMetricsDaoHBase._

import scala.collection.JavaConverters._
import org.yupana.core.model.TsdbQueryMetrics._

object TsdbQueryMetricsDaoHBase {
  val TABLE_NAME: String = "ts_query_metrics"
  val FAMILY: Array[Byte] = Bytes.toBytes("f")
  val QUERY_QUALIFIER: Array[Byte] = Bytes.toBytes(queryColumn)
  val START_DATE_QUALIFIER: Array[Byte] = Bytes.toBytes(startDateColumn)
  val TOTAL_DURATION_QUALIFIER: Array[Byte] = Bytes.toBytes(totalDurationColumn)
  val STATE_QUALIFIER: Array[Byte] = Bytes.toBytes(stateColumn)

  private val UPDATE_ATTEMPTS_LIMIT = 5

  def getTableName(namespace: String): TableName = TableName.valueOf(namespace, TABLE_NAME)
}

class TsdbQueryMetricsDaoHBase(connection: Connection, namespace: String)
    extends TsdbQueryMetricsDao
    with StrictLogging {

  override def initializeQueryMetrics(query: Query, sparkQuery: Boolean): Unit = withTables {
    val id = query.uuid
    val startDate = LocalDateTime.now()
    val queryAsString = if (sparkQuery) "SPARK - " + query.toString else query.toString
    val table = getTable
    val put = new Put(Bytes.toBytes(id))
    put.addColumn(FAMILY, QUERY_QUALIFIER, Bytes.toBytes(queryAsString))
    put.addColumn(FAMILY, START_DATE_QUALIFIER, Bytes.toBytes(startDate.toDateTime().getMillis))
    put.addColumn(FAMILY, TOTAL_DURATION_QUALIFIER, Bytes.toBytes(0.0))
    put.addColumn(FAMILY, STATE_QUALIFIER, Bytes.toBytes(QueryStates.Running.name))
    TsdbQueryMetrics.qualifiers.foreach { qualifier =>
      put.addColumn(FAMILY, Bytes.toBytes(qualifier + "_" + metricCount), Bytes.toBytes(0L))
      put.addColumn(FAMILY, Bytes.toBytes(qualifier + "_" + metricTime), Bytes.toBytes(0.0))
      put.addColumn(FAMILY, Bytes.toBytes(qualifier + "_" + metricSpeed), Bytes.toBytes(0.0))
    }
    table.put(put)
  }

  override def updateQueryMetrics(
      queryId: String,
      queryState: QueryState,
      totalDuration: Double,
      metricValues: Map[String, MetricData],
      sparkQuery: Boolean
  ): Unit = {
    def tryUpdateMetrics(n: Int): Unit = {
      if (n != UPDATE_ATTEMPTS_LIMIT) {
        if (n != 0) {
          logger.info(s"query $queryId attempt: $n")
        }
        val table = getTable
        queriesByFilter(QueryMetricsFilter(Some(queryId))).headOption match {
          case Some(currentData) =>
            if (!sparkQuery && currentData.state == Cancelled) {
              throw new IllegalStateException(s"Query $queryId was cancelled!")
            }
            val put = new Put(Bytes.toBytes(queryId))
            put.addColumn(FAMILY, TOTAL_DURATION_QUALIFIER, Bytes.toBytes(totalDuration))
            put.addColumn(FAMILY, STATE_QUALIFIER, Bytes.toBytes(queryState.name))
            metricValues.foreach {
              case (metricName, metricData) =>
                currentData.metrics.get(metricName) match {
                  case Some(oldMetricData) =>
                    put.addColumn(
                      FAMILY,
                      Bytes.toBytes(metricName + "_" + metricCount),
                      Bytes.toBytes(oldMetricData.count + metricData.count)
                    )
                    put.addColumn(
                      FAMILY,
                      Bytes.toBytes(metricName + "_" + metricTime),
                      Bytes.toBytes(oldMetricData.time + metricData.time)
                    )
                  case None =>
                    put.addColumn(
                      FAMILY,
                      Bytes.toBytes(metricName + "_" + metricCount),
                      Bytes.toBytes(metricData.count)
                    )
                    put.addColumn(FAMILY, Bytes.toBytes(metricName + "_" + metricTime), Bytes.toBytes(metricData.time))
                }
                put.addColumn(FAMILY, Bytes.toBytes(metricName + "_" + metricSpeed), Bytes.toBytes(metricData.speed))
            }
            val result = table.checkAndPut(
              Bytes.toBytes(queryId),
              FAMILY,
              TOTAL_DURATION_QUALIFIER,
              Bytes.toBytes(currentData.totalDuration),
              put
            )
            if (!result) {
              tryUpdateMetrics(n + 1)
            }
          case None =>
            throw new IllegalStateException(s"Query $queryId doesn't exists!")
        }
      } else {
        throw new IllegalStateException(s"Cannot update query $queryId with no reason")
      }
    }

    tryUpdateMetrics(0)
  }

  override def queriesByFilter(filter: QueryMetricsFilter): List[TsdbQueryMetrics] = withTables {
    val table = getTable
    filter.queryId match {
      case Some(id) =>
        val get = new Get(Bytes.toBytes(id)).addFamily(FAMILY)
        val result = table.get(get)
        if (result.isEmpty) List()
        else List(toMetric(result))
      case None =>
        val scan = new Scan().addFamily(FAMILY)
        val result = table.getScanner(scan)
        result.asScala
          .map(toMetric)
          .toList
          .sortBy(-_.startDate.toDateTime.getMillis)
          .take(filter.limit)
    }
  }

  override def setQueryState(queryId: String, queryState: QueryState): Boolean = {
    val table = getTable
    val put = new Put(Bytes.toBytes(queryId))
    put.addColumn(FAMILY, STATE_QUALIFIER, Bytes.toBytes(queryState.name))
    table.checkAndPut(Bytes.toBytes(queryId), FAMILY, STATE_QUALIFIER, Bytes.toBytes(QueryStates.Running.name), put)
  }

  private def toMetric(result: Result): TsdbQueryMetrics = {
    val metrics = TsdbQueryMetrics.qualifiers.map { qualifier =>
      qualifier -> MetricData(
        Bytes.toLong(result.getValue(FAMILY, Bytes.toBytes(qualifier + "_" + metricCount))),
        Bytes.toDouble(result.getValue(FAMILY, Bytes.toBytes(qualifier + "_" + metricTime))),
        Bytes.toDouble(result.getValue(FAMILY, Bytes.toBytes(qualifier + "_" + metricSpeed)))
      )
    }.toMap
    TsdbQueryMetrics(
      queryId = Bytes.toString(result.getRow),
      state = QueryStates.getByName(Bytes.toString(result.getValue(FAMILY, STATE_QUALIFIER))),
      query = Bytes.toString(result.getValue(FAMILY, QUERY_QUALIFIER)),
      startDate = new LocalDateTime(Bytes.toLong(result.getValue(FAMILY, START_DATE_QUALIFIER))),
      totalDuration = Bytes.toDouble(result.getValue(FAMILY, TOTAL_DURATION_QUALIFIER)),
      metrics = metrics
    )
  }

  def withTables[T](block: => T): T = {
    checkTablesExistsElseCreate()
    block
  }

  private def getTable = connection.getTable(getTableName(namespace))

  private def checkTablesExistsElseCreate(): Unit = {
    try {
      val tableName = getTableName(namespace)
      if (!connection.getAdmin.tableExists(tableName)) {
        val desc = new HTableDescriptor(tableName)
          .addFamily(new HColumnDescriptor(FAMILY))
        connection.getAdmin.createTable(desc)
      }
    } catch {
      case _: TableExistsException =>
    }
  }
}

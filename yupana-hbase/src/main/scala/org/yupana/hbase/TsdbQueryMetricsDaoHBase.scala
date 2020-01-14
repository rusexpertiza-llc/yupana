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
import org.apache.hadoop.hbase.filter.{ CompareFilter, FilterList, SingleColumnValueFilter }
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ HColumnDescriptor, HTableDescriptor, TableExistsException, TableName }
import org.joda.time.DateTime
import org.yupana.api.query.Query
import org.yupana.core.dao.{ QueryMetricsFilter, TsdbQueryMetricsDao }
import org.yupana.core.model.QueryStates.{ Cancelled, QueryState }
import org.yupana.core.model.TsdbQueryMetrics._
import org.yupana.core.model.{ MetricData, QueryStates, TsdbQueryMetrics }
import org.yupana.hbase.TsdbQueryMetricsDaoHBase._

import scala.collection.JavaConverters._

object TsdbQueryMetricsDaoHBase {
  val TABLE_NAME: String = "ts_query_metrics"
  val ID_FAMILY: Array[Byte] = Bytes.toBytes("idf")
  val FAMILY: Array[Byte] = Bytes.toBytes("f")
  val QUERY_ID_QUALIFIER: Array[Byte] = Bytes.toBytes(queryIdColumn)
  val QUERY_QUALIFIER: Array[Byte] = Bytes.toBytes(queryColumn)
  val START_DATE_QUALIFIER: Array[Byte] = Bytes.toBytes(startDateColumn)
  val TOTAL_DURATION_QUALIFIER: Array[Byte] = Bytes.toBytes(totalDurationColumn)
  val STATE_QUALIFIER: Array[Byte] = Bytes.toBytes(stateColumn)
  val ENGINE_QUALIFIER: Array[Byte] = Bytes.toBytes(engineColumn)
  val ID_QUALIFIER: Array[Byte] = Bytes.toBytes("ID")
  val RUNNING_PARTITIONS_QUALIFIER: Array[Byte] = Bytes.toBytes("runningPartitions")

  private val UPDATE_ATTEMPTS_LIMIT = 100
  private val MAX_SLEEP_TIME_BETWEEN_ATTEMPTS = 500

  def getTableName(namespace: String): TableName = TableName.valueOf(namespace, TABLE_NAME)
}

class TsdbQueryMetricsDaoHBase(connection: Connection, namespace: String)
    extends TsdbQueryMetricsDao
    with StrictLogging {

  override def initializeQueryMetrics(query: Query, sparkQuery: Boolean): Long = withTables {
    val queryMetricsId = createId()
    val startDate = DateTime.now()
    val engine = if (sparkQuery) "SPARK" else "STANDALONE"
    val table = getTable
    val put = new Put(Bytes.toBytes(queryMetricsId))
    put.addColumn(FAMILY, QUERY_ID_QUALIFIER, Bytes.toBytes(query.uuid))
    put.addColumn(FAMILY, QUERY_QUALIFIER, Bytes.toBytes(query.toString))
    put.addColumn(FAMILY, START_DATE_QUALIFIER, Bytes.toBytes(startDate.getMillis))
    put.addColumn(FAMILY, TOTAL_DURATION_QUALIFIER, Bytes.toBytes(0.0))
    put.addColumn(FAMILY, STATE_QUALIFIER, Bytes.toBytes(QueryStates.Running.name))
    put.addColumn(FAMILY, ENGINE_QUALIFIER, Bytes.toBytes(engine))
    TsdbQueryMetrics.qualifiers.foreach { qualifier =>
      put.addColumn(FAMILY, Bytes.toBytes(qualifier + "_" + metricCount), Bytes.toBytes(0L))
      put.addColumn(FAMILY, Bytes.toBytes(qualifier + "_" + metricTime), Bytes.toBytes(0.0))
      put.addColumn(FAMILY, Bytes.toBytes(qualifier + "_" + metricSpeed), Bytes.toBytes(0.0))
    }
    table.put(put)
    queryMetricsId
  }

  override def updateQueryMetrics(
      queryRowKey: Long,
      queryState: QueryState,
      totalDuration: Double,
      metricValues: Map[String, MetricData],
      sparkQuery: Boolean
  ): Unit = {
    def tryUpdateMetrics(n: Int): Unit = {
      if (n != UPDATE_ATTEMPTS_LIMIT) {
        if (n != 0) {
          logger.info(s"query $queryRowKey attempt: $n")
        }
        val table = getTable
        val filter = QueryMetricsFilter(rowKey = Some(queryRowKey))
        queriesByFilter(Some(filter), limit = Some(1)).headOption match {
          case Some(query) =>
            if (!sparkQuery && query.state == Cancelled) {
              throw new IllegalStateException(s"Query $queryRowKey was cancelled!")
            }
            val put = new Put(Bytes.toBytes(queryRowKey))
            put.addColumn(FAMILY, TOTAL_DURATION_QUALIFIER, Bytes.toBytes(totalDuration))
            put.addColumn(FAMILY, STATE_QUALIFIER, Bytes.toBytes(queryState.name))
            metricValues.foreach {
              case (metricName, metricData) =>
                query.metrics.get(metricName) match {
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
              Bytes.toBytes(queryRowKey),
              FAMILY,
              TOTAL_DURATION_QUALIFIER,
              Bytes.toBytes(query.totalDuration),
              put
            )
            if (!result) {
              Thread.sleep(util.Random.nextInt(MAX_SLEEP_TIME_BETWEEN_ATTEMPTS))
              tryUpdateMetrics(n + 1)
            }
          case None =>
            throw new IllegalStateException(s"Query $queryRowKey doesn't exists!")
        }
      } else {
        throw new IllegalStateException(
          s"Cannot update query $queryRowKey: concurrent update attempt limit $n has been reached"
        )
      }
    }

    tryUpdateMetrics(0)
  }

  override def queriesByFilter(
      filter: Option[QueryMetricsFilter],
      limit: Option[Int] = None
  ): Iterable[TsdbQueryMetrics] = withTables {
    val table = getTable
    val scan = new Scan().addFamily(FAMILY).setReversed(true)
    val queries = filter match {
      case Some(f) =>
        f.rowKey match {
          case Some(rowKey) =>
            val get = new Get(Bytes.toBytes(rowKey)).addFamily(FAMILY)
            val result = table.get(get)
            if (result.isEmpty) List()
            else List(result)
          case None =>
            val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
            f.queryId.foreach { queryId =>
              filterList.addFilter(
                new SingleColumnValueFilter(
                  FAMILY,
                  QUERY_ID_QUALIFIER,
                  CompareFilter.CompareOp.EQUAL,
                  Bytes.toBytes(queryId)
                )
              )
            }
            f.queryState.foreach { queryState =>
              filterList.addFilter(
                new SingleColumnValueFilter(
                  FAMILY,
                  STATE_QUALIFIER,
                  CompareFilter.CompareOp.EQUAL,
                  Bytes.toBytes(queryState.name)
                )
              )
            }
            if (!filterList.getFilters.isEmpty) {
              scan.setFilter(filterList)
            }
            table.getScanner(scan).asScala
        }
      case None =>
        table.getScanner(scan).asScala
    }
    limit match {
      case Some(lim) =>
        queries
          .take(lim)
          .map(toMetric)
      case None =>
        queries
          .map(toMetric)
    }
  }

  override def setQueryState(filter: QueryMetricsFilter, queryState: QueryState): Unit = {
    val table = getTable
    queriesByFilter(filter = Some(filter), limit = Some(1)).headOption match {
      case Some(query) =>
        val put = new Put(Bytes.toBytes(query.rowKey))
        put.addColumn(FAMILY, STATE_QUALIFIER, Bytes.toBytes(queryState.name))
        table.checkAndPut(
          Bytes.toBytes(query.rowKey),
          FAMILY,
          STATE_QUALIFIER,
          Bytes.toBytes(QueryStates.Running.name),
          put
        )
      case None =>
        throw new IllegalArgumentException(s"Query not found by filter $filter!")
    }
  }

  override def setRunningPartitions(queryRowKey: Long, partitions: Int): Unit = {
    val table = getTable
    val put = new Put(Bytes.toBytes(queryRowKey))
    put.addColumn(FAMILY, RUNNING_PARTITIONS_QUALIFIER, Bytes.toBytes(partitions))
    table.put(put)
  }

  def decrementRunningPartitions(queryRowKey: Long): Int = {
    decrementRunningPartitions(queryRowKey, 1)
  }

  private def decrementRunningPartitions(queryRowKey: Long, attempt: Int): Int = {
    val table = getTable

    val get = new Get(Bytes.toBytes(queryRowKey)).addColumn(FAMILY, RUNNING_PARTITIONS_QUALIFIER)
    val res = table.get(get)
    val runningPartitions = Bytes.toInt(res.getValue(FAMILY, RUNNING_PARTITIONS_QUALIFIER))

    val decrementedRunningPartitions = runningPartitions - 1

    val put = new Put(Bytes.toBytes(queryRowKey))
    put.addColumn(FAMILY, RUNNING_PARTITIONS_QUALIFIER, Bytes.toBytes(decrementedRunningPartitions))
    val successes = table.checkAndPut(
      Bytes.toBytes(queryRowKey),
      FAMILY,
      RUNNING_PARTITIONS_QUALIFIER,
      Bytes.toBytes(runningPartitions),
      put
    )
    if (successes) {
      decrementedRunningPartitions
    } else if (attempt < UPDATE_ATTEMPTS_LIMIT) {
      Thread.sleep(util.Random.nextInt(MAX_SLEEP_TIME_BETWEEN_ATTEMPTS))
      decrementRunningPartitions(queryRowKey, attempt + 1)
    } else {
      throw new IllegalStateException(
        s"Cannot decrement running partitions for $queryRowKey, concurrent update attempt limit $attempt has been reached"
      )
    }
  }

  override def deleteMetrics(filter: QueryMetricsFilter): Int = {
    val table = getTable
    var n = 0
    queriesByFilter(filter = Some(filter)).foreach { query =>
      table.delete(new Delete(Bytes.toBytes(query.rowKey)))
      n += 1
    }
    n
  }

  private def toMetric(result: Result): TsdbQueryMetrics = {
    val metrics = TsdbQueryMetrics.qualifiers.collect {
      case qualifier if result.containsColumn(FAMILY, Bytes.toBytes(qualifier + "_" + metricCount)) =>
        qualifier -> {
          MetricData(
            Bytes.toLong(result.getValue(FAMILY, Bytes.toBytes(qualifier + "_" + metricCount))),
            Bytes.toDouble(result.getValue(FAMILY, Bytes.toBytes(qualifier + "_" + metricTime))),
            Bytes.toDouble(result.getValue(FAMILY, Bytes.toBytes(qualifier + "_" + metricSpeed)))
          )
        }
    }.toMap
    TsdbQueryMetrics(
      rowKey = Bytes.toLong(result.getRow),
      queryId = Bytes.toString(result.getValue(FAMILY, QUERY_ID_QUALIFIER)),
      state = QueryStates.getByName(Bytes.toString(result.getValue(FAMILY, STATE_QUALIFIER))),
      engine = Bytes.toString(result.getValue(FAMILY, ENGINE_QUALIFIER)),
      query = Bytes.toString(result.getValue(FAMILY, QUERY_QUALIFIER)),
      startDate = new DateTime(Bytes.toLong(result.getValue(FAMILY, START_DATE_QUALIFIER))),
      totalDuration = Bytes.toDouble(result.getValue(FAMILY, TOTAL_DURATION_QUALIFIER)),
      metrics = metrics
    )
  }

  def withTables[T](block: => T): T = {
    checkTablesExistsElseCreate()
    block
  }

  private def createId(): Long = {
    checkTablesExistsElseCreate()
    getTable.incrementColumnValue(ID_QUALIFIER, ID_FAMILY, ID_QUALIFIER, 1)
  }

  private def getTable = connection.getTable(getTableName(namespace))

  private def checkTablesExistsElseCreate(): Unit = {
    try {
      val tableName = getTableName(namespace)
      if (!connection.getAdmin.tableExists(tableName)) {
        val desc = new HTableDescriptor(tableName)
          .addFamily(new HColumnDescriptor(FAMILY))
          .addFamily(new HColumnDescriptor(ID_FAMILY))
        connection.getAdmin.createTable(desc)
      }
    } catch {
      case _: TableExistsException =>
    }
  }
}

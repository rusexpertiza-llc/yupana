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
import org.yupana.api.utils.ResourceUtils.using
import org.yupana.core.dao.{ QueryMetricsFilter, TsdbQueryMetricsDao }
import org.yupana.core.model.QueryStates.{ Cancelled, QueryState }
import org.yupana.core.model.TsdbQueryMetrics._
import org.yupana.core.model.{ MetricData, QueryStates, TsdbQueryMetrics }
import org.yupana.hbase.TsdbQueryMetricsDaoHBase._

import scala.annotation.tailrec
import scala.collection.JavaConverters._

object TsdbQueryMetricsDaoHBase {
  val TABLE_NAME: String = "ts_query_metrics"
  val ID_FAMILY: Array[Byte] = Bytes.toBytes("idf")
  val FAMILY: Array[Byte] = Bytes.toBytes("f")
  val QUERY_QUALIFIER: Array[Byte] = Bytes.toBytes(queryColumn)
  val START_DATE_QUALIFIER: Array[Byte] = Bytes.toBytes(startDateColumn)
  val TOTAL_DURATION_QUALIFIER: Array[Byte] = Bytes.toBytes(totalDurationColumn)
  val STATE_QUALIFIER: Array[Byte] = Bytes.toBytes(stateColumn)
  val ENGINE_QUALIFIER: Array[Byte] = Bytes.toBytes(engineColumn)
  val RUNNING_PARTITIONS_QUALIFIER: Array[Byte] = Bytes.toBytes("runningPartitions")

  private val UPDATE_ATTEMPTS_LIMIT = 100
  private val MAX_SLEEP_TIME_BETWEEN_ATTEMPTS = 500

  def getTableName(namespace: String): TableName = TableName.valueOf(namespace, TABLE_NAME)
}

class TsdbQueryMetricsDaoHBase(connection: Connection, namespace: String)
    extends TsdbQueryMetricsDao
    with StrictLogging {

  override def initializeQueryMetrics(query: Query, sparkQuery: Boolean): Unit = withTables {
    val startDate = DateTime.now()
    val engine = if (sparkQuery) "SPARK" else "STANDALONE"
    using(getTable) { table =>
      val put = new Put(Bytes.toBytes(query.id))
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
    }
  }

  override def updateQueryMetrics(
      queryId: String,
      queryState: QueryState,
      totalDuration: Double,
      metricValues: Map[String, MetricData],
      sparkQuery: Boolean
  ): Unit = {

    @tailrec
    def tryUpdateMetrics(n: Int): Unit = {
      if (n != UPDATE_ATTEMPTS_LIMIT) {
        if (n != 0) {
          logger.info(s"query $queryId attempt: $n")
        }
        val filter = QueryMetricsFilter(queryId = Some(queryId))
        queriesByFilter(Some(filter), limit = Some(1)).headOption match {
          case Some(query) =>
            if (!sparkQuery && query.state == Cancelled) {
              throw new IllegalStateException(s"Query $queryId was cancelled!")
            }
            val put = new Put(Bytes.toBytes(queryId))
            put.addColumn(FAMILY, TOTAL_DURATION_QUALIFIER, Bytes.toBytes(totalDuration))
            put.addColumn(FAMILY, STATE_QUALIFIER, Bytes.toBytes(queryState.name))
            metricValues.foreach {
              case (metricName, metricData) =>
                query.metrics.get(metricName) match {
                  case Some(oldMetricData) =>
                    val (count, time, speed) = if (metricData.count != 0L) {
                      (
                        oldMetricData.count + metricData.count,
                        oldMetricData.time + metricData.time,
                        metricData.speed
                      )
                    } else (oldMetricData.count, oldMetricData.time, oldMetricData.speed)

                    put.addColumn(
                      FAMILY,
                      Bytes.toBytes(metricName + "_" + metricCount),
                      Bytes.toBytes(count)
                    )
                    put.addColumn(
                      FAMILY,
                      Bytes.toBytes(metricName + "_" + metricTime),
                      Bytes.toBytes(time)
                    )
                    put.addColumn(
                      FAMILY,
                      Bytes.toBytes(metricName + "_" + metricSpeed),
                      Bytes.toBytes(speed)
                    )
                  case None =>
                    put.addColumn(
                      FAMILY,
                      Bytes.toBytes(metricName + "_" + metricCount),
                      Bytes.toBytes(metricData.count)
                    )
                    put.addColumn(FAMILY, Bytes.toBytes(metricName + "_" + metricTime), Bytes.toBytes(metricData.time))
                    put.addColumn(
                      FAMILY,
                      Bytes.toBytes(metricName + "_" + metricSpeed),
                      Bytes.toBytes(metricData.speed)
                    )
                }
            }
            val result = using(getTable) {
              _.checkAndPut(
                Bytes.toBytes(queryId),
                FAMILY,
                TOTAL_DURATION_QUALIFIER,
                Bytes.toBytes(query.totalDuration),
                put
              )
            }
            if (!result) {
              Thread.sleep(util.Random.nextInt(MAX_SLEEP_TIME_BETWEEN_ATTEMPTS))
              tryUpdateMetrics(n + 1)
            }
          case None =>
            throw new IllegalStateException(s"Query $queryId doesn't exists!")
        }
      } else {
        throw new IllegalStateException(
          s"Cannot update query $queryId: concurrent update attempt limit $n has been reached"
        )
      }
    }

    tryUpdateMetrics(0)
  }

  override def queriesByFilter(
      filter: Option[QueryMetricsFilter],
      limit: Option[Int] = None
  ): Iterable[TsdbQueryMetrics] = withTables {
    val queries = using(getTable) { table =>
      val scan = new Scan().addFamily(FAMILY).setReversed(true)
      filter match {
        case Some(f) =>
          f.queryId match {
            case Some(queryId) =>
              val get = new Get(Bytes.toBytes(queryId)).addFamily(FAMILY)
              val result = table.get(get)
              if (result.isEmpty) List()
              else List(result)
            case None =>
              val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
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
              using(table.getScanner(scan))(_.asScala.toList)
          }
        case None =>
          using(table.getScanner(scan))(_.asScala.toList)
      }
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
        val put = new Put(Bytes.toBytes(query.queryId))
        put.addColumn(FAMILY, STATE_QUALIFIER, Bytes.toBytes(queryState.name))
        table.checkAndPut(
          Bytes.toBytes(query.queryId),
          FAMILY,
          STATE_QUALIFIER,
          Bytes.toBytes(QueryStates.Running.name),
          put
        )
      case None =>
        throw new IllegalArgumentException(s"Query not found by filter $filter!")
    }
  }

  override def setRunningPartitions(queryId: String, partitions: Int): Unit = {
    val table = getTable
    val put = new Put(Bytes.toBytes(queryId))
    put.addColumn(FAMILY, RUNNING_PARTITIONS_QUALIFIER, Bytes.toBytes(partitions))
    table.put(put)
  }

  def decrementRunningPartitions(queryId: String): Int = {
    decrementRunningPartitions(queryId, 1)
  }

  @tailrec
  private def decrementRunningPartitions(queryId: String, attempt: Int): Int = {
    val table = getTable

    val get = new Get(Bytes.toBytes(queryId)).addColumn(FAMILY, RUNNING_PARTITIONS_QUALIFIER)
    val res = table.get(get)
    val runningPartitions = Bytes.toInt(res.getValue(FAMILY, RUNNING_PARTITIONS_QUALIFIER))

    val decrementedRunningPartitions = runningPartitions - 1

    val put = new Put(Bytes.toBytes(queryId))
    put.addColumn(FAMILY, RUNNING_PARTITIONS_QUALIFIER, Bytes.toBytes(decrementedRunningPartitions))
    val successes = table.checkAndPut(
      Bytes.toBytes(queryId),
      FAMILY,
      RUNNING_PARTITIONS_QUALIFIER,
      Bytes.toBytes(runningPartitions),
      put
    )
    if (successes) {
      decrementedRunningPartitions
    } else if (attempt < UPDATE_ATTEMPTS_LIMIT) {
      Thread.sleep(util.Random.nextInt(MAX_SLEEP_TIME_BETWEEN_ATTEMPTS))
      decrementRunningPartitions(queryId, attempt + 1)
    } else {
      throw new IllegalStateException(
        s"Cannot decrement running partitions for $queryId, concurrent update attempt limit $attempt has been reached"
      )
    }
  }

  override def deleteMetrics(filter: QueryMetricsFilter): Int = {
    val table = getTable
    var n = 0
    queriesByFilter(filter = Some(filter)).foreach { query =>
      table.delete(new Delete(Bytes.toBytes(query.queryId)))
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
      queryId = Bytes.toString(result.getRow),
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

  private def getTable: Table = connection.getTable(getTableName(namespace))

  private def checkTablesExistsElseCreate(): Unit = {
    try {
      val tableName = getTableName(namespace)
      using(connection.getAdmin) { admin =>
        if (!admin.tableExists(tableName)) {
          val desc = new HTableDescriptor(tableName)
            .addFamily(new HColumnDescriptor(FAMILY))
            .addFamily(new HColumnDescriptor(ID_FAMILY))
          admin.createTable(desc)
        }
      }
    } catch {
      case _: TableExistsException =>
    }
  }
}

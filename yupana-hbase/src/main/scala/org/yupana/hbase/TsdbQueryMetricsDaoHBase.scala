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
import org.apache.hadoop.hbase.filter.{ FilterList, SingleColumnValueFilter }
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ CompareOperator, TableExistsException, TableName }
import org.joda.time.DateTime
import org.yupana.api.query.Query
import org.yupana.api.utils.ResourceUtils.using
import org.yupana.core.dao.{ QueryMetricsFilter, TsdbQueryMetricsDao }
import org.yupana.core.model.QueryStates.QueryState
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

  override def saveQueryMetrics(
      query: Query,
      partitionId: Option[String],
      startDate: Long,
      queryState: QueryState,
      totalDuration: Double,
      metricValues: Map[String, MetricData],
      sparkQuery: Boolean
  ): Unit = withTables {

    val key = rowKey(query.id, partitionId)
    val engine = if (sparkQuery) "SPARK" else "STANDALONE"

    val put = new Put(key)
    put.addColumn(FAMILY, QUERY_QUALIFIER, Bytes.toBytes(query.toString))
    put.addColumn(FAMILY, TOTAL_DURATION_QUALIFIER, Bytes.toBytes(totalDuration))
    put.addColumn(FAMILY, STATE_QUALIFIER, Bytes.toBytes(queryState.name))
    put.addColumn(FAMILY, START_DATE_QUALIFIER, Bytes.toBytes(startDate))
    put.addColumn(FAMILY, ENGINE_QUALIFIER, Bytes.toBytes(engine))
    TsdbQueryMetrics.qualifiers.foreach { metricName =>
      val (count, time, speed) = metricValues.get(metricName) match {
        case Some(data) => (data.count, data.time, data.speed)
        case None       => (0L, 0d, 0d)
      }

      put.addColumn(FAMILY, Bytes.toBytes(metricName + "_" + metricCount), Bytes.toBytes(count))
      put.addColumn(FAMILY, Bytes.toBytes(metricName + "_" + metricTime), Bytes.toBytes(time))
      put.addColumn(FAMILY, Bytes.toBytes(metricName + "_" + metricSpeed), Bytes.toBytes(speed))

    }
    using(getTable)(_.put(put))
  }

  override def queriesByFilter(
      filter: Option[QueryMetricsFilter],
      limit: Option[Int] = None
  ): Iterable[TsdbQueryMetrics] = withTables {
    def setFilters(scan: Scan): Unit = {
      val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
      filter.foreach { f =>
        f.queryState.foreach { queryState =>
          filterList.addFilter(
            new SingleColumnValueFilter(
              FAMILY,
              STATE_QUALIFIER,
              CompareOperator.EQUAL,
              Bytes.toBytes(queryState.name)
            )
          )
        }
      }
      if (!filterList.getFilters.isEmpty) {
        scan.setFilter(filterList)
      }
    }

    val results = using(getTable) { table =>
      filter match {
        case Some(f) =>
          (f.queryId, f.partitionId) match {
            case (Some(queryId), Some(pId)) =>
              val get = new Get(rowKey(queryId, f.partitionId)).addFamily(FAMILY)
              val result = table.get(get)
              if (result.isEmpty) List()
              else List(result)

            case (Some(queryId), None) =>
              val scan = new Scan().setRowPrefixFilter(Bytes.toBytes(queryId + "_")).addFamily(FAMILY).setReversed(true)
              setFilters(scan)
              using(table.getScanner(scan))(_.asScala.toList)

            case _ =>
              val scan = new Scan().addFamily(FAMILY).setReversed(true)
              setFilters(scan)
              using(table.getScanner(scan))(_.asScala.toList)
          }
        case None =>
          val scan = new Scan().addFamily(FAMILY).setReversed(true)
          using(table.getScanner(scan))(_.asScala.toList)
      }
    }
    limit match {
      case Some(lim) =>
        results
          .take(lim)
          .map(toMetric)
      case None =>
        results
          .map(toMetric)
    }
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
    val successes = table
      .checkAndMutate(
        CheckAndMutate
          .newBuilder(Bytes.toBytes(queryId))
          .ifEquals(FAMILY, RUNNING_PARTITIONS_QUALIFIER, Bytes.toBytes(runningPartitions))
          .build(put)
      )
      .isSuccess

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
    val (qId, pId) = parseKey(result.getRow)
    TsdbQueryMetrics(
      queryId = qId,
      partitionId = pId,
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

  private def rowKey(queryId: String, partitionId: Option[String]): Array[Byte] = {
    val key = partitionId.map(x => s"${queryId}_$partitionId").getOrElse(queryId)
    Bytes.toBytes(key)
  }

  private def parseKey(bytes: Array[Byte]): (String, Option[String]) = {
    val strKey = Bytes.toString(bytes)
    val splitIndex = strKey.indexOf('_')
    (strKey.substring(0, splitIndex), if (splitIndex != -1) Some(strKey.substring(splitIndex + 1)) else None)
  }

  private def checkTablesExistsElseCreate(): Unit = {
    try {
      val tableName = getTableName(namespace)
      using(connection.getAdmin) { admin =>
        if (!admin.tableExists(tableName)) {
          val desc = TableDescriptorBuilder
            .newBuilder(tableName)
            .setColumnFamilies(
              Seq(ColumnFamilyDescriptorBuilder.of(FAMILY), ColumnFamilyDescriptorBuilder.of(ID_FAMILY)).asJavaCollection
            )
            .build()
          admin.createTable(desc)
        }
      }
    } catch {
      case _: TableExistsException =>
    }
  }
}

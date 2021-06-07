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

package org.yupana.benchmarks

import org.joda.time.LocalDateTime
import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query.syntax.All._
import org.yupana.api.query._
import org.yupana.api.schema.Dimension
import org.yupana.core.dao._
import org.yupana.core.model._
import org.yupana.core.utils.metric.MetricQueryCollector
import org.yupana.core.{ MapReducible, QueryContext, SimpleTsdbConfig, TSDB }
import org.yupana.schema.{ Dimensions, ItemTableMetrics, SchemaRegistry, Tables }

object BenchTsdb {
  val query: Query = Query(
    table = Tables.itemsKkmTable,
    from = const(Time(LocalDateTime.now().minusDays(1))),
    to = const(Time(LocalDateTime.now())),
    fields = Seq(
      time as "time",
      dimension(Dimensions.ITEM) as "item",
      sum(metric(ItemTableMetrics.quantityField)) as "total_quantity",
      metric(ItemTableMetrics.sumField) as "total_sum",
      divFrac(
        sum(divFrac(double2bigDecimal(metric(ItemTableMetrics.quantityField)), metric(ItemTableMetrics.sumField))),
        long2BigDecimal(count(dimension(Dimensions.ITEM)))
      ) as "avg",
      distinctCount(dimension(Dimensions.KKM_ID)) as "kkm_count"
    ),
    filter = Some(in(dimension(Dimensions.KKM_ID), (1 to 1000).toSet)),
    groupBy = Seq(time, dimension(Dimensions.ITEM))
  )

  val postCondition: Option[Condition] = None
  val queryContext: QueryContext = QueryContext(query, postCondition)
  private val rowBuilder = new InternalRowBuilder(queryContext)

  def getRows(n: Int): Seq[InternalRow] = {
    val qtime = new LocalDateTime(2021, 5, 24, 22, 40, 0)

    (1 to n).map { i =>
      val time = qtime.minusHours(i % 100)

      rowBuilder
        .set(TimeExpr, Time(time))
        .set(MetricExpr(ItemTableMetrics.sumField), BigDecimal(i.toDouble / 1000))
        .set(MetricExpr(ItemTableMetrics.quantityField), 101d - i.toDouble / 10000)
        .set(DimensionExpr(Dimensions.ITEM), s"The thing #${(n - i) % 1000}")
        .set(DimensionExpr(Dimensions.KKM_ID), i % 500)
        .buildAndReset()
    }
  }

  class BenchTsdb
      extends TSDB(
        SchemaRegistry.defaultSchema,
        new BenchDao(1000000),
        new BenchMetricDao,
        new DictionaryProviderImpl(new BenchDictDao),
        identity,
        SimpleTsdbConfig()
      ) {}

  class BenchDao(n: Int) extends TSDao[Iterator, Long] {

    private val rows = BenchTsdb.getRows(n)

    override def query(
        query: InternalQuery,
        valueDataBuilder: InternalRowBuilder,
        metricCollector: MetricQueryCollector
    ): Iterator[InternalRow] = rows.iterator

    override def mapReduceEngine(metricQueryCollector: MetricQueryCollector): MapReducible[Iterator] =
      MapReducible.iteratorMR

    override def isSupportedCondition(condition: Condition): Boolean = false

    override def put(dataPoints: Seq[DataPoint]): Unit = throw new UnsupportedOperationException("Put is not supported")
  }

  class BenchDictDao extends DictionaryDao {
    private def unsupported = throw new UnsupportedOperationException("Dictionaries are not supported")

    override def createSeqId(dimension: Dimension): Int = unsupported
    override def getIdByValue(dimension: Dimension, value: String): Option[Long] = unsupported
    override def getIdsByValues(dimension: Dimension, value: Set[String]): Map[String, Long] = unsupported
    override def checkAndPut(dimension: Dimension, id: Long, value: String): Boolean = unsupported
  }

  class BenchMetricDao extends TsdbQueryMetricsDao {
    override def initializeQueryMetrics(query: Query, sparkQuery: Boolean): Unit = {}
    override def queriesByFilter(filter: Option[QueryMetricsFilter], limit: Option[Int]): Iterable[TsdbQueryMetrics] =
      Nil

    override def updateQueryMetrics(
        queryId: String,
        queryState: QueryStates.QueryState,
        totalDuration: Double,
        metricValues: Map[String, MetricData],
        sparkQuery: Boolean
    ): Unit = {}

    override def setQueryState(filter: QueryMetricsFilter, queryState: QueryStates.QueryState): Unit = {}
    override def setRunningPartitions(queryId: String, partitions: Int): Unit = {}
    override def decrementRunningPartitions(queryId: String): Int = 0
    override def deleteMetrics(filter: QueryMetricsFilter): Int = 0
  }
}

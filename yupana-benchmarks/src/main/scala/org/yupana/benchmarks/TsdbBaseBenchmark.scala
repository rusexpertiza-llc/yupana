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
import org.openjdk.jmh.annotations.{ Benchmark, Scope, State }
import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query.{ DimensionExpr, MetricExpr, Query, TimeExpr }
import org.yupana.api.query.syntax.All.{
  const,
  count,
  dimension,
  distinctCount,
  divFrac,
  double2bigDecimal,
  long2BigDecimal,
  metric,
  sum,
  time
}
import org.yupana.core.model.{ InternalRow, InternalRowBuilder }
import org.yupana.core.utils.metric.NoMetricCollector
import org.yupana.core.{ MapReducible, QueryContext, SimpleTsdbConfig, TSDB, TsdbServerResult }
import org.yupana.schema.{ Dimensions, ItemTableMetrics, SchemaRegistry, Tables }

class TsdbBaseBenchmark {

  @Benchmark
  def processRows(state: TsdbBaseBenchmarkState): TsdbServerResult = {
    val tsdb = state.tsdb
    tsdb.processRows(
      TsdbBaseBenchmark.queryContext,
      NoMetricCollector,
      MapReducible.iteratorMR,
      state.rows.iterator
    )
  }
}

@State(Scope.Thread)
class TsdbBaseBenchmarkState {
  val tsdb: TSDB = new TsdbBaseBenchmark.BenchTsdb()
  val rows = TsdbBaseBenchmark.getRows(1000000)
}

object TsdbBaseBenchmark {
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
    filter = None, //Some(in(dimension(Dimensions.KKM_ID), (1 to 1000).toSet)),
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
        null,
        null,
        identity,
        SimpleTsdbConfig(),
        _ => NoMetricCollector
      )
}

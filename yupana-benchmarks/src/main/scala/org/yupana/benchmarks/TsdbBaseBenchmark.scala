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
import org.yupana.api.query.syntax.All._
import org.yupana.api.query._
import org.yupana.core.model.{ InternalRow, InternalRowBuilder }
import org.yupana.core.utils.metric.NoMetricCollector
import org.yupana.core.{ MapReducible, QueryContext, SimpleTsdbConfig, TSDB }
import org.yupana.schema.{ Dimensions, ItemTableMetrics, SchemaRegistry, Tables }

class TsdbBaseBenchmark {

  @Benchmark
  def processRowsMinimal(state: TsdbBaseBenchmarkStateMin): Int = {
    state.tsdb
      .processRows(
        state.queryContext,
        NoMetricCollector,
        MapReducible.iteratorMR,
        state.rows.iterator
      )
      .size
  }

  @Benchmark
  def processRows(state: TsdbBaseBenchmarkState): Int = {
    state.tsdb
      .processRows(
        state.queryContext,
        NoMetricCollector,
        MapReducible.iteratorMR,
        state.rows.iterator
      )
      .size
  }

  @Benchmark
  def processRowsWithSimpleAgg(state: TsdbBaseBenchmarkStateSimpleAgg): Int = {
    state.tsdb
      .processRows(
        state.queryContext,
        NoMetricCollector,
        MapReducible.iteratorMR,
        state.rows.iterator
      )
      .size
  }

  @Benchmark
  def processRowsWithAgg(state: TsdbBaseBenchmarkStateAgg): Int = {
    state.tsdb
      .processRows(
        state.queryContext,
        NoMetricCollector,
        MapReducible.iteratorMR,
        state.rows.iterator
      )
      .size
  }
}

abstract class TsdbBaseBenchmarkStateBase {
  def query: Query
  def daoExprs: Seq[Expression[_]]

  lazy val queryContext: QueryContext = QueryContext(query, None)
  private def rowBuilder = new InternalRowBuilder(queryContext)

  val tsdb: TSDB = new TsdbBaseBenchmark.BenchTsdb()
  lazy val rows: Seq[InternalRow] = TsdbBaseBenchmark.getRows(
    rowBuilder,
    100000,
    daoExprs
  )
}

@State(Scope.Benchmark)
class TsdbBaseBenchmarkStateMin extends TsdbBaseBenchmarkStateBase {
  override val query: Query = Query(
    table = Tables.itemsKkmTable,
    from = const(Time(LocalDateTime.now().minusDays(1))),
    to = const(Time(LocalDateTime.now())),
    fields = Seq(
      time as "time",
      dimension(Dimensions.ITEM) as "item",
      metric(ItemTableMetrics.quantityField) as "quantity"
    ),
    filter = None,
    groupBy = Seq.empty
  )

  override val daoExprs: Seq[Expression[_]] =
    Seq(time, dimension(Dimensions.ITEM), metric(ItemTableMetrics.quantityField))
}

@State(Scope.Benchmark)
class TsdbBaseBenchmarkState extends TsdbBaseBenchmarkStateBase {
  val query: Query = Query(
    table = Tables.itemsKkmTable,
    from = const(Time(LocalDateTime.now().minusDays(1))),
    to = const(Time(LocalDateTime.now())),
    fields = Seq(
      time as "time",
      dimension(Dimensions.ITEM) as "item",
      divInt(dimension(Dimensions.KKM_ID), const(2)) as "half_of_kkm",
      metric(ItemTableMetrics.quantityField) as "quantity",
      divFrac(metric(ItemTableMetrics.sumField), double2bigDecimal(metric(ItemTableMetrics.quantityField))) as "price"
    ),
    filter = Some(gt(divInt(dimension(Dimensions.KKM_ID), const(2)), const(100))),
    groupBy = Seq.empty
  )

  val daoExprs: Seq[Expression[_]] =
    Seq(
      time,
      dimension(Dimensions.ITEM),
      metric(ItemTableMetrics.quantityField),
      metric(ItemTableMetrics.sumField),
      dimension(Dimensions.KKM_ID)
    )
}

@State(Scope.Benchmark)
class TsdbBaseBenchmarkStateSimpleAgg extends TsdbBaseBenchmarkStateBase {

  override val query: Query = Query(
    table = Tables.itemsKkmTable,
    from = const(Time(LocalDateTime.now().minusDays(1))),
    to = const(Time(LocalDateTime.now())),
    fields = Seq(
      truncDay(time) as "day",
      dimension(Dimensions.ITEM) as "item",
      sum(metric(ItemTableMetrics.quantityField)) as "total_quantity",
      metric(ItemTableMetrics.sumField) as "total_sum",
      max(ItemTableMetrics.sumField) as "max_sum"
    ),
    filter = None,
    groupBy = Seq(time, dimension(Dimensions.ITEM))
  )

  override val daoExprs: Seq[Expression[_]] = Seq(
    time,
    dimension(Dimensions.ITEM),
    metric(ItemTableMetrics.quantityField),
    metric(ItemTableMetrics.sumField)
  )
}

@State(Scope.Benchmark)
class TsdbBaseBenchmarkStateAgg extends TsdbBaseBenchmarkStateBase {

  override val query: Query = Query(
    table = Tables.itemsKkmTable,
    from = const(Time(LocalDateTime.now().minusDays(1))),
    to = const(Time(LocalDateTime.now())),
    fields = Seq(
      truncDay(time) as "day",
      dimension(Dimensions.ITEM) as "item",
      sum(metric(ItemTableMetrics.quantityField)) as "total_quantity",
      metric(ItemTableMetrics.sumField) as "total_sum",
      divFrac(
        sum(divFrac(double2bigDecimal(metric(ItemTableMetrics.quantityField)), metric(ItemTableMetrics.sumField))),
        long2BigDecimal(count(dimension(Dimensions.ITEM)))
      ) as "avg",
      min(
        divFrac(metric(ItemTableMetrics.sumField), double2bigDecimal(metric(ItemTableMetrics.quantityField)))
      ) as "min_price",
      sum(
        condition(
          gt(
            divFrac(metric(ItemTableMetrics.sumField), double2bigDecimal(metric(ItemTableMetrics.quantityField))),
            const(BigDecimal(100))
          ),
          const(1L),
          const(0L)
        )
      ) as "count_expensive"
    ),
    filter = None,
    groupBy = Seq(time, dimension(Dimensions.ITEM))
  )

  override val daoExprs: Seq[Expression[_]] = Seq(
    time,
    dimension(Dimensions.ITEM),
    metric(ItemTableMetrics.quantityField),
    metric(ItemTableMetrics.sumField)
  )
}

object TsdbBaseBenchmark {

  val qtime = new LocalDateTime(2021, 5, 24, 22, 40, 0)

  private val EXPR_CALC: Map[Expression[_], Int => Any] = Map(
    TimeExpr -> (i => Time(qtime.minusHours(i % 100))),
    MetricExpr(ItemTableMetrics.sumField) -> (i => BigDecimal(i.toDouble / 1000)),
    MetricExpr(ItemTableMetrics.quantityField) -> (i => math.abs(101d - i.toDouble / 10000)),
    DimensionExpr(Dimensions.ITEM) -> (i => s"The thing #${i % 1000}"),
    DimensionExpr(Dimensions.KKM_ID) -> (i => i % 500)
  )

  def getRows(rowBuilder: InternalRowBuilder, n: Int, exprs: Seq[Expression[_]]): Seq[InternalRow] = {
    (1 to n).map { i =>
      exprs.foreach(expr => rowBuilder.set(expr, EXPR_CALC(expr)(i)))
      rowBuilder.buildAndReset()
    }
  }

  class BenchTsdb
      extends TSDB(
        SchemaRegistry.defaultSchema,
        null,
        null,
        null,
        identity,
        SimpleTsdbConfig(),
        _ => NoMetricCollector
      )
}

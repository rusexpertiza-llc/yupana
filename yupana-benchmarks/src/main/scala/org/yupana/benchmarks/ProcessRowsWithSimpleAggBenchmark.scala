package org.yupana.benchmarks

import org.joda.time.LocalDateTime
import org.openjdk.jmh.annotations.{ Benchmark, Scope, State }
import org.yupana.api.Time
import org.yupana.api.query.{ Expression, Query }
import org.yupana.api.query.syntax.All.{ const, dimension, max, metric, sum, time, truncDay }
import org.yupana.core.MapReducible
import org.yupana.core.utils.metric.NoMetricCollector
import org.yupana.schema.{ Dimensions, ItemTableMetrics, Tables }

class ProcessRowsWithSimpleAggBenchmark {

  @Benchmark
  def processRowsWithSimpleAgg(state: TsdbBaseBenchmarkStateSimpleAgg): Int = {
    state.tsdb
      .processRows(
        state.queryContext,
        NoMetricCollector,
        MapReducible.iteratorMR,
        state.rows.iterator,
        None
      )
      .size
  }
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

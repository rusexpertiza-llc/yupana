package org.yupana.benchmarks

import org.joda.time.LocalDateTime
import org.openjdk.jmh.annotations.{ Benchmark, Scope, State }
import org.yupana.api.Time
import org.yupana.api.query.{ Expression, Query }
import org.yupana.api.query.syntax.All.{ const, dimension, metric, time }
import org.yupana.core.MapReducible
import org.yupana.core.utils.metric.NoMetricCollector
import org.yupana.schema.{ Dimensions, ItemTableMetrics, Tables }

class ProcessRowsMinBenchmark {

  @Benchmark
  def processRowsMinimal(state: TsdbBaseBenchmarkStateMin): Int = {
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

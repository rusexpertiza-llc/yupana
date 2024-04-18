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

import org.openjdk.jmh.annotations.{ Benchmark, Scope, State }
import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.api.query.syntax.All._
import org.yupana.core.IteratorMapReducible
import org.yupana.core.utils.metric.StandaloneMetricCollector
import org.yupana.metrics.Slf4jMetricReporter
import org.yupana.schema.{ Dimensions, ItemTableMetrics, Tables }

import java.time.LocalDateTime

class ProcessRowsBenchmark {

  @Benchmark
  def processRows(state: TsdbBaseBenchmarkState): Int = {
    val mc = new StandaloneMetricCollector(state.query, "admin", "bench", 1000, new Slf4jMetricReporter)
    val res = state.tsdb
      .processRows(
        state.queryContext,
        mc,
        IteratorMapReducible.iteratorMR,
        state.dataset.iterator
      )
    var i = 0
    while (res.next()) {
      i += 1
    }
    res.close()
    mc.finish()
    i
  }

}

@State(Scope.Benchmark)
class TsdbBaseBenchmarkState extends TsdbBaseBenchmarkStateBase {
  val query: Query = Query(
    table = Tables.itemsKkmTable,
    from = const(Time(LocalDateTime.now().minusDays(1))),
    to = const(Time(LocalDateTime.now())),
    fields = Seq(
      time as "time",
      truncDay(time) as "day",
      dimension(Dimensions.ITEM) as "item",
      divInt(dimension(Dimensions.KKM_ID), const(2)) as "half_of_kkm",
      metric(ItemTableMetrics.quantityField) as "quantity",
      metric(ItemTableMetrics.sumField) as "sum",
      plus(metric(ItemTableMetrics.quantityField), const(1.11)) as "tt",
      plus(metric(ItemTableMetrics.sumField), const(BigDecimal(1))) as "sum"
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

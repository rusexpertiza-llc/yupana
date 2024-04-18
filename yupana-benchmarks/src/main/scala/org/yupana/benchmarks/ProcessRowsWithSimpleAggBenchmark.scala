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
import org.yupana.api.query.{ Expression, Query }
import org.yupana.api.query.syntax.All.{ const, dimension, divInt, metric, sum }
import org.yupana.core.IteratorMapReducible
import org.yupana.core.utils.metric.NoMetricCollector
import org.yupana.schema.{ Dimensions, ItemTableMetrics, Tables }

import java.time.LocalDateTime

class ProcessRowsWithSimpleAggBenchmark {

  @Benchmark
  def processRowsWithSimpleAgg(state: TsdbBaseBenchmarkStateSimpleAgg): Int = {
    val res = state.tsdb
      .processRows(
        state.queryContext,
        NoMetricCollector,
        IteratorMapReducible.iteratorMR,
        state.dataset.iterator
      )
    var i = 0
    while (res.next()) {
      i += 1
    }
    i
  }
}

@State(Scope.Benchmark)
class TsdbBaseBenchmarkStateSimpleAgg extends TsdbBaseBenchmarkStateBase {

  override val query: Query = Query(
    table = Tables.itemsKkmTable,
    from = const(Time(LocalDateTime.now().minusDays(1))),
    to = const(Time(LocalDateTime.now())),
    fields = Seq(
      divInt(dimension(Dimensions.KKM_ID), const(1000)) as "half_of_kkm",
      sum(metric(ItemTableMetrics.quantityField)) as "total_quantity",
      sum(ItemTableMetrics.sumField) as "sum_sum"
    ),
    filter = None,
    groupBy = Seq(divInt(dimension(Dimensions.KKM_ID), const(1000)))
  )

  override val daoExprs: Seq[Expression[_]] = Seq(
    dimension(Dimensions.KKM_ID),
    metric(ItemTableMetrics.quantityField),
    metric(ItemTableMetrics.sumField)
  )
}

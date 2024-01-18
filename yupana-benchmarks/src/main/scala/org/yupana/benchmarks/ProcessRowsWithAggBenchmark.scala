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
import org.yupana.api.query.syntax.All.{
  condition,
  const,
  count,
  dimension,
  divFrac,
  double2bigDecimal,
  gt,
  long2BigDecimal,
  metric,
  min,
  sum,
  time
}
import org.yupana.core.IteratorMapReducible
import org.yupana.core.utils.metric.NoMetricCollector
import org.yupana.schema.{ Dimensions, ItemTableMetrics, Tables }

import java.time.LocalDateTime

class ProcessRowsWithAggBenchmark {

  @Benchmark
  def processRowsWithAgg(state: TsdbBaseBenchmarkStateAgg): Int = {
    state.tsdb
      .processRows(
        state.queryContext,
        state.rowBuilder,
        NoMetricCollector,
        IteratorMapReducible.iteratorMR,
        state.rows.iterator
      )
      .size
  }
}

@State(Scope.Benchmark)
class TsdbBaseBenchmarkStateAgg extends TsdbBaseBenchmarkStateBase {

  override val query: Query = Query(
    table = Tables.itemsKkmTable,
    from = const(Time(LocalDateTime.now().minusDays(1))),
    to = const(Time(LocalDateTime.now())),
    fields = Seq(
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
    groupBy = Seq(dimension(Dimensions.ITEM))
  )

  override val daoExprs: Seq[Expression[_]] = Seq(
    time,
    dimension(Dimensions.ITEM),
    metric(ItemTableMetrics.quantityField),
    metric(ItemTableMetrics.sumField)
  )
}

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
import org.yupana.api.query._
import org.yupana.api.query.syntax.All._
import org.yupana.core.IteratorMapReducible
import org.yupana.core.utils.metric.NoMetricCollector
import org.yupana.schema.{ Dimensions, ItemTableMetrics, Tables }

class ProcessRowsBenchmark {

  @Benchmark
  def processRows(state: TsdbBaseBenchmarkState): Int = {
    state.tsdb
      .processRows(
        state.queryContext,
        NoMetricCollector,
        IteratorMapReducible.iteratorMR,
        state.rows.iterator
      )
      .size
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

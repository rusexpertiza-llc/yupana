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

import org.yupana.api.{ Currency, Time }
import org.yupana.api.query._
import org.yupana.cache.CacheFactory
import org.yupana.core.jit.JIT
import org.yupana.core.model.BatchDataset
import org.yupana.core.utils.metric.NoMetricCollector
import org.yupana.core.{ QueryContext, SimpleTsdbConfig, TSDB }
import org.yupana.schema.{ Dimensions, ItemTableMetrics, SchemaRegistry }
import org.yupana.settings.Settings
import org.yupana.utils.RussianTokenizer

import java.time.LocalDateTime
import java.util.Properties

abstract class TsdbBaseBenchmarkStateBase {
  def query: Query
  def daoExprs: Seq[Expression[_]]

  lazy val queryContext: QueryContext = new QueryContext(query, None, RussianTokenizer, JIT, NoMetricCollector)

  val qtime = LocalDateTime.of(2021, 5, 24, 22, 40, 0)

  private val EXPR_CALC: Map[Expression[_], Int => Any] = Map(
    TimeExpr -> (i => Time(qtime.minusHours(i % 100))),
    MetricExpr(ItemTableMetrics.sumField) -> (i => Currency(i)),
    MetricExpr(ItemTableMetrics.quantityField) -> (i => math.abs(101d - i.toDouble / 10000)),
    DimensionExpr(Dimensions.ITEM) -> (i => s"#${i % 1000}"),
    DimensionExpr(Dimensions.KKM_ID) -> (i => i % 500)
  )

  def getDataset(n: Int): Array[BatchDataset] = {
    val batch = BatchDataset(queryContext)
    (0 until n)
      .foreach { i =>
        daoExprs.foreach { expr =>
          val f = EXPR_CALC(expr)
          val value = f(i + 1)
          batch.set(i, expr.asInstanceOf[Expression[Any]], value)
        }
      }
    Array(batch)
  }

  val properties = new Properties()
  properties.load(getClass.getClassLoader.getResourceAsStream("app.properties"))
  CacheFactory.init(Settings(properties))

  val tsdb: TSDB = new BenchTsdb()
  lazy val dataset: Array[BatchDataset] = getDataset(100000)
}

class BenchTsdb
    extends TSDB(
      SchemaRegistry.defaultSchema,
      null,
      null,
      identity,
      SimpleTsdbConfig(),
      (_, _) => NoMetricCollector
    )

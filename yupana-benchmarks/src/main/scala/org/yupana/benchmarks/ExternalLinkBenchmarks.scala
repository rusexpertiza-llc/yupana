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
import org.yupana.api.schema.{ Dimension, ExternalLink, LinkField, RawDimension, Table => SchemaTable }
import org.yupana.core.{ ExpressionCalculatorFactory, QueryContext }
import org.yupana.core.model.{ InternalRow, InternalRowBuilder, TimeSensitiveFieldValues }
import org.yupana.core.utils.metric.NoMetricCollector
import org.yupana.core.utils.{ SparseTable, Table }
import org.yupana.externallinks.ExternalLinkUtils
import org.yupana.schema.Tables
import java.time.{ OffsetDateTime, ZoneOffset }

object BenchLink extends ExternalLink {

  override type DimType = Int

  val dim: RawDimension[Int] = RawDimension[Int]("benchDim")
  val F1 = "f1"
  val F2 = "f2"
  override val linkName: String = "benchLink"
  override val dimension: Dimension.Aux[Int] = dim
  override val fields: Set[LinkField] = Set(LinkField[String](F1), LinkField[String](F2))
}

class ExternalLinkBenchmarks {

  @Benchmark
  def setLinkedValues(state: ExternalLinkBenchmarkState): Unit = {
    ExternalLinkUtils.setLinkedValues[Int](
      state.externalLink,
      state.exprIndex,
      state.rows,
      state.exprs,
      fieldValuesForDimValues
    )
  }

  @Benchmark
  def setLinkedValuesTimeSensitive(state: ExternalLinkBenchmarkState): Unit = {
    ExternalLinkUtils.setLinkedValuesTimeSensitive[Int](
      state.externalLink,
      state.exprIndex,
      state.rows,
      state.exprs,
      fieldValuesForDimValuesTimeSensitive
    )
  }

  def fieldValuesForDimValues(fields: Set[String], dimValues: Set[Int]): Table[Int, String, String] = {
    // [dim, field, value]
    SparseTable(dimValues.map(i => (i, BenchLink.F1, s"$i-f1-val")))
  }

  def fieldValuesForDimValuesTimeSensitive(
      fields: Set[String],
      dimIds: Set[Int],
      from: Time,
      to: Time
  ): Map[Int, Array[TimeSensitiveFieldValues]] = {
    val times = for (t <- from.millis to to.millis by 1) yield Time(t)
    dimIds.map { dimId =>
      dimId -> times.map(t => TimeSensitiveFieldValues(t, Map(BenchLink.F1 -> s"$dimId-f1-val"))).toArray
    }.toMap
  }
}

@State(Scope.Benchmark)
class ExternalLinkBenchmarkState {

  val dim: RawDimension[Int] = BenchLink.dim
  val dimExpr: DimensionExpr[Int] = DimensionExpr[Int](dim.aux)
  val table = new SchemaTable(1, "benchTable", 1L, Seq(dim), Seq.empty, Seq(BenchLink), Tables.epochTime)
  val linkExpr: LinkExpr[String] = LinkExpr(BenchLink, BenchLink.F1)
  val t0: OffsetDateTime = OffsetDateTime.of(2019, 4, 2, 0, 0, 0, 0, ZoneOffset.UTC)
  val t1: OffsetDateTime = t0.plusYears(1)
  val query = new Query(
    Some(table),
    Seq(QueryField(dim.name, dimExpr), QueryField(BenchLink.F1, linkExpr)),
    Some(
      AndExpr(
        Seq(
          GeExpr[Time](TimeExpr, ConstantExpr[Time](Time(t0))),
          LtExpr[Time](TimeExpr, ConstantExpr[Time](Time(t1)))
        )
      )
    )
  )
  val queryContext: QueryContext = new QueryContext(query, None, ExpressionCalculatorFactory, NoMetricCollector)

  var externalLink: ExternalLink.Aux[Int] = BenchLink
  var exprIndex: Map[Expression[_], Int] = queryContext.exprsIndex.toMap
  var rows: Seq[InternalRow] = 1 to 10000 map { i =>
    new InternalRowBuilder(exprIndex, None)
      .set(dimExpr, i - (i % 2))
      .set(TimeExpr, Time(System.currentTimeMillis()))
      .buildAndReset()
  }
  var exprs: Set[LinkExpr[_]] = Set(linkExpr)
}

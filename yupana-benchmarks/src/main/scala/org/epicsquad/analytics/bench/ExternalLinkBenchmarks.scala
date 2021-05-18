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

package org.epicsquad.analytics.bench

import org.joda.time.DateTime
import org.openjdk.jmh.annotations.{ Benchmark, Scope, State }
import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.api.schema.{ Dimension, ExternalLink, LinkField, RawDimension, Table => YTable }
import org.yupana.api.types.DataType
import org.yupana.core.QueryContext
import org.yupana.core.model.{ InternalRow, InternalRowBuilder }
import org.yupana.core.utils.{ SparseTable, Table }
import org.yupana.externallinks.ExternalLinkUtils
import org.yupana.schema.Tables

object BenchLink extends ExternalLink {

  override type DimType = Int

  val dim: RawDimension[Int] = RawDimension[Int]("benchDim")
  val F1 = "f1"
  val F2 = "f2"
  override val linkName: String = "benchLink"
  override val dimension: Dimension.Aux[Int] = dim
  implicit val dataType = DataType.stringDt
  override val fields: Set[LinkField] = Set(LinkField(F1), LinkField(F2))
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
      dimValuesWithTimes: Set[(Int, Time)]
  ): Table[(Int, Time), String, String] = {
    // [(dim, time) field, value]
    SparseTable(dimValuesWithTimes.map { case (i, t) => ((i, t), BenchLink.F1, s"$i-f1-val") })
  }
}

@State(Scope.Benchmark)
class ExternalLinkBenchmarkState {

  val dim: RawDimension[Int] = BenchLink.dim
  val dimExpr: DimensionExpr[Int] = DimensionExpr[Int](dim.aux)
  val table = new YTable("benchTable", 1L, Seq(dim), Seq.empty, Seq(BenchLink), Tables.epochTime)
  val linkExpr: LinkExpr[String] = LinkExpr(BenchLink, BenchLink.F1)
  val t0 = new DateTime("2019-04-20")
  val t1: DateTime = t0.plusYears(1)
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
  val queryContext: QueryContext = QueryContext(query, None)

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

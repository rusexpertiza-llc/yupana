package org.epicsquad.analytics.bench

import org.joda.time.DateTime
import org.openjdk.jmh.annotations.{ Benchmark, Scope, State }
import org.yupana.api.Time
import org.yupana.api.query.{
  AndExpr,
  BinaryOperationExpr,
  ConstantExpr,
  DimensionExpr,
  Expression,
  LinkExpr,
  Query,
  QueryField,
  TimeExpr
}
import org.yupana.api.schema.{ Dimension, ExternalLink, Table => YTable }
import org.yupana.api.types.BinaryOperation
import org.yupana.core.QueryContext
import org.yupana.core.model.{ InternalRow, InternalRowBuilder }
import org.yupana.core.utils.{ SparseTable, Table }
import org.yupana.externallinks.ExternalLinkUtils

object BenchLink extends ExternalLink {
  val dim: Dimension = Dimension("benchDim")
  val F1 = "f1"
  val F2 = "f2"
  override val linkName: String = "benchLink"
  override val dimension: Dimension = dim
  override val fieldsNames: Set[String] = Set(F1, F2)
}

class ExternalLinkBenchmarks {

  @Benchmark
  def setLinkedValues(state: BenchmarkState): Unit = {
    ExternalLinkUtils.setLinkedValues(
      state.externalLink,
      state.exprIndex,
      state.rows,
      state.exprs,
      fieldValuesForDimValues
    )
  }

  @Benchmark
  def setLinkedValuesTimeSensitive(state: BenchmarkState): Unit = {
    ExternalLinkUtils.setLinkedValuesTimeSensitive(
      state.externalLink,
      state.exprIndex,
      state.rows,
      state.exprs,
      fieldValuesForDimValuesTimeSensitive
    )
  }

  def fieldValuesForDimValues(fields: Set[String], dimValues: Set[String]): Table[String, String, String] = {
    // [dim, field, value]
    SparseTable(dimValues.map(i => (i, BenchLink.F1, s"$i-f1-val")))
  }

  def fieldValuesForDimValuesTimeSensitive(
      fields: Set[String],
      dimValuesWithTimes: Set[(String, Time)]
  ): Table[String, String, String] = {
    // [dim, field, value]
    SparseTable(dimValuesWithTimes.map { case (i, t) => (i, BenchLink.F1, s"$i-f1-val") })
  }
}

@State(Scope.Benchmark)
class BenchmarkState {

  val dim: Dimension = BenchLink.dim
  val dimExpr: DimensionExpr = DimensionExpr(dim)
  val table = new YTable("benchTable", 1L, Seq(dim), Seq.empty, Seq(BenchLink))
  val linkExpr = LinkExpr(BenchLink, BenchLink.F1)
  val t0 = new DateTime("2019-04-20")
  val t1 = t0.plusYears(1)
  val query = new Query(
    Some(table),
    Seq(QueryField(dim.name, dimExpr), QueryField(BenchLink.F1, linkExpr)),
    Some(
      AndExpr(
        Seq(
          BinaryOperationExpr(BinaryOperation.ge[Time], TimeExpr, ConstantExpr[Time](Time(t0))),
          BinaryOperationExpr(BinaryOperation.lt[Time], TimeExpr, ConstantExpr[Time](Time(t1)))
        )
      )
    )
  )
  val queryContext: QueryContext = QueryContext(query, None)

  var externalLink: ExternalLink = BenchLink
  var exprIndex: Map[Expression, Int] = queryContext.exprsIndex.toMap
  var rows: Seq[InternalRow] = 1 to 10000 map { i =>
    new InternalRowBuilder(exprIndex)
      .set(dimExpr, Some((i - (i % 100)).toString))
      .set(TimeExpr, Some(Time(System.currentTimeMillis())))
      .buildAndReset()
  }
  var exprs: Set[LinkExpr] = Set(linkExpr)
}

package org.yupana.core

import org.scalatest.{ Matchers, WordSpecLike }
import org.yupana.api.query.{ DimensionExpr, LinkExpr, MetricExpr, TimeExpr }
import org.yupana.api.schema.{ Dimension, ExternalLink, Metric }
import org.yupana.core.model.InternalRow

import scala.collection.mutable

class ExpressionCalculatorTest extends WordSpecLike with Matchers {

  "Expression calculator" should {

    "Evaluate expression to None" in {
      val nullQueryContext: QueryContext = null
      ExpressionCalculator.evaluateExpression(
        DimensionExpr(Dimension("testDim")),
        nullQueryContext,
        new InternalRow(Array.empty),
        tryEval = false
      ) shouldBe None
      val queryContextWithoutThatExpr = QueryContext(
        null,
        None,
        mutable.HashMap.empty,
        Array.empty,
        Array.empty,
        Array.empty,
        Array.empty,
        Seq.empty,
        Array.empty
      )
      ExpressionCalculator.evaluateExpression(
        DimensionExpr(Dimension("testDim")),
        queryContextWithoutThatExpr,
        new InternalRow(Array.empty),
        tryEval = false
      ) shouldBe None
    }

    "Never try to evaluate time, dim, metric or link expressions" in {
      val queryContext = QueryContext(
        null,
        None,
        mutable.HashMap.empty,
        Array.empty,
        Array.empty,
        Array.empty,
        Array.empty,
        Seq.empty,
        Array.empty
      )
      ExpressionCalculator.evaluateExpression(TimeExpr, queryContext, new InternalRow(Array.empty), tryEval = true) shouldBe None
      ExpressionCalculator.evaluateExpression(
        DimensionExpr(Dimension("anyDim")),
        queryContext,
        new InternalRow(Array.empty),
        tryEval = true
      ) shouldBe None
      ExpressionCalculator.evaluateExpression(
        MetricExpr(Metric[Int]("anyMetric", 1)),
        queryContext,
        new InternalRow(Array.empty),
        tryEval = true
      ) shouldBe None
      val TestLink = new ExternalLink {
        override val linkName: String = "test_link"
        override val dimension: Dimension = Dimension("testDim")
        override val fieldsNames: Set[String] = Set("foo", "bar")
      }
      ExpressionCalculator.evaluateExpression(
        LinkExpr(TestLink, "foo"),
        queryContext,
        new InternalRow(Array.empty),
        tryEval = true
      ) shouldBe None
    }
  }

}

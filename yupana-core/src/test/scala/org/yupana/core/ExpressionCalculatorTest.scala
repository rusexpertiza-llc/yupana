/*
package org.yupana.core

import org.scalatest.{ Matchers, OptionValues, WordSpecLike }
import org.yupana.api.query.{ DimensionExpr, LinkExpr, MetricExpr, TimeExpr }
import org.yupana.api.schema.{ Dimension, ExternalLink, Metric }
import org.yupana.core.model.InternalRow

import scala.collection.mutable

class ExpressionCalculatorTest extends WordSpecLike with Matchers with OptionValues {

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

    "Evaluate constants" in {
      import org.yupana.api.query.syntax.All
      import org.yupana.api.query.syntax.All._

      ExpressionCalculator.evaluateConstant(plus(const(2), times(const(2), const(2)))).value shouldEqual 6
      ExpressionCalculator.evaluateConstant(divInt(All.length(const("9 letters")), const(3))).value shouldEqual 3
      ExpressionCalculator
        .evaluateConstant(All.not(contains(const(Array(1L, 2L, 3L)), const(5L))))
        .value shouldEqual true
    }
  }

}
*/

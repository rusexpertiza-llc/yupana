package org.yupana.core

import org.scalatest.{ Matchers, OptionValues, WordSpecLike }
import org.yupana.api.query.{ DimensionExpr, LinkExpr, MetricExpr, TimeExpr }
import org.yupana.api.schema.{ DictionaryDimension, Dimension, ExternalLink, LinkField, Metric, RawDimension }
import org.yupana.core.model.InternalRow

import scala.collection.mutable

class ExpressionCalculatorTest extends WordSpecLike with Matchers with OptionValues {

  "Expression calculator" should {

    "Evaluate expression to None" in {
      val nullQueryContext: QueryContext = null
      ExpressionCalculator.evaluateExpression(
        DimensionExpr(DictionaryDimension("testDim")),
        nullQueryContext,
        new InternalRow(Array.empty),
        tryEval = false
      ) shouldBe null
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
        DimensionExpr(DictionaryDimension("testDim")),
        queryContextWithoutThatExpr,
        new InternalRow(Array.empty),
        tryEval = false
      ) shouldBe null
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
      ExpressionCalculator.evaluateExpression(TimeExpr, queryContext, new InternalRow(Array.empty), tryEval = true) shouldBe null
      ExpressionCalculator.evaluateExpression(
        DimensionExpr(RawDimension[Int]("anyDim")),
        queryContext,
        new InternalRow(Array.empty),
        tryEval = true
      ) shouldBe null.asInstanceOf[String]
      ExpressionCalculator.evaluateExpression(
        MetricExpr(Metric[Int]("anyMetric", 1)),
        queryContext,
        new InternalRow(Array.empty)
      ) shouldBe null.asInstanceOf[String]
      val TestLink = new ExternalLink {
        override type DimType = String
        override val linkName: String = "test_link"
        override val dimension: Dimension.Aux[String] = DictionaryDimension("testDim")
        override val fields: Set[LinkField] = Set("foo", "bar").map(LinkField[String])
      }
      ExpressionCalculator.evaluateExpression(
        LinkExpr(TestLink, "foo"),
        queryContext,
        new InternalRow(Array.empty),
        tryEval = true
      ) shouldBe null
    }

    "Evaluate constants" in {
      import org.yupana.api.query.syntax.All
      import org.yupana.api.query.syntax.All._

      ExpressionCalculator.evaluateConstant(plus(const(2), times(const(2), const(2)))) shouldEqual 6
      ExpressionCalculator.evaluateConstant(divInt(All.length(const("9 letters")), const(3))) shouldEqual 3
      ExpressionCalculator
        .evaluateConstant(All.not(contains(const(Array(1L, 2L, 3L)), const(5L)))) shouldEqual true
    }
  }

}

package org.yupana.core

import org.joda.time.LocalDateTime
import org.scalatest.{ Matchers, OptionValues, WordSpecLike }
import org.yupana.api.Time
import org.yupana.api.query.{ ConstantExpr, DimensionExpr, Expression, LinkExpr, MetricExpr, Query, TimeExpr }
import org.yupana.api.schema.{ DictionaryDimension, Dimension, ExternalLink, LinkField, Metric, RawDimension }
import org.yupana.core.model.{ InternalRow, InternalRowBuilder }
import org.yupana.utils.RussianTokenizer

import scala.collection.mutable

class ExpressionCalculatorTest extends WordSpecLike with Matchers with OptionValues {

  private val calculator = new ExpressionCalculator(RussianTokenizer)

  "Expression calculator" should {

    import org.yupana.api.query.syntax.All._

    "Never try to evaluate time, dim, metric or link expressions" in {
      val queryContext = createContext(Seq.empty)
      calculator.evaluateExpression(TimeExpr, queryContext, new InternalRow(Array.empty)) shouldBe null

      calculator.evaluateExpression(
        DimensionExpr(RawDimension[Int]("anyDim")),
        queryContext,
        new InternalRow(Array.empty)
      ) shouldBe null.asInstanceOf[String]

      calculator.evaluateExpression(
        MetricExpr(Metric[Int]("anyMetric", 1)),
        queryContext,
        new InternalRow(Array.empty)
      ) shouldBe null.asInstanceOf[String]

      val testLink: ExternalLink = new ExternalLink {
        override type DimType = String
        override val linkName: String = "test_link"
        override val dimension: Dimension.Aux[String] = DictionaryDimension("testDim")
        override val fields: Set[LinkField] = Set("foo", "bar").map(LinkField[String])
      }

      calculator.evaluateExpression(LinkExpr(testLink, "foo"), queryContext, new InternalRow(Array.empty)) shouldBe null
    }

    "Evaluate constants" in {
      import org.yupana.api.query.syntax.All

      calculator.evaluateConstant(plus(const(2), times(const(2), const(2)))) shouldEqual 6
      calculator.evaluateConstant(divInt(All.length(const("9 letters")), const(3))) shouldEqual 3
      calculator.evaluateConstant(All.not(contains(const(Seq(1L, 2L, 3L)), const(5L)))) shouldEqual true
    }

    "Evaluate different time functions" in {
      val qc = createContext(Seq(TimeExpr))
      val builder = new InternalRowBuilder(qc)
      val row = builder.set(Time(new LocalDateTime(2020, 10, 21, 11, 36, 42))).buildAndReset()

      calculator.evaluateExpression(extractYear(time), qc, row) shouldEqual 2020
      calculator.evaluateExpression(extractMonth(time), qc, row) shouldEqual 10
      calculator.evaluateExpression(extractDay(time), qc, row) shouldEqual 21
      calculator.evaluateExpression(extractHour(time), qc, row) shouldEqual 11
      calculator.evaluateExpression(extractMinute(time), qc, row) shouldEqual 36
      calculator.evaluateExpression(extractSecond(time), qc, row) shouldEqual 42

      calculator.evaluateExpression(truncYear(time), qc, row) shouldEqual Time(new LocalDateTime(2020, 1, 1, 0, 0))
      calculator.evaluateExpression(truncMonth(time), qc, row) shouldEqual Time(new LocalDateTime(2020, 10, 1, 0, 0))
      calculator.evaluateExpression(truncDay(time), qc, row) shouldEqual Time(new LocalDateTime(2020, 10, 21, 0, 0))
      calculator.evaluateExpression(truncWeek(time), qc, row) shouldEqual Time(new LocalDateTime(2020, 10, 19, 0, 0))
      calculator.evaluateExpression(truncHour(time), qc, row) shouldEqual Time(new LocalDateTime(2020, 10, 21, 11, 0))
      calculator.evaluateExpression(truncMinute(time), qc, row) shouldEqual Time(
        new LocalDateTime(2020, 10, 21, 11, 36)
      )
      calculator.evaluateExpression(truncSecond(time), qc, row) shouldEqual Time(
        new LocalDateTime(2020, 10, 21, 11, 36, 42)
      )
    }

    "Evaluate string functions" in {
      val qc = createContext(Seq(TimeExpr, DimensionExpr(TestDims.DIM_A)))
      val builder = new InternalRowBuilder(qc)
      val row = builder
        .set(Time(new LocalDateTime(2020, 10, 21, 11, 36, 42)))
        .set(DimensionExpr(TestDims.DIM_A), "Вкусная водичка №7")
        .buildAndReset()

      calculator.evaluateExpression(upper(dimension(TestDims.DIM_A)), qc, row) shouldEqual "ВКУСНАЯ ВОДИЧКА №7"
      calculator.evaluateExpression(lower(dimension(TestDims.DIM_A)), qc, row) shouldEqual "вкусная водичка №7"

      calculator.evaluateExpression(split(dimension(TestDims.DIM_A)), qc, row) should contain theSameElementsInOrderAs Seq(
        "Вкусная",
        "водичка",
        "7"
      )

      calculator
        .evaluateExpression(tokens(dimension(TestDims.DIM_A)), qc, row) should contain theSameElementsInOrderAs Seq(
        "vkusn",
        "vodichk",
        "№7"
      )
    }
  }

  private def createContext(exprs: Seq[Expression[_]]): QueryContext = {
    QueryContext(
      Query(
        TestSchema.testTable,
        ConstantExpr(Time(LocalDateTime.now().minusMonths(1))),
        ConstantExpr(Time(LocalDateTime.now())),
        exprs.zipWithIndex.map { case (e, i) => e as i.toString }
      ),
      mutable.HashMap(exprs.zipWithIndex: _*),
      Array.empty,
      Array.empty,
      Array.empty,
      Array.empty,
      Seq.empty,
      Array.empty
    )
  }

}

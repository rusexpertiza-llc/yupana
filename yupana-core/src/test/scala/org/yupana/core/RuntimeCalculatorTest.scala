package org.yupana.core

import org.joda.time.LocalDateTime
import org.scalatest.OptionValues
import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.api.schema._
import org.yupana.core.model.{ InternalRow, InternalRowBuilder }
import org.yupana.utils.RussianTokenizer

import scala.collection.mutable
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class RuntimeCalculatorTest extends AnyWordSpecLike with Matchers with OptionValues {

  private val calculator = new RuntimeCalculator(RussianTokenizer)

  "Runtime calculator" should {

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

    "Evaluate array functions" in {
      calculator.evaluateConstant(arrayLength(const(Seq(1, 2, 3)))) shouldEqual 3
      calculator.evaluateConstant(arrayToString(const(Seq(1, 2, 3, 4)))) shouldEqual "1, 2, 3, 4"
      calculator.evaluateConstant(arrayToString(const(Seq("1", "2", "4")))) shouldEqual "1, 2, 4"
      calculator.evaluateConstant(ArrayTokensExpr(const(Seq("красная вода", "соленые яблоки")))) should contain theSameElementsAs Seq(
        "krasn",
        "vod",
        "solen",
        "yablok"
      )

      calculator.evaluateConstant(arrayToString(array(const(1), plus(const(2), const(3)), const(4)))) shouldEqual "1, 5, 4"

      calculator.evaluateConstant(containsAll(array(const(1), const(2), const(3)), const(Seq(2, 3)))) shouldBe true
      calculator.evaluateConstant(containsAll(array(const(1), const(2), const(3)), const(Seq(2, 4)))) shouldBe false

      calculator.evaluateConstant(containsAny(array(const(1), const(2), const(3)), const(Seq(2, 3)))) shouldBe true
      calculator.evaluateConstant(containsAny(const(Seq(1, 2, 3)), const(Seq(2, 4)))) shouldBe true

      calculator.evaluateConstant(containsSame(array(const(1), const(2)), const(Seq(1, 2)))) shouldBe true
      calculator.evaluateConstant(containsSame(array(const("1"), const("2")), const(Seq("2", "1")))) shouldBe true
      calculator.evaluateConstant(containsSame(const(Seq(1, 2, 2)), const(Seq(1, 2)))) shouldBe false
    }

    "Evaluate aggregations" in {
      val qc =
        createContext(
          Seq(
            time,
            metric(TestTableFields.TEST_FIELD),
            sum(metric(TestTableFields.TEST_FIELD)),
            min(metric(TestTableFields.TEST_FIELD)),
            max(metric(TestTableFields.TEST_FIELD)),
            count(metric(TestTableFields.TEST_FIELD)),
            distinctCount(metric(TestTableFields.TEST_FIELD)),
            distinctRandom(metric(TestTableFields.TEST_FIELD))
          )
        )
      val builder = new InternalRowBuilder(qc)
      val rows = Seq(
        builder
          .set(Time(new LocalDateTime(2020, 10, 21, 11, 36, 42)))
          .set(MetricExpr(TestTableFields.TEST_FIELD), 73d)
          .buildAndReset(),
        builder
          .set(Time(new LocalDateTime(2020, 10, 22, 15, 36, 42)))
          .set(MetricExpr(TestTableFields.TEST_FIELD), 154d)
          .buildAndReset(),
        builder
          .set(Time(new LocalDateTime(2020, 10, 22, 15, 59, 24)))
          .set(MetricExpr(TestTableFields.TEST_FIELD), 73d)
          .buildAndReset()
      )

      val aggregations = Seq[AggregateExpr[_, _, _]](
        sum(metric(TestTableFields.TEST_FIELD)),
        min(metric(TestTableFields.TEST_FIELD)),
        max(metric(TestTableFields.TEST_FIELD)),
        count(metric(TestTableFields.TEST_FIELD)),
        distinctCount(metric(TestTableFields.TEST_FIELD)),
        distinctRandom(metric(TestTableFields.TEST_FIELD))
      )

      for {
        row <- rows
        agg <- aggregations
      } yield {
        val v = calculator.evaluateMap(agg, qc, row)
        row.set(qc, agg, v)
      }

      val resultRow = rows.reduce { (r1, r2) =>
        aggregations.foreach { agg =>
          val v = calculator.evaluateReduce(agg, qc, r1, r2)
          r1.set(qc, agg, v)
        }
        r1
      }

      aggregations.foreach { agg =>
        val v = calculator.evaluatePostMap(agg, qc, resultRow)
        resultRow.set(qc, agg, v)
      }

      resultRow.get(qc, sum(metric(TestTableFields.TEST_FIELD))) shouldEqual 300d
      resultRow.get(qc, min(metric(TestTableFields.TEST_FIELD))) shouldEqual 73d
      resultRow.get(qc, max(metric(TestTableFields.TEST_FIELD))) shouldEqual 154d
      resultRow.get(qc, count(metric(TestTableFields.TEST_FIELD))) shouldEqual 3L
      resultRow.get(qc, distinctCount(metric(TestTableFields.TEST_FIELD))) shouldEqual 2
      resultRow.get(qc, distinctRandom(metric(TestTableFields.TEST_FIELD))) should equal(154).or(equal(73d))
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

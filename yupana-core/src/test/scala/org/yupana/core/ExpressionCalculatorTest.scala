package org.yupana.core

import org.joda.time.{ DateTime, DateTimeZone }
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.Time
import org.yupana.api.query.Query
import org.yupana.core.model.InternalRowBuilder

class ExpressionCalculatorTest extends AnyFlatSpec with Matchers with GivenWhenThen {
  import org.yupana.api.query.syntax.All._

  "ExpressionCalculator" should "filter rows" in {
    val cond = gt(plus(dimension(TestDims.DIM_B), const(1.toShort)), const(42.toShort))

    val query = Query(
      TestSchema.testTable,
      const(Time(123456789)),
      const(Time(234567890)),
      Seq(
        dimension(TestDims.DIM_A) as "A",
        metric(TestTableFields.TEST_FIELD) as "F",
        time as "T"
      ),
      cond
    )

    val qc = QueryContext(query, Some(cond))

    val calc = ExpressionCalculator.makeCalculator(qc, Some(cond))

    val builder = new InternalRowBuilder(qc)

    calc.evaluateFilter(
      builder
        .set(Time(DateTime.now()))
        .set(dimension(TestDims.DIM_A), "значение")
        .set(dimension(TestDims.DIM_B), 12.toShort)
        .buildAndReset()
    ) shouldBe false

    calc.evaluateFilter(
      builder
        .set(Time(DateTime.now()))
        .set(dimension(TestDims.DIM_A), "value")
        .set(dimension(TestDims.DIM_B), 42.toShort)
        .buildAndReset()
    ) shouldBe true
  }

  it should "evaluate row values" in {
    val now = DateTime.now()
    val query = Query(
      TestSchema.testTable,
      const(Time(now.minusDays(3))),
      const(Time(now)),
      Seq(
        metric(TestTableFields.TEST_FIELD) as "F",
        truncDay(time) as "T",
        divFrac(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2)) as "PRICE",
        plus(dimension(TestDims.DIM_B), const(1.toShort)) as "B_PLUS_1",
        divInt(dimension(TestDims.DIM_B), plus(dimension(TestDims.DIM_B), const(1.toShort))) as "bbb",
        divInt(dimension(TestDims.DIM_B), plus(dimension(TestDims.DIM_B), const(1.toShort))) as "bbb_2"
      )
    )

    val qc = QueryContext(query, None)

    val calc = ExpressionCalculator.makeCalculator(qc, None)

    val builder = new InternalRowBuilder(qc)

    val row = builder
      .set(Time(now.minusDays(2)))
      .set(metric(TestTableFields.TEST_FIELD), 10d)
      .set(metric(TestTableFields.TEST_FIELD2), 5d)
      .buildAndReset()

    calc.evaluateExpressions(row)
    row.get(qc, divFrac(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2))) shouldEqual 2d
    row.get(qc, truncDay(time)) shouldEqual Time(now.withZone(DateTimeZone.UTC).minusDays(2).withTimeAtStartOfDay())

    val rowWithNulls = builder
      .set(Time(now.minusDays(1)))
      .set(metric(TestTableFields.TEST_FIELD), 3d)
      .buildAndReset()

    calc.evaluateExpressions(rowWithNulls)
    rowWithNulls.get(qc, divFrac(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2))) shouldEqual null
      .asInstanceOf[java.lang.Double]
  }

  it should "calculate aggregation" in {
    Given("Query with aggregate expressions")

    val now = DateTime.now()
    val query = Query(
      TestSchema.testTable,
      const(Time(now.minusDays(3))),
      const(Time(now)),
      Seq(
        sum(metric(TestTableFields.TEST_FIELD)) as "SUM",
        max(metric(TestTableFields.TEST_FIELD)) as "MAX",
        count(metric(TestTableFields.TEST_FIELD)) as "COUNT",
        count(metric(TestTableFields.TEST_STRING_FIELD)) as "CS",
        distinctCount(metric(TestTableFields.TEST_FIELD)) as "DISTINCT",
        distinctRandom(metric(TestTableFields.TEST_FIELD)) as "RANDOM",
        truncDay(time) as "T",
        min(divFrac(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2))) as "MIN_PRICE"
      ),
      None,
      Seq(truncDay(time))
    )

    val qc = QueryContext(query, None)
    val calc = ExpressionCalculator.makeCalculator(qc, None)
    val builder = new InternalRowBuilder(qc)

    When("map called")
    val row1 = builder
      .set(Time(now.minusDays(1)))
      .set(metric(TestTableFields.TEST_FIELD), 10d)
      .set(metric(TestTableFields.TEST_FIELD2), 5d)
      .buildAndReset()

    val row2 = builder
      .set(Time(now.minusDays(1)))
      .set(metric(TestTableFields.TEST_FIELD), 12d)
      .set(metric(TestTableFields.TEST_FIELD2), 4d)
      .set(metric(TestTableFields.TEST_STRING_FIELD), "foo")
      .buildAndReset()

    val mapped1 = calc.evaluateMap(calc.evaluateExpressions(row1))
    val mapped2 = calc.evaluateMap(calc.evaluateExpressions(row2))
    Then("fields filled with map phase values")
    mapped1.get(qc, sum(metric(TestTableFields.TEST_FIELD))) shouldEqual 10d
    mapped1.get(qc, max(metric(TestTableFields.TEST_FIELD))) shouldEqual 10d
    mapped1.get(qc, count(metric(TestTableFields.TEST_FIELD))) shouldEqual 1L
    mapped1.get(qc, count(metric(TestTableFields.TEST_STRING_FIELD))) shouldEqual 0L
    mapped1.get(qc, distinctCount(metric(TestTableFields.TEST_FIELD))) shouldEqual Set(10d)
    mapped1.get(qc, distinctRandom(metric(TestTableFields.TEST_FIELD))) shouldEqual Set(10d)
    mapped1.get(qc, min(divFrac(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2)))) shouldEqual 2d

    mapped2.get(qc, sum(metric(TestTableFields.TEST_FIELD))) shouldEqual 12d
    mapped2.get(qc, max(metric(TestTableFields.TEST_FIELD))) shouldEqual 12d
    mapped2.get(qc, count(metric(TestTableFields.TEST_FIELD))) shouldEqual 1L
    mapped2.get(qc, count(metric(TestTableFields.TEST_STRING_FIELD))) shouldEqual 1L
    mapped2.get(qc, distinctCount(metric(TestTableFields.TEST_FIELD))) shouldEqual Set(12d)
    mapped2.get(qc, distinctRandom(metric(TestTableFields.TEST_FIELD))) shouldEqual Set(12d)
    mapped2.get(qc, min(divFrac(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2)))) shouldEqual 3d

    When("reduce called")
    val reduced = calc.evaluateReduce(mapped1, mapped2)
    Then("reduced values shall be calculated")
    reduced.get(qc, sum(metric(TestTableFields.TEST_FIELD))) shouldEqual 22d
    reduced.get(qc, max(metric(TestTableFields.TEST_FIELD))) shouldEqual 12d
    reduced.get(qc, count(metric(TestTableFields.TEST_FIELD))) shouldEqual 2L
    reduced.get(qc, count(metric(TestTableFields.TEST_STRING_FIELD))) shouldEqual 1L
    reduced.get(qc, distinctCount(metric(TestTableFields.TEST_FIELD))) shouldEqual Set(10d, 12d)
    reduced.get(qc, distinctRandom(metric(TestTableFields.TEST_FIELD))) shouldEqual Set(10d, 12d)
    reduced.get(qc, min(divFrac(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2)))) shouldEqual 2d

    When("postMap called")
    val postMapped = calc.evaluatePostMap(reduced)
    Then("post map calculations shall be performed")
    postMapped.get(qc, sum(metric(TestTableFields.TEST_FIELD))) shouldEqual 22d
    postMapped.get(qc, max(metric(TestTableFields.TEST_FIELD))) shouldEqual 12d
    postMapped.get(qc, count(metric(TestTableFields.TEST_FIELD))) shouldEqual 2L
    postMapped.get(qc, count(metric(TestTableFields.TEST_STRING_FIELD))) shouldEqual 1L
    postMapped.get(qc, distinctCount(metric(TestTableFields.TEST_FIELD))) shouldEqual 2
    postMapped.get(qc, distinctRandom(metric(TestTableFields.TEST_FIELD))) should (equal(10d) or equal(12d))
    postMapped.get(qc, min(divFrac(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2)))) shouldEqual 2d
  }
}

package org.yupana.core

import org.joda.time.{ DateTime, DateTimeZone }
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.Time
import org.yupana.api.query.{ ConcatExpr, LengthExpr, Query }
import org.yupana.core.model.InternalRowBuilder
import org.yupana.core.utils.metric.NoMetricCollector
import org.yupana.utils.RussianTokenizer

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

    val qc = QueryContext(query, Some(cond), NoMetricCollector)
    val calc = qc.calculator

    val builder = new InternalRowBuilder(qc)

    calc.evaluateFilter(
      RussianTokenizer,
      builder
        .set(Time(DateTime.now()))
        .set(dimension(TestDims.DIM_A), "значение")
        .set(dimension(TestDims.DIM_B), 12.toShort)
        .buildAndReset()
    ) shouldBe false

    calc.evaluateFilter(
      RussianTokenizer,
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

    val qc = QueryContext(query, None, NoMetricCollector)
    val calc = qc.calculator

    val builder = new InternalRowBuilder(qc)

    val row = builder
      .set(Time(now.minusDays(2)))
      .set(metric(TestTableFields.TEST_FIELD), 10d)
      .set(metric(TestTableFields.TEST_FIELD2), 5d)
      .buildAndReset()

    calc.evaluateExpressions(RussianTokenizer, row)
    row.get(qc, divFrac(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2))) shouldEqual 2d
    row.get(qc, truncDay(time)) shouldEqual Time(now.withZone(DateTimeZone.UTC).minusDays(2).withTimeAtStartOfDay())

    val rowWithNulls = builder
      .set(Time(now.minusDays(1)))
      .set(metric(TestTableFields.TEST_FIELD), 3d)
      .buildAndReset()

    calc.evaluateExpressions(RussianTokenizer, rowWithNulls)
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
        min(divFrac(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2))) as "MIN_PRICE",
        divFrac(plus(max(metric(TestTableFields.TEST_FIELD)), min(metric(TestTableFields.TEST_FIELD))), const(2d)) as "MIDDLE"
      ),
      None,
      Seq(truncDay(time))
    )

    val qc = QueryContext(query, None, NoMetricCollector)
    val calc = qc.calculator
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

    val mapped1 = calc.evaluateMap(RussianTokenizer, calc.evaluateExpressions(RussianTokenizer, row1))
    val mapped2 = calc.evaluateMap(RussianTokenizer, calc.evaluateExpressions(RussianTokenizer, row2))
    Then("fields filled with map phase values")
    mapped1.get(qc, sum(metric(TestTableFields.TEST_FIELD))) shouldEqual 10d
    mapped1.get(qc, max(metric(TestTableFields.TEST_FIELD))) shouldEqual 10d
    mapped1.get(qc, min(metric(TestTableFields.TEST_FIELD))) shouldEqual 10d
    mapped1.get(qc, count(metric(TestTableFields.TEST_FIELD))) shouldEqual 1L
    mapped1.get(qc, count(metric(TestTableFields.TEST_STRING_FIELD))) shouldEqual 0L
    mapped1.get(qc, distinctCount(metric(TestTableFields.TEST_FIELD))) shouldEqual Set(10d)
    mapped1.get(qc, distinctRandom(metric(TestTableFields.TEST_FIELD))) shouldEqual Set(10d)
    mapped1.get(qc, min(divFrac(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2)))) shouldEqual 2d
    mapped1.isEmpty(
      qc,
      divFrac(plus(max(metric(TestTableFields.TEST_FIELD)), min(metric(TestTableFields.TEST_FIELD))), const(2d))
    ) shouldBe true

    mapped2.get(qc, sum(metric(TestTableFields.TEST_FIELD))) shouldEqual 12d
    mapped2.get(qc, max(metric(TestTableFields.TEST_FIELD))) shouldEqual 12d
    mapped2.get(qc, min(metric(TestTableFields.TEST_FIELD))) shouldEqual 12d
    mapped2.get(qc, count(metric(TestTableFields.TEST_FIELD))) shouldEqual 1L
    mapped2.get(qc, count(metric(TestTableFields.TEST_STRING_FIELD))) shouldEqual 1L
    mapped2.get(qc, distinctCount(metric(TestTableFields.TEST_FIELD))) shouldEqual Set(12d)
    mapped2.get(qc, distinctRandom(metric(TestTableFields.TEST_FIELD))) shouldEqual Set(12d)
    mapped2.get(qc, min(divFrac(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2)))) shouldEqual 3d
    mapped2.isEmpty(
      qc,
      divFrac(plus(max(metric(TestTableFields.TEST_FIELD)), min(metric(TestTableFields.TEST_FIELD))), const(2d))
    ) shouldBe true

    When("reduce called")
    val reduced = calc.evaluateReduce(RussianTokenizer, mapped1, mapped2)
    Then("reduced values shall be calculated")
    reduced.get(qc, sum(metric(TestTableFields.TEST_FIELD))) shouldEqual 22d
    reduced.get(qc, max(metric(TestTableFields.TEST_FIELD))) shouldEqual 12d
    reduced.get(qc, min(metric(TestTableFields.TEST_FIELD))) shouldEqual 10d
    reduced.get(qc, count(metric(TestTableFields.TEST_FIELD))) shouldEqual 2L
    reduced.get(qc, count(metric(TestTableFields.TEST_STRING_FIELD))) shouldEqual 1L
    reduced.get(qc, distinctCount(metric(TestTableFields.TEST_FIELD))) shouldEqual Set(10d, 12d)
    reduced.get(qc, distinctRandom(metric(TestTableFields.TEST_FIELD))) shouldEqual Set(10d, 12d)
    reduced.get(qc, min(divFrac(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2)))) shouldEqual 2d
    reduced.isEmpty(
      qc,
      divFrac(plus(max(metric(TestTableFields.TEST_FIELD)), min(metric(TestTableFields.TEST_FIELD))), const(2d))
    ) shouldBe true

    When("postMap called")
    val postMapped = calc.evaluatePostMap(RussianTokenizer, reduced)
    Then("post map calculations shall be performed")
    postMapped.get(qc, sum(metric(TestTableFields.TEST_FIELD))) shouldEqual 22d
    postMapped.get(qc, max(metric(TestTableFields.TEST_FIELD))) shouldEqual 12d
    postMapped.get(qc, min(metric(TestTableFields.TEST_FIELD))) shouldEqual 10d
    postMapped.get(qc, count(metric(TestTableFields.TEST_FIELD))) shouldEqual 2L
    postMapped.get(qc, count(metric(TestTableFields.TEST_STRING_FIELD))) shouldEqual 1L
    postMapped.get(qc, distinctCount(metric(TestTableFields.TEST_FIELD))) shouldEqual 2
    postMapped.get(qc, distinctRandom(metric(TestTableFields.TEST_FIELD))) should (equal(10d) or equal(12d))
    postMapped.get(qc, min(divFrac(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2)))) shouldEqual 2d
    postMapped.isEmpty(
      qc,
      divFrac(plus(max(metric(TestTableFields.TEST_FIELD)), min(metric(TestTableFields.TEST_FIELD))), const(2d))
    ) shouldBe true

    When("evaluatePostAggregateExprs called")
    val postCalculated = calc.evaluatePostAggregateExprs(RussianTokenizer, postMapped)
    Then("calculations on aggregates are performed")
    postCalculated.get(
      qc,
      divFrac(plus(max(metric(TestTableFields.TEST_FIELD)), min(metric(TestTableFields.TEST_FIELD))), const(2d))
    ) shouldEqual 11d
  }

  it should "evaluate string functions" in {
    val now = DateTime.now()
    val query = Query(
      TestSchema.testTable,
      const(Time(now.minusDays(3))),
      const(Time(now)),
      Seq(
        LengthExpr(dimension(TestDims.DIM_A)) as "len",
        split(dimension(TestDims.DIM_A)) as "split",
        tokens(dimension(TestDims.DIM_A)) as "tokens",
        upper(dimension(TestDims.DIM_A)) as "up",
        lower(dimension(TestDims.DIM_A)) as "down",
        ConcatExpr(dimension(TestDims.DIM_A), const("!!!")) as "yeah"
      )
    )

    val qc = QueryContext(query, None, NoMetricCollector)
    val calc = qc.calculator
    val builder = new InternalRowBuilder(qc)

    val row = builder
      .set(Time(now.minusDays(2)))
      .set(dimension(TestDims.DIM_A), "Вкусная водичка №7")
      .buildAndReset()

    val result = calc.evaluateExpressions(RussianTokenizer, row)
    result.get(qc, LengthExpr(dimension(TestDims.DIM_A))) shouldEqual 18
    result.get(qc, split(dimension(TestDims.DIM_A))) should contain theSameElementsInOrderAs List(
      "Вкусная",
      "водичка",
      "7"
    )
    result.get(qc, tokens(dimension(TestDims.DIM_A))) should contain theSameElementsInOrderAs List(
      "vkusn",
      "vodichk",
      "№7"
    )
    result.get(qc, upper(dimension(TestDims.DIM_A))) shouldEqual "ВКУСНАЯ ВОДИЧКА №7"
    result.get(qc, lower(dimension(TestDims.DIM_A))) shouldEqual "вкусная водичка №7"
    result.get(qc, ConcatExpr(dimension(TestDims.DIM_A), const("!!!"))) shouldEqual "Вкусная водичка №7!!!"
  }

  it should "evaluate array functions" in {
    val now = DateTime.now()
    val query = Query(
      TestSchema.testTable,
      const(Time(now.minusDays(3))),
      const(Time(now)),
      Seq(
        arrayLength(tokens(dimension(TestDims.DIM_A))) as "len",
        contains(tokens(dimension(TestDims.DIM_A)), const("vodichk")) as "c1",
        containsAll(tokens(dimension(TestDims.DIM_A)), array(const("vkusn"), const("vodichk"))) as "c2",
        containsAny(tokens(dimension(TestDims.DIM_A)), array(const("ochen"), const("vkusn"), const("vodichk"))) as "c3",
        containsSame(tokens(dimension(TestDims.DIM_A)), array(const("vkusn"), const("vodichk"))) as "c4"
      )
    )

    val qc = QueryContext(query, None, NoMetricCollector)
    val calc = qc.calculator
    val builder = new InternalRowBuilder(qc)

    val row = builder
      .set(Time(now.minusDays(2)))
      .set(dimension(TestDims.DIM_A), "Вкусная водичка №7")
      .buildAndReset()

    val result = calc.evaluateExpressions(RussianTokenizer, row)
    result.get(qc, arrayLength(tokens(dimension(TestDims.DIM_A)))) shouldEqual 3
    result.get(qc, contains(tokens(dimension(TestDims.DIM_A)), const("vodichk"))) shouldEqual true
    result.get(qc, containsAll(tokens(dimension(TestDims.DIM_A)), array(const("vkusn"), const("vodichk")))) shouldEqual true
    result.get(
      qc,
      containsAny(tokens(dimension(TestDims.DIM_A)), array(const("ochen"), const("vkusn"), const("vodichk")))
    ) shouldEqual true
    result.get(qc, containsSame(tokens(dimension(TestDims.DIM_A)), array(const("vkusn"), const("vodichk")))) shouldEqual false
  }

  it should "support comparing of non-numeric types" in {
    val now = DateTime.now()
    val query = Query(
      TestSchema.testTable,
      const(Time(now.minusDays(3))),
      const(Time(now)),
      Seq(
        min(time) as "min_time",
        dimension(TestDims.DIM_A) as "A"
      ),
      None,
      Seq(dimension(TestDims.DIM_A)),
      None,
      Some(lt(min(time), const(Time(now.minusMonths(1)))))
    )

    val qc = QueryContext(query, None, NoMetricCollector)
    val calc = qc.calculator

    val builder = new InternalRowBuilder(qc)

    val row1 = builder
      .set(Time(now.minusDays(2)))
      .set(dimension(TestDims.DIM_A), "AAA")
      .buildAndReset()

    val row2 = builder
      .set(Time(now.minusDays(32)))
      .set(dimension(TestDims.DIM_A), "AAA")
      .buildAndReset()

    val row3 = builder
      .set(Time(now.minusDays(35)))
      .set(dimension(TestDims.DIM_A), "BBB")
      .buildAndReset()

    val rows = Seq(row1, row2, row3).groupBy(_.get(qc, dimension(TestDims.DIM_A)))
    val mapped = rows.map { case (s, rs)    => s -> rs.map(r => calc.evaluateMap(RussianTokenizer, r)) }
    val reduced = mapped.map { case (_, rs) => rs.reduce((a, b) => calc.evaluateReduce(RussianTokenizer, a, b)) }
    val postMapped = reduced.map(r => calc.evaluatePostMap(RussianTokenizer, r))

    val postFiltered = postMapped
      .filter(r => calc.evaluatePostFilter(RussianTokenizer, r))
      .toList
      .sortBy(_.get(qc, dimension(TestDims.DIM_A)))

    postFiltered should have size 2
    postFiltered(0).get(qc, dimension(TestDims.DIM_A)) shouldEqual "AAA"
    postFiltered(0).get(qc, min(time)) shouldEqual Time(now.minusDays(32))

    postFiltered(1).get(qc, dimension(TestDims.DIM_A)) shouldEqual "BBB"
    postFiltered(1).get(qc, min(time)) shouldEqual Time(now.minusDays(35))
  }

  it should "support arrays" in {
    val now = DateTime.now()
    val ce = condition(
      containsAny(tokens(dimension(TestDims.DIM_A)), const(Seq("aaa", "bbb"))),
      const("X"),
      const("Y")
    )

    val query = Query(
      TestSchema.testTable,
      const(Time(now.minusDays(3))),
      const(Time(now)),
      Seq(
        min(time) as "min_time",
        ce as "X_OR_Y"
      ),
      None,
      Seq(dimension(TestDims.DIM_A)),
      None,
      Some(lt(min(time), const(Time(now.minusMonths(1)))))
    )

    val qc = QueryContext(query, None, NoMetricCollector)
    val calc = qc.calculator

    val builder = new InternalRowBuilder(qc)

    val row1 = calc.evaluateExpressions(
      RussianTokenizer,
      builder
        .set(Time(now.minusDays(2)))
        .set(dimension(TestDims.DIM_A), "aaa")
        .buildAndReset()
    )

    row1.get(qc, ce) shouldEqual "X"

    val row2 = calc.evaluateExpressions(
      RussianTokenizer,
      builder
        .set(Time(now.minusDays(32)))
        .set(dimension(TestDims.DIM_A), "ggg")
        .buildAndReset()
    )

    row2.get(qc, ce) shouldEqual "Y"
  }

  it should "handle nulls properly" in {
    val now = DateTime.now()

    val exp = divFrac(plus(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2)), const(2d))
    val cond = equ(exp, const(0d))

    val query = Query(
      TestSchema.testTable,
      const(Time(now.minusDays(3))),
      const(Time(now)),
      Seq(
        metric(TestTableFields.TEST_FIELD) as "F",
        metric(TestTableFields.TEST_FIELD2) as "F2"
      )
    )

    val qc = QueryContext(query, Some(cond), NoMetricCollector)
    val calc = qc.calculator

    val builder = new InternalRowBuilder(qc)

    calc.evaluateFilter(
      RussianTokenizer,
      builder
        .set(Time(now.minusDays(1)))
        .set(metric(TestTableFields.TEST_FIELD2), 42d)
        .buildAndReset()
    ) shouldBe false
  }

  it should "handle large input data arrays" in {
    val now = DateTime.now()
    val cond = in(metric(TestTableFields.TEST_LONG_FIELD), (1L to 1000000L).toSet)

    val query = Query(
      TestSchema.testTable,
      const(Time(now.minusDays(3))),
      const(Time(now)),
      Seq(
        metric(TestTableFields.TEST_FIELD) as "F"
      )
    )

    val qc = QueryContext(query, Some(cond), NoMetricCollector)
    val calc = qc.calculator

    val builder = new InternalRowBuilder(qc)

    calc.evaluateFilter(
      RussianTokenizer,
      builder
        .set(Time(now.minusDays(1)))
        .set(metric(TestTableFields.TEST_FIELD), 42d)
        .set(metric(TestTableFields.TEST_LONG_FIELD), 2234512L)
        .buildAndReset()
    ) shouldBe false
  }
}

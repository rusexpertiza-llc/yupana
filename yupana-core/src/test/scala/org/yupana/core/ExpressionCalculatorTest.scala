package org.yupana.core

import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.threeten.extra.PeriodDuration
import org.yupana.api.Time
import org.yupana.api.query.{ ConcatExpr, LengthExpr, NullExpr, Query }
import org.yupana.api.types.DataType
import org.yupana.core.model.InternalRowBuilder
import org.yupana.utils.RussianTokenizer

import java.time.temporal.{ ChronoUnit, TemporalAdjusters }
import java.time.{ Duration, OffsetDateTime, Period, ZoneOffset }

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

    val qc = new QueryContext(query, Some(cond), ExpressionCalculatorFactory)
    val calc = qc.calculator

    val builder = new InternalRowBuilder(qc)

    calc.evaluateFilter(
      RussianTokenizer,
      builder
        .set(Time(OffsetDateTime.now()))
        .set(dimension(TestDims.DIM_A), "значение")
        .set(dimension(TestDims.DIM_B), 12.toShort)
        .buildAndReset()
    ) shouldBe false

    calc.evaluateFilter(
      RussianTokenizer,
      builder
        .set(Time(OffsetDateTime.now()))
        .set(dimension(TestDims.DIM_A), "value")
        .set(dimension(TestDims.DIM_B), 42.toShort)
        .buildAndReset()
    ) shouldBe true
  }

  it should "evaluate row values" in {
    val now = OffsetDateTime.now()
    val query = Query(
      TestSchema.testTable,
      const(Time(now.minusDays(3))),
      const(Time(now)),
      Seq(
        metric(TestTableFields.TEST_FIELD) as "F",
        truncMonth(time) as "T",
        divFrac(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2)) as "PRICE",
        plus(dimension(TestDims.DIM_B), const(1.toShort)) as "B_PLUS_1",
        divInt(dimension(TestDims.DIM_B), plus(dimension(TestDims.DIM_B), const(1.toShort))) as "bbb",
        divInt(dimension(TestDims.DIM_B), plus(dimension(TestDims.DIM_B), const(1.toShort))) as "bbb_2"
      )
    )

    val qc = new QueryContext(query, None, ExpressionCalculatorFactory)
    val calc = qc.calculator

    val builder = new InternalRowBuilder(qc)

    val row = builder
      .set(Time(now.minusDays(2)))
      .set(metric(TestTableFields.TEST_FIELD), 10d)
      .set(metric(TestTableFields.TEST_FIELD2), 5d)
      .buildAndReset()

    calc.evaluateExpressions(RussianTokenizer, row)
    row.get(qc, divFrac(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2))) shouldEqual 2d
    row.get(qc, truncMonth(time)) shouldEqual Time(
      now
        .withOffsetSameInstant(ZoneOffset.UTC)
        .minusDays(2)
        .`with`(TemporalAdjusters.firstDayOfMonth())
        .truncatedTo(ChronoUnit.DAYS)
    )

    val rowWithNulls = builder
      .set(Time(now.minusDays(1)))
      .set(metric(TestTableFields.TEST_FIELD), 3d)
      .buildAndReset()

    calc.evaluateExpressions(RussianTokenizer, rowWithNulls)
    rowWithNulls.get(
      qc,
      divFrac(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2))
    ) shouldEqual null
      .asInstanceOf[java.lang.Double]
  }

  it should "calculate aggregation" in {
    Given("Query with aggregate expressions")

    val now = OffsetDateTime.now()
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
        hllCount(metric(TestTableFields.TEST_LONG_FIELD), 0.01) as "HLL",
        distinctRandom(metric(TestTableFields.TEST_FIELD)) as "RANDOM",
        truncDay(time) as "T",
        min(divFrac(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2))) as "MIN_PRICE",
        divFrac(
          plus(max(metric(TestTableFields.TEST_FIELD)), min(metric(TestTableFields.TEST_FIELD))),
          const(2d)
        ) as "MIDDLE"
      ),
      None,
      Seq(truncDay(time))
    )

    val qc = new QueryContext(query, None, ExpressionCalculatorFactory)
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

    val row22 = builder
      .set(Time(now.minusDays(1)))
      .set(metric(TestTableFields.TEST_FIELD), 2d)
      .set(metric(TestTableFields.TEST_FIELD2), 3d)
      .set(metric(TestTableFields.TEST_STRING_FIELD), "bar")
      .buildAndReset()

    val mapped1 = calc.evaluateZero(RussianTokenizer, calc.evaluateExpressions(RussianTokenizer, row1))
    val mapped2 = calc.evaluateZero(RussianTokenizer, calc.evaluateExpressions(RussianTokenizer, row2))
    Then("fields filled with map phase values")
    mapped1.get(qc, sum(metric(TestTableFields.TEST_FIELD))) shouldEqual 10d
    mapped1.get(qc, max(metric(TestTableFields.TEST_FIELD))) shouldEqual 10d
    mapped1.get(qc, min(metric(TestTableFields.TEST_FIELD))) shouldEqual 10d
    mapped1.get(qc, count(metric(TestTableFields.TEST_FIELD))) shouldEqual 1L
    mapped1.get(qc, count(metric(TestTableFields.TEST_STRING_FIELD))) shouldEqual 0L
    mapped1.get(qc, distinctCount(metric(TestTableFields.TEST_FIELD))) shouldEqual Set(10d)
    mapped1.get(qc, distinctRandom(metric(TestTableFields.TEST_FIELD))) shouldEqual Set(10d)
    mapped1.get(
      qc,
      min(divFrac(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2)))
    ) shouldEqual 2d
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
    mapped2.get(
      qc,
      min(divFrac(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2)))
    ) shouldEqual 3d
    mapped2.isEmpty(
      qc,
      divFrac(plus(max(metric(TestTableFields.TEST_FIELD)), min(metric(TestTableFields.TEST_FIELD))), const(2d))
    ) shouldBe true

    When("fold is called")

    val folded2 = calc.evaluateSequence(RussianTokenizer, mapped2, calc.evaluateExpressions(RussianTokenizer, row22))
    folded2.get(qc, sum(metric(TestTableFields.TEST_FIELD))) shouldEqual 14d
    folded2.get(qc, max(metric(TestTableFields.TEST_FIELD))) shouldEqual 12d
    folded2.get(qc, min(metric(TestTableFields.TEST_FIELD))) shouldEqual 2d
    folded2.get(qc, count(metric(TestTableFields.TEST_FIELD))) shouldEqual 2L
    folded2.get(qc, count(metric(TestTableFields.TEST_STRING_FIELD))) shouldEqual 2L
    folded2.get(qc, distinctCount(metric(TestTableFields.TEST_FIELD))) shouldEqual Set(2d, 12d)
    folded2.get(qc, distinctRandom(metric(TestTableFields.TEST_FIELD))) shouldEqual Set(2d, 12d)
    folded2.get(
      qc,
      min(divFrac(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2)))
    ) shouldEqual 2d / 3d
    folded2.isEmpty(
      qc,
      divFrac(plus(max(metric(TestTableFields.TEST_FIELD)), min(metric(TestTableFields.TEST_FIELD))), const(2d))
    ) shouldBe true

    When("reduce called")
    val reduced = calc.evaluateCombine(RussianTokenizer, mapped1, folded2)
    Then("reduced values shall be calculated")
    reduced.get(qc, sum(metric(TestTableFields.TEST_FIELD))) shouldEqual 24d
    reduced.get(qc, max(metric(TestTableFields.TEST_FIELD))) shouldEqual 12d
    reduced.get(qc, min(metric(TestTableFields.TEST_FIELD))) shouldEqual 2d
    reduced.get(qc, count(metric(TestTableFields.TEST_FIELD))) shouldEqual 3L
    reduced.get(qc, count(metric(TestTableFields.TEST_STRING_FIELD))) shouldEqual 2L
    reduced.get(qc, distinctCount(metric(TestTableFields.TEST_FIELD))) shouldEqual Set(2d, 10d, 12d)
    reduced.get(qc, distinctRandom(metric(TestTableFields.TEST_FIELD))) shouldEqual Set(2d, 10d, 12d)
    reduced.get(
      qc,
      min(divFrac(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2)))
    ) shouldEqual 2d / 3d
    reduced.isEmpty(
      qc,
      divFrac(plus(max(metric(TestTableFields.TEST_FIELD)), min(metric(TestTableFields.TEST_FIELD))), const(2d))
    ) shouldBe true

    When("postMap called")
    val postMapped = calc.evaluatePostMap(RussianTokenizer, reduced)
    Then("post map calculations shall be performed")
    postMapped.get(qc, sum(metric(TestTableFields.TEST_FIELD))) shouldEqual 24d
    postMapped.get(qc, max(metric(TestTableFields.TEST_FIELD))) shouldEqual 12d
    postMapped.get(qc, min(metric(TestTableFields.TEST_FIELD))) shouldEqual 2d
    postMapped.get(qc, count(metric(TestTableFields.TEST_FIELD))) shouldEqual 3L
    postMapped.get(qc, count(metric(TestTableFields.TEST_STRING_FIELD))) shouldEqual 2L
    postMapped.get(qc, distinctCount(metric(TestTableFields.TEST_FIELD))) shouldEqual 3
    postMapped.get(qc, distinctRandom(metric(TestTableFields.TEST_FIELD))) should (equal(2d) or equal(10d) or equal(
      12d
    ))
    postMapped.get(
      qc,
      min(divFrac(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2)))
    ) shouldEqual 2d / 3d
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
    ) shouldEqual 7d
  }

  it should "evaluate string functions" in {
    val now = OffsetDateTime.now()
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

    val qc = new QueryContext(query, None, ExpressionCalculatorFactory)
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
    val now = OffsetDateTime.now()
    val query = Query(
      TestSchema.testTable,
      const(Time(now.minusDays(3))),
      const(Time(now)),
      Seq(
        arrayLength(tokens(dimension(TestDims.DIM_A))) as "len",
        contains(tokens(dimension(TestDims.DIM_A)), const("vodichk")) as "c1",
        containsAll(tokens(dimension(TestDims.DIM_A)), array(const("vkusn"), const("vodichk"))) as "c2",
        containsAny(tokens(dimension(TestDims.DIM_A)), array(const("ochen"), const("vkusn"), const("vodichk"))) as "c3",
        containsSame(tokens(dimension(TestDims.DIM_A)), array(const("vkusn"), const("vodichk"))) as "c4",
        arrayToString(tokenizeArray(split(dimension(TestDims.DIM_A)))) as "ats"
      )
    )

    val qc = new QueryContext(query, None, ExpressionCalculatorFactory)
    val calc = qc.calculator
    val builder = new InternalRowBuilder(qc)

    val row = builder
      .set(Time(now.minusDays(2)))
      .set(dimension(TestDims.DIM_A), "Вкусная водичка №7")
      .buildAndReset()

    val result = calc.evaluateExpressions(RussianTokenizer, row)
    result.get(qc, arrayLength(tokens(dimension(TestDims.DIM_A)))) shouldEqual 3
    result.get(qc, contains(tokens(dimension(TestDims.DIM_A)), const("vodichk"))) shouldEqual true
    result.get(
      qc,
      containsAll(tokens(dimension(TestDims.DIM_A)), array(const("vkusn"), const("vodichk")))
    ) shouldEqual true
    result.get(
      qc,
      containsAny(tokens(dimension(TestDims.DIM_A)), array(const("ochen"), const("vkusn"), const("vodichk")))
    ) shouldEqual true
    result.get(
      qc,
      containsSame(tokens(dimension(TestDims.DIM_A)), array(const("vkusn"), const("vodichk")))
    ) shouldEqual false

    result.get(qc, arrayToString(tokenizeArray(split(dimension(TestDims.DIM_A))))) shouldEqual "vkusn, vodichk, 7"
  }

  it should "calculate time functions" in {
    val pointTime = OffsetDateTime.of(2021, 12, 24, 16, 52, 22, 123, ZoneOffset.UTC)
    val query = Query(
      TestSchema.testTable,
      const(Time(pointTime.minusDays(1))),
      const(Time(pointTime.plusDays(1))),
      Seq(
        truncYear(time) as "ty",
        truncQuarter(time) as "tq",
        truncMonth(time) as "tM",
        truncWeek(time) as "tw",
        truncDay(time) as "td",
        truncHour(time) as "th",
        truncMinute(time) as "tm",
        truncSecond(time) as "ts",
        extractYear(time) as "ey",
        extractMonth(time) as "eM",
        extractDay(time) as "ed",
        extractHour(time) as "eh",
        extractMinute(time) as "em",
        extractSecond(time) as "es",
        minus(time, const(Time(pointTime.minusHours(1)))) as "mt",
        minus(time, const(PeriodDuration.of(Period.ofWeeks(2)))) as "mp",
        plus(time, const(PeriodDuration.of(Duration.ofHours(12)))) as "pp"
      )
    )

    val qc = new QueryContext(query, None, ExpressionCalculatorFactory)
    val calc = qc.calculator
    val builder = new InternalRowBuilder(qc)

    val row = builder.set(Time(pointTime)).buildAndReset()

    val result = calc.evaluateExpressions(RussianTokenizer, row)

    result.get(qc, truncYear(time)) shouldEqual Time(OffsetDateTime.of(2021, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC))
    result.get(qc, truncQuarter(time)) shouldEqual Time(OffsetDateTime.of(2021, 10, 1, 0, 0, 0, 0, ZoneOffset.UTC))
    result.get(qc, truncMonth(time)) shouldEqual Time(OffsetDateTime.of(2021, 12, 1, 0, 0, 0, 0, ZoneOffset.UTC))
    result.get(qc, truncWeek(time)) shouldEqual Time(OffsetDateTime.of(2021, 12, 20, 0, 0, 0, 0, ZoneOffset.UTC))
    result.get(qc, truncDay(time)) shouldEqual Time(OffsetDateTime.of(2021, 12, 24, 0, 0, 0, 0, ZoneOffset.UTC))
    result.get(qc, truncHour(time)) shouldEqual Time(OffsetDateTime.of(2021, 12, 24, 16, 0, 0, 0, ZoneOffset.UTC))
    result.get(qc, truncMinute(time)) shouldEqual Time(OffsetDateTime.of(2021, 12, 24, 16, 52, 0, 0, ZoneOffset.UTC))
    result.get(qc, truncSecond(time)) shouldEqual Time(OffsetDateTime.of(2021, 12, 24, 16, 52, 22, 0, ZoneOffset.UTC))

    result.get(qc, extractYear(time)) shouldEqual 2021
    result.get(qc, extractMonth(time)) shouldEqual 12
    result.get(qc, extractDay(time)) shouldEqual 24
    result.get(qc, extractHour(time)) shouldEqual 16
    result.get(qc, extractMinute(time)) shouldEqual 52
    result.get(qc, extractSecond(time)) shouldEqual 22

    result.get(qc, minus(time, const(Time(pointTime.minusHours(1))))) shouldEqual 3600000
    result.get(qc, minus(time, const(PeriodDuration.of(Period.ofWeeks(2))))) shouldEqual Time(
      OffsetDateTime.of(2021, 12, 10, 16, 52, 22, 123, ZoneOffset.UTC)
    )
    result.get(qc, plus(time, const(PeriodDuration.of(Duration.ofHours(12))))) shouldEqual Time(
      OffsetDateTime.of(2021, 12, 25, 4, 52, 22, 123, ZoneOffset.UTC)
    )
  }

  it should "handle tuples" in {
    val now = OffsetDateTime.now()
    val query = Query(
      TestSchema.testTable,
      const(Time(now.minusDays(3))),
      const(Time(now)),
      Seq(
        tuple(dimension(TestDims.DIM_A), minus(metric(TestTableFields.TEST_FIELD))) as "tuple"
      )
    )

    val qc = new QueryContext(query, None, ExpressionCalculatorFactory)
    val calc = qc.calculator
    val builder = new InternalRowBuilder(qc)

    val row = builder
      .set(Time(now.minusHours(1)))
      .set(dimension(TestDims.DIM_A), "a value")
      .set(metric(TestTableFields.TEST_FIELD), 42d)
      .buildAndReset()

    val result = calc.evaluateExpressions(RussianTokenizer, row)

    result.get(qc, tuple(dimension(TestDims.DIM_A), minus(metric(TestTableFields.TEST_FIELD)))) shouldEqual (
      (
        "a value",
        -42d
      )
    )
  }

  it should "support comparing of non-numeric types" in {
    val now = OffsetDateTime.now()
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

    val qc = new QueryContext(query, None, ExpressionCalculatorFactory)
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
    val mapped = rows.map { case (s, rs) => s -> rs.map(r => calc.evaluateZero(RussianTokenizer, r)) }
    val reduced = mapped.map { case (_, rs) => rs.reduce((a, b) => calc.evaluateCombine(RussianTokenizer, a, b)) }
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
    val now = OffsetDateTime.now()
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

    val qc = new QueryContext(query, None, ExpressionCalculatorFactory)
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
    val now = OffsetDateTime.now()

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

    val qc = new QueryContext(query, Some(cond), ExpressionCalculatorFactory)
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
    val now = OffsetDateTime.now()
    val cond = in(metric(TestTableFields.TEST_LONG_FIELD), (1L to 1000000L).toSet)

    val query = Query(
      TestSchema.testTable,
      const(Time(now.minusDays(3))),
      const(Time(now)),
      Seq(
        metric(TestTableFields.TEST_FIELD) as "F"
      )
    )

    val qc = new QueryContext(query, Some(cond), ExpressionCalculatorFactory)
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

  it should "not evaluate conditional branch if not needed" in {
    val now = OffsetDateTime.now()

    val x = condition(
      neq(metric(TestTableFields.TEST_LONG_FIELD), const(0L)),
      divInt(dimension(TestDims.DIM_Y), metric(TestTableFields.TEST_LONG_FIELD)),
      const(-1L)
    )

    val query = Query(
      TestSchema.testTable2,
      const(Time(now.minusDays(3))),
      const(Time(now)),
      Seq(x as "x")
    )

    val qc = new QueryContext(query, None, ExpressionCalculatorFactory)
    val calc = qc.calculator

    val builder = new InternalRowBuilder(qc)

    val row = calc.evaluateExpressions(
      RussianTokenizer,
      builder
        .set(Time(now.minusHours(5)))
        .set(metric(TestTableFields.TEST_LONG_FIELD), 0L)
        .set(dimension(TestDims.DIM_Y), 3L)
        .buildAndReset()
    )

    row.get(qc, x) shouldEqual -1L
  }

  it should "handle conditions on aggregates" in {
    val now = OffsetDateTime.now()

    val x = condition(
      neq(sum(metric(TestTableFields.TEST_LONG_FIELD)), const(0L)),
      divInt(sum(dimension(TestDims.DIM_Y)), sum(metric(TestTableFields.TEST_LONG_FIELD))),
      const(-1L)
    )

    val query = Query(
      TestSchema.testTable2,
      const(Time(now.minusDays(3))),
      const(Time(now)),
      Seq(x as "x")
    )

    val qc = new QueryContext(query, None, ExpressionCalculatorFactory)

    val builder = new InternalRowBuilder(qc)

    val rows = Seq(
      builder
        .set(Time(now.minusHours(1)))
        .set(metric(TestTableFields.TEST_LONG_FIELD), 2L)
        .set(dimension(TestDims.DIM_Y), 4L)
        .buildAndReset(),
      builder
        .set(Time(now.minusHours(1)))
        .set(metric(TestTableFields.TEST_LONG_FIELD), -2L)
        .set(dimension(TestDims.DIM_Y), 2L)
        .buildAndReset()
    )

    val evaluated = rows.map(qc.calculator.evaluateExpressions(RussianTokenizer, _))

    val mapped = evaluated.map(qc.calculator.evaluateZero(RussianTokenizer, _))
    val reduced = mapped.reduce((a, b) => qc.calculator.evaluateCombine(RussianTokenizer, a, b))
    val postMapped = qc.calculator.evaluatePostMap(RussianTokenizer, reduced)

    postMapped.get(qc, sum(metric(TestTableFields.TEST_LONG_FIELD))) shouldEqual 0L
    postMapped.get(qc, sum(dimension(TestDims.DIM_Y))) shouldEqual 6L

    val result = qc.calculator.evaluatePostAggregateExprs(RussianTokenizer, postMapped)

    result.get(qc, x) shouldEqual -1L
  }

  it should "Support type conversions" in {
    val now = OffsetDateTime.now()

    val x = plus(metric(TestTableFields.TEST_FIELD), byte2Double(metric(TestTableFields.TEST_BYTE_FIELD)))

    val y = plus(metric(TestTableFields.TEST_LONG_FIELD), short2Long(dimension(TestDims.DIM_B)))
    val z =
      divFrac(double2bigDecimal(metric(TestTableFields.TEST_FIELD)), metric(TestTableFields.TEST_BIGDECIMAL_FIELD))

    val query = Query(
      TestSchema.testTable,
      const(Time(now.minusDays(3))),
      const(Time(now)),
      Seq(x as "x", y as "y", z as "z")
    )

    val qc = new QueryContext(query, None, ExpressionCalculatorFactory)

    val builder = new InternalRowBuilder(qc)

    val row = builder
      .set(Time(now.minusDays(2)))
      .set(metric(TestTableFields.TEST_FIELD), 10d)
      .set(metric(TestTableFields.TEST_BYTE_FIELD), 1.toByte)
      .set(dimension(TestDims.DIM_B), 7.toShort)
      .set(metric(TestTableFields.TEST_LONG_FIELD), 3L)
      .buildAndReset()

    val result = qc.calculator.evaluateExpressions(RussianTokenizer, row)

    result.get(qc, x) shouldEqual 11d
    result.get(qc, y) shouldEqual 10L
    result.isEmpty(qc, z) shouldBe true
  }

  it should "handle nulls in case when in aggregation" in {
    val now = OffsetDateTime.now()

    val a = sum(
      condition(
        gt(metric(TestTableFields.TEST_FIELD), const(3d)),
        double2bigDecimal(metric(TestTableFields.TEST_FIELD)),
        metric(TestTableFields.TEST_BIGDECIMAL_FIELD)
      )
    )

    val query = Query(TestSchema.testTable, const(Time(now.minusDays(5))), const(Time(now.minusDays(2))), Seq(a as "a"))
    val qc = new QueryContext(query = query, postCondition = None, calculatorFactory = ExpressionCalculatorFactory)

    val builder = new InternalRowBuilder(qc)

    val r1 = builder
      .set(Time(now.minusDays(4)))
      .set(metric(TestTableFields.TEST_FIELD), 5d)
      .set(metric(TestTableFields.TEST_BIGDECIMAL_FIELD), BigDecimal(4))
      .buildAndReset()

    val r2 = builder
      .set(Time(now.minusDays(3)))
      .set(metric(TestTableFields.TEST_FIELD), 1d)
      .buildAndReset()

    val e1 = qc.calculator.evaluateExpressions(RussianTokenizer, r1)
    val e2 = qc.calculator.evaluateExpressions(RussianTokenizer, r2)

    val z = qc.calculator.evaluateZero(RussianTokenizer, e1)

    val s = qc.calculator.evaluateSequence(RussianTokenizer, z, e2)

    val result = qc.calculator.evaluatePostMap(RussianTokenizer, s)

    result.get(qc, a) shouldEqual BigDecimal(5)
  }

  it should "handle null literals in case when in aggregation" in {
    val now = OffsetDateTime.now()

    val a = sum(
      condition(
        gt(dimension(TestDims.DIM_B), const(3.toShort)),
        divFrac(short2BigDecimal(dimension(TestDims.DIM_B)), metric(TestTableFields.TEST_BIGDECIMAL_FIELD)),
        NullExpr[BigDecimal](DataType[BigDecimal])
      )
    )

    val query = Query(TestSchema.testTable, const(Time(now.minusDays(5))), const(Time(now.minusDays(2))), Seq(a as "a"))
    val qc = new QueryContext(query = query, postCondition = None, calculatorFactory = ExpressionCalculatorFactory)

    val builder = new InternalRowBuilder(qc)

    val r1 = builder
      .set(Time(now.minusDays(4)))
      .set(dimension(TestDims.DIM_B), 2.toShort)
      .set(metric(TestTableFields.TEST_BIGDECIMAL_FIELD), BigDecimal(5))
      .buildAndReset()

    val r2 = builder
      .set(Time(now.minusDays(3)))
      .set(dimension(TestDims.DIM_B), 5.toShort)
      .set(metric(TestTableFields.TEST_BIGDECIMAL_FIELD), BigDecimal(2))
      .buildAndReset()

    val e1 = qc.calculator.evaluateExpressions(RussianTokenizer, r1)
    val e2 = qc.calculator.evaluateExpressions(RussianTokenizer, r2)

    val z = qc.calculator.evaluateZero(RussianTokenizer, e1)

    val s = qc.calculator.evaluateSequence(RussianTokenizer, z, e2)

    val result = qc.calculator.evaluatePostMap(RussianTokenizer, s)

    result.get(qc, a) shouldEqual BigDecimal(2.5)
  }
}

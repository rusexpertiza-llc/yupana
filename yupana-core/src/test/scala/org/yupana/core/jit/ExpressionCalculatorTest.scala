package org.yupana.core.jit

import com.twitter.algebird.HyperLogLogAggregator
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.threeten.extra.PeriodDuration
import org.yupana.api.Time
import org.yupana.api.query.{ LengthExpr, NullExpr, Query }
import org.yupana.api.types.DataType
import org.yupana.core.model.{ BatchDataset, HashTableDataset }
import org.yupana.core.utils.metric.NoMetricCollector
import org.yupana.core.QueryContext
import org.yupana.testutils.{ TestDims, TestSchema, TestTableFields }

import java.time.temporal.{ ChronoUnit, TemporalAdjusters }
import java.time.{ Duration, LocalDateTime, OffsetDateTime, Period, ZoneOffset }

class ExpressionCalculatorTest extends AnyFlatSpec with Matchers with GivenWhenThen {
  import org.yupana.api.query.syntax.All._

  private val tokenizer = TestSchema.schema.tokenizer

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

    val qc = new QueryContext(query, Some(cond), tokenizer, JIT, NoMetricCollector)
    val calc = qc.calculator

    val batch = BatchDataset(qc)

    batch.set(0, Time(OffsetDateTime.now()))
    batch.set(0, dimension(TestDims.DIM_A), "значение")
    batch.set(0, dimension(TestDims.DIM_B), 12.toShort)

    batch.set(1, Time(OffsetDateTime.now()))
    batch.set(1, dimension(TestDims.DIM_A), "value")
    batch.set(1, dimension(TestDims.DIM_B), 42.toShort)

    calc.evaluateFilter(batch, Time(LocalDateTime.now()), IndexedSeq.empty)

    batch.isDeleted(0) shouldEqual true
    batch.isDeleted(1) shouldEqual false
  }

  it should "evaluate expressions" in {
    val now = OffsetDateTime.now()
    val query = Query(
      TestSchema.testTable,
      const(Time(now.minusDays(3))),
      const(Time(now)),
      Seq(
        metric(TestTableFields.TEST_FIELD) as "F",
        truncMonth(time) as "T",
        div(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2)) as "PRICE",
        plus(dimension(TestDims.DIM_B), const(1.toShort)) as "B_PLUS_1",
        div(dimension(TestDims.DIM_B), plus(dimension(TestDims.DIM_B), const(1.toShort))) as "bbb",
        div(dimension(TestDims.DIM_B), plus(dimension(TestDims.DIM_B), const(1.toShort))) as "bbb_2"
      )
    )

    val qc = new QueryContext(query, None, tokenizer, JIT, NoMetricCollector)
    val calc = qc.calculator

    val batch = BatchDataset(qc)

    batch.set(0, Time(now.minusDays(2)))
    batch.set(0, metric(TestTableFields.TEST_FIELD), 10d)
    batch.set(0, metric(TestTableFields.TEST_FIELD2), 5d)

    batch.set(1, Time(now.minusDays(1)))
    batch.set(1, metric(TestTableFields.TEST_FIELD), 3d)

    calc.evaluateExpressions(batch, Time(now), IndexedSeq.empty)
    batch.get(0, div(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2))) shouldEqual 2d
    batch.get(0, truncMonth(time)) shouldEqual Time(
      now
        .withOffsetSameInstant(ZoneOffset.UTC)
        .minusDays(2)
        .`with`(TemporalAdjusters.firstDayOfMonth())
        .truncatedTo(ChronoUnit.DAYS)
    )

    batch.isNull(
      1,
      div(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2))
    ) shouldBe true
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
        min(div(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2))) as "MIN_PRICE",
        div(
          plus(max(metric(TestTableFields.TEST_FIELD)), min(metric(TestTableFields.TEST_FIELD))),
          const(2d)
        ) as "MIDDLE"
      ),
      None,
      Seq(truncDay(time))
    )

    val qc = new QueryContext(query, None, tokenizer, JIT, NoMetricCollector)
    val calc = qc.calculator

    When("incoming dataset contains only one row")

    val batch1 = BatchDataset(qc)
    val acc1 = HashTableDataset(qc)

    batch1.set(0, Time(now.minusDays(1)))
    batch1.set(0, metric(TestTableFields.TEST_FIELD), 10d)
    batch1.set(0, metric(TestTableFields.TEST_FIELD2), 5d)

    calc.evaluateExpressions(batch1, Time(now), IndexedSeq.empty)
    calc.evaluateFold(acc1, batch1, Time(now), IndexedSeq.empty)

    Then("fields filled with initial values (row or zero value depending on aggregate function)")

    val expectedTime1 = Time(now.toInstant.atOffset(ZoneOffset.UTC).minusDays(1).truncatedTo(ChronoUnit.DAYS))

    val acc1batch = acc1.iterator.next()
    acc1batch.get(0, truncDay(time)) shouldEqual expectedTime1
    acc1batch.isDefined(0, sum(metric(TestTableFields.TEST_FIELD))) shouldEqual true
    acc1batch.get(0, sum(metric(TestTableFields.TEST_FIELD))) shouldEqual 10d
    acc1batch.get(0, max(metric(TestTableFields.TEST_FIELD))) shouldEqual 10d
    acc1batch.get(0, min(metric(TestTableFields.TEST_FIELD))) shouldEqual 10d
    acc1batch.get(0, count(metric(TestTableFields.TEST_FIELD))) shouldEqual 1L
    acc1batch.get(0, count(metric(TestTableFields.TEST_STRING_FIELD))) shouldEqual 0L
    acc1batch.isNullRef(0, distinctCount(metric(TestTableFields.TEST_FIELD))) shouldEqual false
    acc1batch.isNullRef(0, distinctRandom(metric(TestTableFields.TEST_FIELD))) shouldEqual false
    acc1batch.getRef(0, distinctCount(metric(TestTableFields.TEST_FIELD))) shouldEqual Set(10d)
    acc1batch.getRef(0, distinctRandom(metric(TestTableFields.TEST_FIELD))) shouldEqual Set(10d)
    acc1batch.get(
      0,
      min(div(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2)))
    ) shouldEqual 2d
    acc1batch.isNull(
      0,
      div(plus(max(metric(TestTableFields.TEST_FIELD)), min(metric(TestTableFields.TEST_FIELD))), const(2d))
    ) shouldBe true

    When("incoming dataset contains several rows")

    val batch2 = BatchDataset(qc)
    val acc2 = HashTableDataset(qc)

    batch2.set(0, Time(now.minusDays(1)))
    batch2.set(0, metric(TestTableFields.TEST_FIELD), 10d)
    batch2.set(0, metric(TestTableFields.TEST_FIELD2), 5d)

    batch2.set(1, Time(now.minusDays(1)))
    batch2.set(1, metric(TestTableFields.TEST_FIELD), 12d)
    batch2.set(1, metric(TestTableFields.TEST_FIELD2), 4d)
    batch2.set(1, metric(TestTableFields.TEST_STRING_FIELD), "foo")

    batch2.set(2, Time(now.minusDays(1)))
    batch2.set(2, metric(TestTableFields.TEST_FIELD), 2d)
    batch2.set(2, metric(TestTableFields.TEST_FIELD2), 3d)
    batch2.set(2, metric(TestTableFields.TEST_STRING_FIELD), "bar")

    And("fold called")
    calc.evaluateExpressions(batch2, Time(now), IndexedSeq.empty)
    calc.evaluateFold(acc2, batch2, Time(now), IndexedSeq.empty)

    Then("fields filled with aggregated values")
    val expectedTime2 = Time(now.toInstant.atOffset(ZoneOffset.UTC).minusDays(1).truncatedTo(ChronoUnit.DAYS))

    val acc2batch = acc2.iterator.next()

    acc2batch.get(0, truncDay(time)) shouldEqual expectedTime2
    acc2batch.get(0, sum(metric(TestTableFields.TEST_FIELD))) shouldEqual 24d
    acc2batch.get(0, max(metric(TestTableFields.TEST_FIELD))) shouldEqual 12d
    acc2batch.get(0, min(metric(TestTableFields.TEST_FIELD))) shouldEqual 2d
    acc2batch.get(0, count(metric(TestTableFields.TEST_FIELD))) shouldEqual 3L
    acc2batch.get(0, count(metric(TestTableFields.TEST_STRING_FIELD))) shouldEqual 2L
    acc2batch.getRef(0, distinctCount(metric(TestTableFields.TEST_FIELD))) shouldEqual Set(2d, 12d, 10d)
    acc2batch.getRef(0, distinctRandom(metric(TestTableFields.TEST_FIELD))) shouldEqual Set(2d, 12d, 10d)
    acc2batch.get(
      0,
      min(div(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2)))
    ) shouldEqual 2d / 3d
    acc2batch.isNull(
      0,
      div(plus(max(metric(TestTableFields.TEST_FIELD)), min(metric(TestTableFields.TEST_FIELD))), const(2d))
    ) shouldBe true

    When("incoming already aggregated datasets")

    val time3 = Time(now.toInstant.atOffset(ZoneOffset.UTC).minusDays(1).truncatedTo(ChronoUnit.DAYS))

    val acc3 = HashTableDataset(qc)
    val acc3batch = acc3.iterator.next()

    acc3batch.set(0, truncDay(time), time3)
    acc3batch.set(0, sum(metric(TestTableFields.TEST_FIELD)), 13d)
    acc3batch.set(0, max(metric(TestTableFields.TEST_FIELD)), 4d)
    acc3batch.set(0, min(metric(TestTableFields.TEST_FIELD)), 1d)
    acc3batch.set(0, count(metric(TestTableFields.TEST_FIELD)), 3L)
    acc3batch.set(0, count(metric(TestTableFields.TEST_STRING_FIELD)), 1L)
    acc3batch.setRef(0, distinctCount(metric(TestTableFields.TEST_FIELD)), Set(1d, 2d, 3d))
    acc3batch.setRef(0, distinctRandom(metric(TestTableFields.TEST_FIELD)), Set(1d, 2d, 3d))
    acc3batch.set(
      0,
      min(div(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2))),
      2d / 3d
    )
    val hll = HyperLogLogAggregator.withErrorGeneric[Long](0.01d)
    acc3batch.setRef(0, hllCount(metric(TestTableFields.TEST_LONG_FIELD), 0.01), hll.prepare(10))
    acc3.updateKey(calc.createKey(acc3batch, 0), acc3.createPtr(0, 0))

    val batch3 = BatchDataset(qc)

    batch3.set(0, truncDay(time), time3)
    batch3.set(0, sum(metric(TestTableFields.TEST_FIELD)), 17d)
    batch3.set(0, max(metric(TestTableFields.TEST_FIELD)), 6d)
    batch3.set(0, min(metric(TestTableFields.TEST_FIELD)), 2d)
    batch3.set(0, count(metric(TestTableFields.TEST_FIELD)), 17L)
    batch3.set(0, count(metric(TestTableFields.TEST_STRING_FIELD)), 19L)
    batch3.setRef(0, distinctCount(metric(TestTableFields.TEST_FIELD)), Set(1d, 2d, 4d))
    batch3.setRef(0, distinctRandom(metric(TestTableFields.TEST_FIELD)), Set(1d, 2d, 4d))
    batch3.set(
      0,
      min(div(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2))),
      1d / 3d
    )
    batch3.setRef(0, hllCount(metric(TestTableFields.TEST_LONG_FIELD), 0.01), hll.prepare(11))

    calc.evaluateCombine(acc3, batch3)
    Then("combined values shall be calculated")
    acc3batch.get(0, sum(metric(TestTableFields.TEST_FIELD))) shouldEqual 30d
    acc3batch.get(0, max(metric(TestTableFields.TEST_FIELD))) shouldEqual 6d
    acc3batch.get(0, min(metric(TestTableFields.TEST_FIELD))) shouldEqual 1d
    acc3batch.get(0, count(metric(TestTableFields.TEST_FIELD))) shouldEqual 20L
    acc3batch.get(0, count(metric(TestTableFields.TEST_STRING_FIELD))) shouldEqual 20L
    acc3batch.getRef(0, distinctCount(metric(TestTableFields.TEST_FIELD))) shouldEqual Set(1d, 2d, 3d, 4d)
    acc3batch.getRef(0, distinctRandom(metric(TestTableFields.TEST_FIELD))) shouldEqual Set(1d, 2d, 3d, 4d)
    acc3batch.get(
      0,
      min(div(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2)))
    ) shouldEqual 1d / 3d
    acc3batch.isNull(
      0,
      div(plus(max(metric(TestTableFields.TEST_FIELD)), min(metric(TestTableFields.TEST_FIELD))), const(2d))
    ) shouldBe true

    And("and values on post combine stage map shall be calculated")

    val batch4 = acc3.iterator.next()

    calc.evaluatePostCombine(batch4)

    batch4.get(0, sum(metric(TestTableFields.TEST_FIELD))) shouldEqual 30d
    batch4.get(0, max(metric(TestTableFields.TEST_FIELD))) shouldEqual 6d
    batch4.get(0, min(metric(TestTableFields.TEST_FIELD))) shouldEqual 1d
    batch4.get(0, count(metric(TestTableFields.TEST_FIELD))) shouldEqual 20L
    batch4.get(0, count(metric(TestTableFields.TEST_STRING_FIELD))) shouldEqual 20L
    batch4.get(0, distinctCount(metric(TestTableFields.TEST_FIELD))) shouldEqual 4
    batch4.get(
      0,
      distinctRandom(metric(TestTableFields.TEST_FIELD))
    ) should (be(1d) or be(2d) or be(3d) or be(4d))

    batch4.get(
      0,
      min(div(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2)))
    ) shouldEqual 1d / 3d
    batch4.isNull(
      0,
      div(plus(max(metric(TestTableFields.TEST_FIELD)), min(metric(TestTableFields.TEST_FIELD))), const(2d))
    ) shouldBe true

    And("and values for post aggregate expressions shall be calculated")

    calc.evaluatePostAggregateExprs(batch4)

    batch4.get(
      0,
      div(plus(max(metric(TestTableFields.TEST_FIELD)), min(metric(TestTableFields.TEST_FIELD))), const(2d))
    ) shouldEqual 3.5d
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
        plus(dimension(TestDims.DIM_A), const("!!!")) as "yeah"
      )
    )

    val qc = new QueryContext(query, None, tokenizer, JIT, NoMetricCollector)
    val calc = qc.calculator
    val batch = BatchDataset(qc)

    batch.set(0, Time(now.minusDays(2)))
    batch.set(0, dimension(TestDims.DIM_A), "Вкусная водичка №7")

    calc.evaluateExpressions(batch, Time(now), IndexedSeq.empty)

    batch.get(0, LengthExpr(dimension(TestDims.DIM_A))) shouldEqual 18
    batch.get(0, split(dimension(TestDims.DIM_A))) should contain theSameElementsInOrderAs List(
      "Вкусная",
      "водичка",
      "7"
    )
    batch.get(0, tokens(dimension(TestDims.DIM_A))) should contain theSameElementsInOrderAs List(
      "vkusn",
      "vodichk",
      "№7"
    )
    batch.get(0, upper(dimension(TestDims.DIM_A))) shouldEqual "ВКУСНАЯ ВОДИЧКА №7"
    batch.get(0, lower(dimension(TestDims.DIM_A))) shouldEqual "вкусная водичка №7"
    batch.get(0, plus(dimension(TestDims.DIM_A), const("!!!"))) shouldEqual "Вкусная водичка №7!!!"
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

    val qc = new QueryContext(query, None, tokenizer, JIT, NoMetricCollector)
    val calc = qc.calculator

    val batch = BatchDataset(qc)

    batch.set(0, Time(now.minusDays(2)))
    batch.set(0, dimension(TestDims.DIM_A), "Вкусная водичка №7")

    calc.evaluateExpressions(batch, Time(now), IndexedSeq.empty)

    batch.get(0, arrayLength(tokens(dimension(TestDims.DIM_A)))) shouldEqual 3
    batch.get(0, contains(tokens(dimension(TestDims.DIM_A)), const("vodichk"))) shouldEqual true
    batch.get(
      0,
      containsAll(tokens(dimension(TestDims.DIM_A)), array(const("vkusn"), const("vodichk")))
    ) shouldEqual true
    batch.get(
      0,
      containsAny(tokens(dimension(TestDims.DIM_A)), array(const("ochen"), const("vkusn"), const("vodichk")))
    ) shouldEqual true
    batch.get(
      0,
      containsSame(tokens(dimension(TestDims.DIM_A)), array(const("vkusn"), const("vodichk")))
    ) shouldEqual false

    batch.get(0, arrayToString(tokenizeArray(split(dimension(TestDims.DIM_A))))) shouldEqual "vkusn, vodichk, 7"
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

    val qc = new QueryContext(query, None, tokenizer, JIT, NoMetricCollector)
    val calc = qc.calculator

    val batch = BatchDataset(qc)
    batch.set(0, Time(pointTime))

    calc.evaluateExpressions(batch, Time(LocalDateTime.now()), IndexedSeq.empty)

    batch.get(0, truncYear(time)) shouldEqual Time(OffsetDateTime.of(2021, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC))
    batch.get(0, truncQuarter(time)) shouldEqual Time(OffsetDateTime.of(2021, 10, 1, 0, 0, 0, 0, ZoneOffset.UTC))
    batch.get(0, truncMonth(time)) shouldEqual Time(OffsetDateTime.of(2021, 12, 1, 0, 0, 0, 0, ZoneOffset.UTC))
    batch.get(0, truncWeek(time)) shouldEqual Time(OffsetDateTime.of(2021, 12, 20, 0, 0, 0, 0, ZoneOffset.UTC))
    batch.get(0, truncDay(time)) shouldEqual Time(OffsetDateTime.of(2021, 12, 24, 0, 0, 0, 0, ZoneOffset.UTC))
    batch.get(0, truncHour(time)) shouldEqual Time(OffsetDateTime.of(2021, 12, 24, 16, 0, 0, 0, ZoneOffset.UTC))
    batch.get(0, truncMinute(time)) shouldEqual Time(
      OffsetDateTime.of(2021, 12, 24, 16, 52, 0, 0, ZoneOffset.UTC)
    )
    batch.get(0, truncSecond(time)) shouldEqual Time(
      OffsetDateTime.of(2021, 12, 24, 16, 52, 22, 0, ZoneOffset.UTC)
    )

    batch.get(0, extractYear(time)) shouldEqual 2021
    batch.get(0, extractMonth(time)) shouldEqual 12
    batch.get(0, extractDay(time)) shouldEqual 24
    batch.get(0, extractHour(time)) shouldEqual 16
    batch.get(0, extractMinute(time)) shouldEqual 52
    batch.get(0, extractSecond(time)) shouldEqual 22

    batch.get(0, minus(time, const(Time(pointTime.minusHours(1))))) shouldEqual 3600000
    batch.get(0, minus(time, const(PeriodDuration.of(Period.ofWeeks(2))))) shouldEqual Time(
      OffsetDateTime.of(2021, 12, 10, 16, 52, 22, 123, ZoneOffset.UTC)
    )
    batch.get(0, plus(time, const(PeriodDuration.of(Duration.ofHours(12))))) shouldEqual Time(
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

    val qc = new QueryContext(query, None, tokenizer, JIT, NoMetricCollector)
    val calc = qc.calculator

    val batch = BatchDataset(qc)
    batch.set(0, Time(now.minusHours(1)))
    batch.set(0, dimension(TestDims.DIM_A), "a value")
    batch.set(0, metric(TestTableFields.TEST_FIELD), 42d)

    calc.evaluateExpressions(batch, Time(now), IndexedSeq.empty)

    batch.get(0, tuple(dimension(TestDims.DIM_A), minus(metric(TestTableFields.TEST_FIELD)))) shouldEqual (
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

    val qc = new QueryContext(query, None, tokenizer, JIT, NoMetricCollector)
    val calc = qc.calculator

    val batch = BatchDataset(qc)

    val acc = HashTableDataset(qc)

    batch.set(0, Time(now.minusDays(2)))
    batch.set(0, dimension(TestDims.DIM_A), "AAA")

    batch.set(1, Time(now.minusDays(32)))
    batch.set(1, dimension(TestDims.DIM_A), "AAA")

    batch.set(2, Time(now.minusDays(35)))
    batch.set(2, dimension(TestDims.DIM_A), "BBB")

    calc.evaluateExpressions(batch, Time(now), IndexedSeq.empty)
    calc.evaluateFold(acc, batch, Time(now), IndexedSeq.empty)

    val batch2 = acc.iterator.next()
    calc.evaluatePostCombine(batch2)
    calc.evaluatePostAggregateExprs(batch2)
    calc.evaluatePostFilter(batch2, Time(now), IndexedSeq.empty)

    batch2.get(0, dimension(TestDims.DIM_A)) shouldEqual "AAA"
    batch2.get(0, min(time)) shouldEqual Time(now.minusDays(32))

    batch2.get(1, dimension(TestDims.DIM_A)) shouldEqual "BBB"
    batch2.get(1, min(time)) shouldEqual Time(now.minusDays(35))
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

    val qc = new QueryContext(query, None, tokenizer, JIT, NoMetricCollector)
    val calc = qc.calculator

    val batch = BatchDataset(qc)

    batch.set(0, Time(now.minusDays(2)))
    batch.set(0, dimension(TestDims.DIM_A), "aaa")
    batch.set(1, Time(now.minusDays(32)))
    batch.set(1, dimension(TestDims.DIM_A), "ggg")

    calc.evaluatePostAggregateExprs(batch)

    batch.get(0, ce) shouldEqual "X"
    batch.get(1, ce) shouldEqual "Y"
  }

  it should "handle nulls properly" in {
    val now = OffsetDateTime.now()

    val exp = div(plus(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2)), const(2d))
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

    val qc = new QueryContext(query, Some(cond), tokenizer, JIT, NoMetricCollector)
    val calc = qc.calculator

    val batch = BatchDataset(qc)

    batch.set(0, Time(now.minusDays(1)))
    batch.set(0, metric(TestTableFields.TEST_FIELD2), 42d)

    calc.evaluateFilter(batch, Time(now), IndexedSeq.empty)
    batch.isDeleted(0) shouldEqual true
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

    val qc = new QueryContext(query, Some(cond), tokenizer, JIT, NoMetricCollector)
    val calc = qc.calculator

    val batch = BatchDataset(qc)
    batch.set(0, Time(now.minusDays(1)))
    batch.set(0, metric(TestTableFields.TEST_FIELD), 42d)
    batch.set(0, metric(TestTableFields.TEST_LONG_FIELD), 2234512L)

    calc.evaluateFilter(batch, Time(now), IndexedSeq.empty)
    batch.isDeleted(0) shouldBe true
  }

  it should "not evaluate conditional branch if not needed" in {
    val now = OffsetDateTime.now()

    val x = condition(
      neq(metric(TestTableFields.TEST_LONG_FIELD), const(0L)),
      div(dimension(TestDims.DIM_Y), metric(TestTableFields.TEST_LONG_FIELD)),
      const(-1L)
    )

    val query = Query(
      TestSchema.testTable2,
      const(Time(now.minusDays(3))),
      const(Time(now)),
      Seq(x as "x")
    )

    val qc = new QueryContext(query, None, tokenizer, JIT, NoMetricCollector)
    val calc = qc.calculator

    val batch = BatchDataset(qc)

    batch.set(0, Time(now.minusHours(5)))
    batch.set(0, metric(TestTableFields.TEST_LONG_FIELD), 0L)
    batch.set(0, dimension(TestDims.DIM_Y), 3L)
    calc.evaluateExpressions(batch, Time(now), IndexedSeq.empty)

    batch.get(0, x) shouldEqual -1L
  }

  it should "handle conditions on aggregates" in {
    val now = OffsetDateTime.now()

    val x = condition(
      neq(sum(metric(TestTableFields.TEST_LONG_FIELD)), const(0L)),
      div(sum(dimension(TestDims.DIM_Y)), sum(metric(TestTableFields.TEST_LONG_FIELD))),
      const(-1L)
    )

    val query = Query(
      TestSchema.testTable2,
      const(Time(now.minusDays(3))),
      const(Time(now)),
      Seq(x as "x")
    )

    val qc = new QueryContext(query, None, tokenizer, JIT, NoMetricCollector)
    val calc = qc.calculator

    val batch = BatchDataset(qc)

    batch.set(0, Time(now.minusHours(1)))
    batch.set(0, metric(TestTableFields.TEST_LONG_FIELD), 2L)
    batch.set(0, dimension(TestDims.DIM_Y), 4L)

    batch.set(1, Time(now.minusHours(1)))
    batch.set(1, metric(TestTableFields.TEST_LONG_FIELD), -2L)
    batch.set(1, dimension(TestDims.DIM_Y), 2L)

    val acc = HashTableDataset(qc)
    calc.evaluateExpressions(batch, Time(now), IndexedSeq.empty)
    calc.evaluateFold(acc, batch, Time(now), IndexedSeq.empty)
    val batch2 = acc.iterator.next()
    calc.evaluatePostCombine(batch2)

    batch2.get(0, sum(metric(TestTableFields.TEST_LONG_FIELD))) shouldEqual 0L
    batch2.get(0, sum(dimension(TestDims.DIM_Y))) shouldEqual 6L

    calc.evaluatePostAggregateExprs(batch2)
    batch2.get(0, x) shouldEqual -1L
  }

  it should "Support type conversions" in {
    val now = OffsetDateTime.now()

    val x = plus(metric(TestTableFields.TEST_FIELD), byte2Double(metric(TestTableFields.TEST_BYTE_FIELD)))

    val y = plus(metric(TestTableFields.TEST_LONG_FIELD), short2Long(dimension(TestDims.DIM_B)))
    val z =
      div(double2bigDecimal(metric(TestTableFields.TEST_FIELD)), metric(TestTableFields.TEST_BIGDECIMAL_FIELD))

    val query = Query(
      TestSchema.testTable,
      const(Time(now.minusDays(3))),
      const(Time(now)),
      Seq(x as "x", y as "y", z as "z")
    )

    val qc = new QueryContext(query, None, tokenizer, JIT, NoMetricCollector)

    val batch = BatchDataset(qc)

    batch.set(0, Time(now.minusDays(2)))
    batch.set(0, metric(TestTableFields.TEST_FIELD), 10d)
    batch.set(0, metric(TestTableFields.TEST_BYTE_FIELD), 1.toByte)
    batch.set(0, dimension(TestDims.DIM_B), 7.toShort)
    batch.set(0, metric(TestTableFields.TEST_LONG_FIELD), 3L)

    qc.calculator.evaluateExpressions(batch, Time(now), IndexedSeq.empty)

    batch.get(0, x) shouldEqual 11d
    batch.get(0, y) shouldEqual 10L
    batch.isNull(0, z) shouldBe true
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
    val qc = new QueryContext(query, None, tokenizer, JIT, NoMetricCollector)

    val batch = BatchDataset(qc)

    batch.set(0, Time(now.minusDays(4)))
    batch.set(0, metric(TestTableFields.TEST_FIELD), 5d)
    batch.set(0, metric(TestTableFields.TEST_BIGDECIMAL_FIELD), BigDecimal(4))

    batch.set(1, Time(now.minusDays(3)))
    batch.set(1, metric(TestTableFields.TEST_FIELD), 1d)

    qc.calculator.evaluateExpressions(batch, Time(now), IndexedSeq.empty)

    val acc = HashTableDataset(qc)
    qc.calculator.evaluateFold(acc, batch, Time(now), IndexedSeq.empty)

    val batch2 = acc.iterator.next()
    qc.calculator.evaluatePostCombine(batch2)

    batch2.get(0, a) shouldEqual BigDecimal(5)
  }

  it should "handle null literals in case when in aggregation" in {
    val now = OffsetDateTime.now()

    val a = sum(
      condition(
        gt(dimension(TestDims.DIM_B), const(3.toShort)),
        div(short2BigDecimal(dimension(TestDims.DIM_B)), metric(TestTableFields.TEST_BIGDECIMAL_FIELD)),
        NullExpr[BigDecimal](DataType[BigDecimal])
      )
    )

    val query = Query(TestSchema.testTable, const(Time(now.minusDays(5))), const(Time(now.minusDays(2))), Seq(a as "a"))
    val qc = new QueryContext(query, None, tokenizer, JIT, NoMetricCollector)

    val batch = BatchDataset(qc)

    batch.set(0, Time(now.minusDays(4)))
    batch.set(0, dimension(TestDims.DIM_B), 2.toShort)
    batch.set(0, metric(TestTableFields.TEST_BIGDECIMAL_FIELD), BigDecimal(5))

    batch.set(1, Time(now.minusDays(3)))
    batch.set(1, dimension(TestDims.DIM_B), 5.toShort)
    batch.set(1, metric(TestTableFields.TEST_BIGDECIMAL_FIELD), BigDecimal(2))

    qc.calculator.evaluateExpressions(batch, Time(now), IndexedSeq.empty)
    val acc = HashTableDataset(qc)

    qc.calculator.evaluateFold(acc, batch, Time(now), IndexedSeq.empty)
    val batch2 = acc.iterator.next()

    qc.calculator.evaluatePostCombine(batch2)

    batch2.get(0, a) shouldEqual BigDecimal(2.5)
  }
}

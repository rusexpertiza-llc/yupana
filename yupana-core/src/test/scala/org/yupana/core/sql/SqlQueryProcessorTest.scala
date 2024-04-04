package org.yupana.core.sql

import org.scalatest.{ Inside, OptionValues }
import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.api.schema.MetricValue
import org.yupana.core._
import org.yupana.core.sql.parser.SqlParser
import org.yupana.api.utils.ConditionMatchers.{ GeMatcher, LtMatcher }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.threeten.extra.PeriodDuration
import org.yupana.api.types.{ DataType, SimpleStringReaderWriter, StringReaderWriter }

import java.time.{ LocalDateTime, OffsetDateTime, Period, ZoneOffset }

class SqlQueryProcessorTest extends AnyFlatSpec with Matchers with Inside with OptionValues {

  import org.yupana.api.query.syntax.All._

  implicit val srw: StringReaderWriter = SimpleStringReaderWriter
  private val sqlQueryProcessor = new SqlQueryProcessor(TestSchema.schema)

  import TestDims._

  "SqlQueryProcessor" should "create queries" in {
    testQuery("""SELECT MAX(testField) FROM test_table
        |   WHERE time >= TIMESTAMP '2017-06-12' AND time < TIMESTAMP '2017-06-30' and a = 'AbraCadabra'
        |   GROUP BY day(time)""".stripMargin) { x =>
      x.table.value.name shouldEqual "test_table"
      x.filter.value shouldEqual and(
        ge[Time](time, const(Time(OffsetDateTime.of(2017, 6, 12, 0, 0, 0, 0, ZoneOffset.UTC)))),
        lt[Time](time, const(Time(OffsetDateTime.of(2017, 6, 30, 0, 0, 0, 0, ZoneOffset.UTC)))),
        equ(lower(dimension(DIM_A)), const("abracadabra"))
      )
      x.groupBy should contain theSameElementsAs Seq(truncDay(time))
      x.fields should contain theSameElementsInOrderAs List(
        max(metric(TestTableFields.TEST_FIELD)) as "max(testField)"
      )
    }
  }

  it should "have no name collisions when different aggregations are applied to the same field" in {
    testQuery("""
        | SELECT max(testField), min(testField), sum(testField) as sum, b as i, count(B) FROM test_table
        |   WHERE time >= TIMESTAMP '2018-01-01' and time < TIMESTAMP '2018-01-30'
        |   GROUP BY day(time), i
        | """.stripMargin) { q =>
      q.table.value.name shouldEqual "test_table"
      q.filter.value shouldBe and(
        ge(time, const(Time(OffsetDateTime.of(2018, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)))),
        lt(time, const(Time(OffsetDateTime.of(2018, 1, 30, 0, 0, 0, 0, ZoneOffset.UTC))))
      )
      q.groupBy should contain theSameElementsAs List(dimension(DIM_B), truncDay(time))
      q.fields should contain theSameElementsInOrderAs List(
        max(metric(TestTableFields.TEST_FIELD)) as "max(testField)",
        min(metric(TestTableFields.TEST_FIELD)) as "min(testField)",
        sum(metric(TestTableFields.TEST_FIELD)) as "sum",
        dimension(DIM_B) as "i",
        count(dimension(DIM_B)) as "count(B)"
      )
    }
  }

  it should "support <= and > for time" in {
    testQuery(
      """SELECT testField2 FROM test_table
        |  WHERE time > {ts '2017-06-12'} and time <= { ts '2017-06-13' }
      """.stripMargin
    ) { x =>
      x.table.value.name shouldEqual "test_table"
      x.filter.value shouldBe and(
        gt(time, const(Time(OffsetDateTime.of(2017, 6, 12, 0, 0, 0, 0, ZoneOffset.UTC)))),
        le(time, const(Time(OffsetDateTime.of(2017, 6, 13, 0, 0, 0, 0, ZoneOffset.UTC))))
      )
      x.groupBy shouldBe empty
      x.fields should contain theSameElementsInOrderAs List(
        metric(TestTableFields.TEST_FIELD2) as "testField2"
      )
    }
  }

  it should "support case classes in metrics" in {
    testQuery("""SELECT testField FROM test_table
        |  where time >= TIMESTAMP '2017-08-23' and time < TIMESTAMP '2017-08-23 17:41:00.123'""".stripMargin) { x =>
      x.table.value.name shouldEqual "test_table"
      x.filter.value shouldEqual and(
        ge(time, const(Time(OffsetDateTime.of(2017, 8, 23, 0, 0, 0, 0, ZoneOffset.UTC)))),
        lt(time, const(Time(OffsetDateTime.of(2017, 8, 23, 17, 41, 0, 123, ZoneOffset.UTC))))
      )
      x.fields should contain theSameElementsInOrderAs List(
        metric(TestTableFields.TEST_FIELD) as "testField"
      )
    }
  }

  it should "support time aggregations in field list" in {
    testQuery("""SELECT COUNT(testField), day(time) AS d FROM test_table
        |  WHERE time >= TIMESTAMP '2017-8-1' AND TIME < TIMESTAMP '2017-08-08' AND b = 27
        |  GROUP BY day(time)
      """.stripMargin) { x =>
      x.table.value.name shouldEqual "test_table"
      x.filter.value shouldEqual and(
        ge(time, const(Time(OffsetDateTime.of(2017, 8, 1, 0, 0, 0, 0, ZoneOffset.UTC)))),
        lt(time, const(Time(OffsetDateTime.of(2017, 8, 8, 0, 0, 0, 0, ZoneOffset.UTC)))),
        equ(dimension(DIM_B), const(27.toShort))
      )
      x.groupBy should contain theSameElementsAs Seq(truncDay(time))
      x.fields should contain theSameElementsInOrderAs List(
        count(metric(TestTableFields.TEST_FIELD)) as "count(testField)",
        truncDay(time) as "d"
      )
    }
  }

  it should "support aggregations on time" in {
    testQuery(
      """SELECT min(time) as min_time, max(time) as max_time, day(time) as d
        | FROM test_table
        | WHERE time >= TIMESTAMP '2017-10-18' and time <= TIMESTAMP '2017-10-28'
        | GROUP BY d
      """.stripMargin
    ) { q =>
      q.table.value.name shouldEqual "test_table"
      q.filter.value shouldBe and(
        ge(time, const(Time(OffsetDateTime.of(2017, 10, 18, 0, 0, 0, 0, ZoneOffset.UTC)))),
        le(time, const(Time(OffsetDateTime.of(2017, 10, 28, 0, 0, 0, 0, ZoneOffset.UTC))))
      )
      q.groupBy should contain theSameElementsAs Seq(truncDay(time))
      q.fields should contain theSameElementsInOrderAs List(
        min(time) as "min_time",
        max(time) as "max_time",
        truncDay(time) as "d"
      )
    }
  }

  it should "use milliseconds as default time grouping" in {
    testQuery(
      """
        |SELECT "receipt"."time" AS "time", "receipt"."a" as "a"
        | FROM "test_table"
        | WHERE (("receipt"."time" >= {ts '2017-10-30 00:00:00'}) AND ("receipt"."time" <= {ts '2017-11-01 00:00:00'}))
        | GROUP BY "receipt"."time", a
      """.stripMargin
    ) { x =>
      x.table.value.name shouldEqual "test_table"
      x.filter.value shouldBe and(
        ge(time, const(Time(OffsetDateTime.of(2017, 10, 30, 0, 0, 0, 0, ZoneOffset.UTC)))),
        le(time, const(Time(OffsetDateTime.of(2017, 11, 1, 0, 0, 0, 0, ZoneOffset.UTC))))
      )
      x.groupBy should contain theSameElementsAs Seq(time, dimension(DIM_A))
      x.fields should contain theSameElementsAs Seq(
        time as "time",
        dimension(DIM_A) as "a"
      )
    }
  }

  it should "support grouping by tags" in {
    testQuery(
      """SELECT SUM(testField) as sum, day(time) as d FROM test_table
        |  WHERE time >= TIMESTAMP '2017-8-1' AND TIME < TIMESTAMP '2017-08-08 10:30:00'
        |  GROUP BY d, a
      """.stripMargin
    ) { x =>
      x.table.value.name shouldEqual "test_table"
      x.filter.value shouldBe and(
        ge(time, const(Time(OffsetDateTime.of(2017, 8, 1, 0, 0, 0, 0, ZoneOffset.UTC)))),
        lt(time, const(Time(OffsetDateTime.of(2017, 8, 8, 10, 30, 0, 0, ZoneOffset.UTC))))
      )
      x.groupBy should contain theSameElementsAs Seq(dimension(DIM_A), truncDay(time))
      x.fields should contain theSameElementsInOrderAs List(
        sum(metric(TestTableFields.TEST_FIELD)) as "sum",
        truncDay(time) as "d"
      )
    }
  }

  it should "support filter by external link value" in {
    testQuery(
      """SELECT SUM(testField), day(time) AS d, TestLink_testField as word FROM test_table
        |  WHERE time >= TIMESTAMP '2017-8-1' AND TIME < TIMESTAMP '2017-08-08' AND word = 'простокваша' and A = '12345'
        |  GROUP BY day(time), word
      """.stripMargin
    ) { x =>
      x.table.value.name shouldEqual "test_table"
      x.filter.value shouldEqual and(
        ge(time, const(Time(OffsetDateTime.of(2017, 8, 1, 0, 0, 0, 0, ZoneOffset.UTC)))),
        lt(time, const(Time(OffsetDateTime.of(2017, 8, 8, 0, 0, 0, 0, ZoneOffset.UTC)))),
        equ(lower(link(TestLinks.TEST_LINK, "testField")), const("простокваша")),
        equ(lower(dimension(DIM_A)), const("12345"))
      )
      x.groupBy should contain theSameElementsAs Seq(truncDay(time), link(TestLinks.TEST_LINK, "testField"))
      x.fields should contain theSameElementsInOrderAs List(
        sum(metric(TestTableFields.TEST_FIELD)) as "sum(testField)",
        truncDay(time) as "d",
        link(TestLinks.TEST_LINK, "testField") as "word"
      )
    }
  }

  it should "support filter by value expressions" in {
    testQuery("""SELECT SUM(testLongField) as total, day(time) AS d FROM test_table
        | WHERE time >= TIMESTAMP '2017-8-1' AND TIME < TIMESTAMP '2017-08-08' AND testLongField > 1000
        | GROUP BY day(time)
      """.stripMargin) { q =>
      q.table.value.name shouldEqual "test_table"
      q.filter.value shouldBe and(
        ge(time, const(Time(OffsetDateTime.of(2017, 8, 1, 0, 0, 0, 0, ZoneOffset.UTC)))),
        lt(time, const(Time(OffsetDateTime.of(2017, 8, 8, 0, 0, 0, 0, ZoneOffset.UTC)))),
        gt(metric(TestTableFields.TEST_LONG_FIELD), const(1000L))
      )
      q.groupBy should contain theSameElementsAs Seq(truncDay(time))
      q.fields should contain theSameElementsInOrderAs List(
        sum(metric(TestTableFields.TEST_LONG_FIELD)) as "total",
        truncDay(time) as "d"
      )
    }
  }

  it should "support filter by values which requires some type adjustment" in {
    testQuery("""SELECT SUM(testField2) as total, day(time) AS d FROM test_table
        | WHERE time >= TIMESTAMP '2017-8-1' AND TIME < TIMESTAMP '2017-08-08' AND testField > 10
        | GROUP BY day(time)
      """.stripMargin) { q =>
      q.table.value.name shouldEqual "test_table"
      q.filter.value shouldBe and(
        ge(time, const(Time(OffsetDateTime.of(2017, 8, 1, 0, 0, 0, 0, ZoneOffset.UTC)))),
        lt(time, const(Time(OffsetDateTime.of(2017, 8, 8, 0, 0, 0, 0, ZoneOffset.UTC)))),
        gt(metric(TestTableFields.TEST_FIELD), const(10d))
      )
      q.groupBy should contain theSameElementsAs Seq(truncDay(time))
      q.fields should contain theSameElementsInOrderAs List(
        sum(metric(TestTableFields.TEST_FIELD2)) as "total",
        truncDay(time) as "d"
      )
    }
  }

  it should "support IN conditions" in {
    testQuery(
      """
        | SELECT SUM(testField), day(time) as d, b from test_table
        |  WHERE time >= TIMESTAMP '2018-03-26' AND time < TIMESTAMP '2018-03-27' AND A IN ( '123', 'aaa', 'BBB')
        |  GROUP BY d, b
      """.stripMargin
    ) { q =>
      q.table.value.name shouldEqual "test_table"
      q.filter.value shouldBe and(
        ge(time, const(Time(OffsetDateTime.of(2018, 3, 26, 0, 0, 0, 0, ZoneOffset.UTC)))),
        lt(time, const(Time(OffsetDateTime.of(2018, 3, 27, 0, 0, 0, 0, ZoneOffset.UTC)))),
        in(lower(dimension(DIM_A)), Set("123", "aaa", "bbb"))
      )
      q.groupBy should contain theSameElementsAs List(dimension(DIM_B), truncDay(time))
      q.fields should contain theSameElementsInOrderAs List(
        sum(metric(TestTableFields.TEST_FIELD)) as "sum(testField)",
        truncDay(time) as "d",
        dimension(DIM_B) as "b"
      )
    }
  }

  it should "support IN conditions for values" in {
    testQuery(
      """
        | SELECT SUM(testField), day(time) as d, b from test_table
        |  WHERE time >= TIMESTAMP '2018-03-26' AND time < TIMESTAMP '2018-03-27' AND testField2 IN (123, 456, 789)
        |  GROUP BY d, B
      """.stripMargin
    ) { q =>
      q.table.value.name shouldEqual "test_table"
      q.filter.value shouldBe and(
        ge(time, const(Time(OffsetDateTime.of(2018, 3, 26, 0, 0, 0, 0, ZoneOffset.UTC)))),
        lt(time, const(Time(OffsetDateTime.of(2018, 3, 27, 0, 0, 0, 0, ZoneOffset.UTC)))),
        in(metric(TestTableFields.TEST_FIELD2), Set(123d, 456d, 789d))
      )
      q.groupBy should contain theSameElementsAs List(dimension(DIM_B), truncDay(time))
      q.fields should contain theSameElementsInOrderAs List(
        sum(metric(TestTableFields.TEST_FIELD)) as "sum(testField)",
        truncDay(time) as "d",
        dimension(DIM_B) as "b"
      )
    }
  }

  it should "support NOT IN conditions" in {
    testQuery("""
        | SELECT MIN(testField) tf, trunc_month(time) m FROM test_table
        |   WHERE time >= TIMESTAMP '2019-03-30' and time < TIMESTAMP '2019-03-31' AND testField2 NOT IN (5, 6, 7)
        |   GROUP BY m
      """.stripMargin) { q =>
      q.table.value.name shouldEqual "test_table"
      q.filter.value shouldEqual and(
        ge(time, const(Time(OffsetDateTime.of(2019, 3, 30, 0, 0, 0, 0, ZoneOffset.UTC)))),
        lt(time, const(Time(OffsetDateTime.of(2019, 3, 31, 0, 0, 0, 0, ZoneOffset.UTC)))),
        notIn(metric(TestTableFields.TEST_FIELD2), Set(5d, 6d, 7d))
      )
      q.fields should contain theSameElementsInOrderAs List(
        min(metric(TestTableFields.TEST_FIELD)) as "tf",
        truncMonth(time) as "m"
      )
      q.groupBy should contain theSameElementsInOrderAs List(truncMonth(time))
    }
  }

  it should "support between conditions" in {
    testQuery(
      """
        | SELECT testField tf, trunc_month(time) m FROM test_table
        |   WHERE time BETWEEN TIMESTAMP '2019-03-30' and TIMESTAMP '2019-03-31' AND testField2 BETWEEN 5 AND 7
        |""".stripMargin
    ) { q =>
      q.table.value.name shouldEqual "test_table"
      q.filter.value shouldEqual and(
        and(
          ge(time, const(Time(OffsetDateTime.of(2019, 3, 30, 0, 0, 0, 0, ZoneOffset.UTC)))),
          le(time, const(Time(OffsetDateTime.of(2019, 3, 31, 0, 0, 0, 0, ZoneOffset.UTC))))
        ),
        and(
          ge(metric(TestTableFields.TEST_FIELD2), const(5d)),
          le(metric(TestTableFields.TEST_FIELD2), const(7d))
        )
      )
    }
  }

  it should "support functions as conditions" in {
    testQuery(
      """
        |SELECT a, array_to_string(tokens(a))
        |  FROM test_table
        |  WHERE time >= timestamp '2019-03-14' and time < TIMESTAMP '2019-03-15' and contains_any(tokens(a), tokens('ВОДА'))
      """.stripMargin
    ) { q =>
      q.table.value.name shouldEqual "test_table"
      q.fields should contain theSameElementsInOrderAs List(
        dimension(DIM_A) as "a",
        arrayToString(tokens(dimension(DIM_A))) as "array_to_string(tokens(a))"
      )
      q.filter.value shouldBe and(
        ge(time, const(Time(OffsetDateTime.of(2019, 3, 14, 0, 0, 0, 0, ZoneOffset.UTC)))),
        lt(time, const(Time(OffsetDateTime.of(2019, 3, 15, 0, 0, 0, 0, ZoneOffset.UTC)))),
        containsAny(
          tokens(lower(dimension(DIM_A))),
          tokens(const("вода"))
        )
      )
    }
  }

  it should "support functions of arrays" in {
    testQuery("""
        |SELECT
        |  b,
        |  case
        |    when contains_any(tokens(a), tokens('крЫжовник')) then 'зеленые'
        |    when contains_any(tokens(a), tokens({'клубника', 'малина'})) then 'КРАСНЫЕ'
        |    when contains_any(tokens(a), tokens({'черника', 'ежевика', 'ИРГА'})) then 'черные'
        |    else 'прочие' as color,
        |  sum(testField)
        |FROM test_table
        |WHERE time >= timestamp '2019-03-14' AND time < timestamp '2019-03-26' AND TestLink_testField = 'ягода'
        |GROUP BY b, color
      """.stripMargin) { q =>
      q.table.value.name shouldEqual "test_table"

      val colorExpr = condition(
        containsAny(
          tokens(lower(dimension(DIM_A))),
          tokens(const("крыжовник"))
        ),
        const("зеленые"),
        condition(
          containsAny(
            tokens(lower(dimension(DIM_A))),
            tokenizeArray(array(const("клубника"), const("малина")))
          ),
          const("КРАСНЫЕ"),
          condition(
            containsAny(
              tokens(lower(dimension(DIM_A))),
              tokenizeArray(
                array(const("черника"), const("ежевика"), const("ирга"))
              )
            ),
            const("черные"),
            const("прочие")
          )
        )
      )

      q.fields should contain theSameElementsInOrderAs Seq(
        dimension(DIM_B) as "b",
        colorExpr as "color",
        sum(metric(TestTableFields.TEST_FIELD)) as "sum(testField)"
      )

      q.filter.value shouldEqual and(
        ge(time, const(Time(OffsetDateTime.of(2019, 3, 14, 0, 0, 0, 0, ZoneOffset.UTC)))),
        lt(time, const(Time(OffsetDateTime.of(2019, 3, 26, 0, 0, 0, 0, ZoneOffset.UTC)))),
        equ(lower(link(TestLinks.TEST_LINK, "testField")), const("ягода"))
      )

      q.groupBy should contain theSameElementsAs List(dimension(DIM_B), colorExpr)
    }
  }

  it should "handle tuples" in {
    testQuery("""
        |SELECT min(testField) mtf, hour(time) h
        |  FROM test_table
        |  WHERE time >= TIMESTAMP '2022-05-01' and time < TIMESTAMP '2022-05-05'
        |    AND (testStringField, testLongField) IN (('foo', 1), ('bar', 21))
        |  GROUP BY h""".stripMargin) { q =>

      q.table.value.name shouldEqual "test_table"
      q.fields should contain theSameElementsInOrderAs Seq(
        min(metric(TestTableFields.TEST_FIELD)) as "mtf",
        truncHour(time) as "h"
      )
      q.filter.value shouldEqual and(
        ge(time, const(Time(LocalDateTime.of(2022, 5, 1, 0, 0)))),
        lt(time, const(Time(LocalDateTime.of(2022, 5, 5, 0, 0)))),
        in(
          tuple(lower(metric(TestTableFields.TEST_STRING_FIELD)), metric(TestTableFields.TEST_LONG_FIELD)),
          Set(("foo", 1L), ("bar", 21L))
        )
      )
      q.groupBy should contain theSameElementsAs Seq(truncHour(time))
    }
  }

  it should "substitute passed placeholders values" in {
    val statement =
      """SELECT SUM(TestField), month(time) as m, b FROM test_table
        | WHERE time >= ? and time < ? AND a = ?
        | GROUP BY m, b
      """.stripMargin

    val from = OffsetDateTime.of(2017, 9, 1, 0, 0, 0, 0, ZoneOffset.UTC)
    val to = OffsetDateTime.of(2017, 9, 15, 0, 0, 0, 0, ZoneOffset.UTC)

    inside(
      createQuery(
        statement,
        Map(
          1 -> parser.TypedValue(Time(from)),
          2 -> parser.TypedValue(Time(to)),
          3 -> parser.TypedValue("123456789")
        )
      )
    ) {
      case Right(q) =>
        q.table.value.name shouldEqual "test_table"
        q.filter.value shouldBe and(
          ge(time, ConstantExpr(Time(from), prepared = true)),
          lt(time, ConstantExpr(Time(to), prepared = true)),
          equ(lower(dimension(DIM_A)), ConstantExpr("123456789", prepared = true))
        )
        q.groupBy should contain theSameElementsAs List(dimension(DIM_B), truncMonth(time))
        q.fields should contain theSameElementsInOrderAs List(
          sum(metric(TestTableFields.TEST_FIELD)) as "sum(TestField)",
          truncMonth(time) as "m",
          dimension(DIM_B) as "b"
        )
      case Left(msg) => fail(msg)
    }
  }

  it should "handle untyped placeholder values" in {
    val statement =
      """SELECT SUM(TestField), month(time) as m, b FROM test_table
        | WHERE time >= ? and time < ? AND a = ?
        | GROUP BY m, b
      """.stripMargin

    val from = "2024-03-27T15:49:44"
    val to = "2024-03-27T23:57:32"

    inside(
      createQuery(
        statement,
        Map(
          1 -> parser.UntypedValue(from),
          2 -> parser.UntypedValue(to),
          3 -> parser.TypedValue("123456789")
        )
      )
    ) {
      case Right(q) =>
        q.table.value.name shouldEqual "test_table"
        q.filter.value shouldBe and(
          ge(time, ConstantExpr(Time(LocalDateTime.of(2024, 3, 27, 15, 49, 44)), prepared = true)),
          lt(time, ConstantExpr(Time(LocalDateTime.of(2024, 3, 27, 23, 57, 32)), prepared = true)),
          equ(lower(dimension(DIM_A)), ConstantExpr("123456789", prepared = true))
        )
    }
  }

  it should "handle nested queries with const fields" in {
    testQuery("""
        | SELECT 1 as "Number_of_Records",
        |   "Query"."sum" as "sum",
        |   "Query"."d" as "d"
        | FROM (
        |   SELECT day(time) as d,
        |     sum(testLongField) sum,
        |     A
        |   FROM test_table
        |   WHERE time >= TIMESTAMP '2017-01-01' AND time < TIMESTAMP '2017-02-01'
        |   GROUP BY d, A
        | ) "Query"
      """.stripMargin) { q =>
      q.table.value.name shouldEqual "test_table"
      q.filter.value shouldBe and(
        ge(time, const(Time(OffsetDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)))),
        lt(time, const(Time(OffsetDateTime.of(2017, 2, 1, 0, 0, 0, 0, ZoneOffset.UTC))))
      )
      q.groupBy should contain theSameElementsAs List(dimension(DIM_A), truncDay(time))
      q.fields should contain theSameElementsInOrderAs List(
        const(BigDecimal(1)) as "Number_of_Records",
        sum(metric(TestTableFields.TEST_LONG_FIELD)) as "sum",
        truncDay(time) as "d"
      )
    }
  }

  it should "support placeholders in const fields" in {
    val statement =
      """
        | SELECT ? as my_string, day(time) as d
        |   FROM test_table
        |   WHERE time >= TIMESTAMP '2018-1-1' AND time < ?
      """.stripMargin

    inside(
      createQuery(
        statement,
        Map(
          1 -> parser.TypedValue("Test me"),
          2 -> parser.TypedValue(Time(OffsetDateTime.of(2018, 1, 23, 16, 44, 20, 0, ZoneOffset.UTC)))
        )
      )
    ) {
      case Right(q) =>
        q.table.value.name shouldEqual "test_table"
        q.filter.value shouldBe and(
          ge(time, ConstantExpr(Time(OffsetDateTime.of(2018, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)), prepared = false)),
          lt(time, ConstantExpr(Time(OffsetDateTime.of(2018, 1, 23, 16, 44, 20, 0, ZoneOffset.UTC)), prepared = true))
        )
        q.groupBy shouldBe empty
        q.fields should contain theSameElementsInOrderAs List(
          const("Test me") as "my_string",
          truncDay(time) as "d"
        )

      case Left(msg) =>
        fail(msg)
    }
  }

  it should "be possible to call functions on tags and catalogs" in {
    testQuery("""
        | SELECT count(A) as count_a, max(testLink3_testField3_2), day(time)
        |   FROM test_table
        |   WHERE time > TIMESTAMP '2017-11-01' AND time < TIMESTAMP '2017-12-01'
        |   GROUP BY day(time)
      """.stripMargin) { q =>
      q.table.value.name shouldEqual "test_table"
      q.filter.value shouldBe and(
        gt(time, const(Time(OffsetDateTime.of(2017, 11, 1, 0, 0, 0, 0, ZoneOffset.UTC)))),
        lt(time, const(Time(OffsetDateTime.of(2017, 12, 1, 0, 0, 0, 0, ZoneOffset.UTC))))
      )
      q.groupBy should contain theSameElementsAs Seq(truncDay(time))
      q.fields should contain theSameElementsInOrderAs List(
        count(dimension(DIM_A)) as "count_a",
        max(link(TestLinks.TEST_LINK3, "testField3_2")) as "max(testLink3_testField3_2)",
        truncDay(time) as "day(time)"
      )
    }
  }

  it should "support named binary functions" in {
    testQuery("""
        |SELECT a, array_to_string(tokens(a)), contains_any(tokens(a), tokens('вода')) as is_water
        |  FROM test_table
        |  WHERE time >= timestamp '2019-03-14' and time < TIMESTAMP '2019-03-15'
      """.stripMargin) { q =>
      q.table.value.name shouldEqual "test_table"
      q.fields should contain theSameElementsInOrderAs List(
        dimension(DIM_A) as "a",
        arrayToString(tokens(dimension(DIM_A))) as "array_to_string(tokens(a))",
        containsAny(
          tokens(dimension(DIM_A)),
          tokens(const("вода"))
        ) as "is_water"
      )
      q.filter.value shouldBe and(
        ge(time, const(Time(OffsetDateTime.of(2019, 3, 14, 0, 0, 0, 0, ZoneOffset.UTC)))),
        lt(time, const(Time(OffsetDateTime.of(2019, 3, 15, 0, 0, 0, 0, ZoneOffset.UTC))))
      )
    }
  }

  it should "support window functions" in {
    testQuery("""
        | SELECT a, lag(time), lag(testField) as lag_totalSum
        |   FROM test_table
        |   WHERE time > TIMESTAMP '2017-11-01' AND time < TIMESTAMP '2017-12-01'
        |   GROUP BY a
      """.stripMargin) { q =>
      q.table.value.name shouldEqual "test_table"
      q.filter.value shouldBe and(
        gt(time, const(Time(OffsetDateTime.of(2017, 11, 1, 0, 0, 0, 0, ZoneOffset.UTC)))),
        lt(time, const(Time(OffsetDateTime.of(2017, 12, 1, 0, 0, 0, 0, ZoneOffset.UTC))))
      )
      q.groupBy should contain theSameElementsAs Set(dimension(DIM_A))
      q.fields should contain theSameElementsInOrderAs List(
        dimension(DIM_A) as "a",
        lag(time) as "lag(time)",
        lag(metric(TestTableFields.TEST_FIELD)) as "lag_totalSum"
      )
    }
  }

  it should "support case when expressions" in {
    testQuery("""
        | SELECT
        |   time,
        |   CASE
        |     WHEN testField > 1000 THEN 2
        |     WHEN testField > 100 THEN 1
        |     ELSE 0
        |   AS log_sum
        | FROM test_table_2
        |   WHERE time >= TIMESTAMP '2018-1-1' AND time < TIMESTAMP '2018-2-1' AND X = '1234567890'
      """.stripMargin) { q =>
      q.table.value.name shouldEqual "test_table_2"
      q.filter.value shouldBe and(
        ge(time, const(Time(OffsetDateTime.of(2018, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)))),
        lt(time, const(Time(OffsetDateTime.of(2018, 2, 1, 0, 0, 0, 0, ZoneOffset.UTC)))),
        equ(lower(dimension(DIM_X)), const("1234567890"))
      )
      q.groupBy shouldBe empty
      q.fields should contain theSameElementsInOrderAs List(
        time as "time",
        condition(
          gt(metric(TestTable2Fields.TEST_FIELD), const(BigDecimal(1000))),
          const(BigDecimal(2)),
          condition(
            gt(metric(TestTable2Fields.TEST_FIELD), const(BigDecimal(100))),
            const(BigDecimal(1)),
            const(BigDecimal(0))
          )
        ) as "log_sum"
      )
    }
  }

  it should "support case when expressions wrapped in functions" in {
    testQuery("""
        | SELECT
        |   time,
        |   sum(case
        |     WHEN testField > 10 THEN 1
        |     ELSE 0
        |  ) AS q
        |  FROM test_table
        |  WHERE time >= TIMESTAMP '2018-1-1' and time < TIMESTAMP '2018-2-1'
        |  GROUP BY time
      """.stripMargin) { q =>
      q.table.value.name shouldEqual "test_table"
      q.filter.value shouldBe and(
        ge(time, const(Time(OffsetDateTime.of(2018, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)))),
        lt(time, const(Time(OffsetDateTime.of(2018, 2, 1, 0, 0, 0, 0, ZoneOffset.UTC))))
      )
      q.groupBy shouldBe Seq(time)
      q.fields should contain theSameElementsInOrderAs List(
        time as "time",
        sum(
          condition(
            gt(metric(TestTableFields.TEST_FIELD), const(10d)),
            const(BigDecimal(1)),
            const(BigDecimal(0))
          )
        ) as "q"
      )
    }
  }

  it should "support case when expressions with fields" in {
    testQuery("""
        | SELECT
        |   time,
        |   sum(case
        |     WHEN y = 1 THEN 1
        |     ELSE 0
        |  ) AS count,
        |   sum(case
        |     WHEN y = 1 THEN testField
        |     ELSE 0
        |  ) AS sum
        |  FROM test_table_2
        |  WHERE time >= TIMESTAMP '2018-1-1' and time < TIMESTAMP '2018-2-1'
        |  GROUP BY time
      """.stripMargin) { q =>
      q.table.value.name shouldEqual "test_table_2"
      q.filter.value shouldBe and(
        ge(time, const(Time(OffsetDateTime.of(2018, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)))),
        lt(time, const(Time(OffsetDateTime.of(2018, 2, 1, 0, 0, 0, 0, ZoneOffset.UTC))))
      )
      q.groupBy shouldBe Seq(time)
      q.fields should contain theSameElementsInOrderAs List(
        time as "time",
        sum(
          condition(
            equ(dimension(DIM_Y), const(1L)),
            const(BigDecimal(1)),
            const(BigDecimal(0))
          )
        ) as "count",
        sum(
          condition(
            equ(dimension(DIM_Y), const(1L)),
            metric(TestTable2Fields.TEST_FIELD),
            const(BigDecimal(0))
          )
        ) as "sum"
      )
    }
  }

  it should "support case when with boolean literals" in {
    testQuery("""
        |SELECT
        |    time,
        |    testField
        |FROM test_table
        |WHERE
        |  time >= timestamp '2020-02-16' and time <= timestamp '2020-02-17' and
        |  (case when (testField is null)
        |    then true
        |    else testField > 100
        |   )
        |limit 10
        |""".stripMargin) { q =>
      q.table.value.name shouldEqual "test_table"
      q.fields should contain theSameElementsInOrderAs List(time.toField, metric(TestTableFields.TEST_FIELD).toField)
      q.filter.value shouldEqual and(
        ge(time, const(Time(LocalDateTime.of(2020, 2, 16, 0, 0)))),
        le(time, const(Time(LocalDateTime.of(2020, 2, 17, 0, 0)))),
        condition(
          isNull(metric(TestTableFields.TEST_FIELD)),
          const(true),
          gt(metric(TestTableFields.TEST_FIELD), const(100d))
        )
      )
    }
  }

  it should "support null in case when expressions" in {
    testQuery("""
        | SELECT
        |   sum(case
        |     WHEN testField > 0 THEN testLongField / testField
        |     ELSE null
        |  ) AS d
        |  FROM test_table
        |  WHERE time >= TIMESTAMP '2018-1-1' and time < TIMESTAMP '2018-2-1'
        |  GROUP BY d
      """.stripMargin) { q =>
      q.table.value.name shouldEqual "test_table"
      q.fields should contain theSameElementsInOrderAs List(
        sum(
          condition(
            gt(metric(TestTableFields.TEST_FIELD), const(0d)),
            divFrac(long2Double(metric(TestTableFields.TEST_LONG_FIELD)), metric(TestTableFields.TEST_FIELD)),
            NullExpr(DataType[Double])
          )
        ) as "d"
      )
    }
  }

  it should "support having expressions" in {
    testQuery("""
        | SELECT
        |  a,
        |  time AS t,
        |  lag(time) AS lagTime
        | FROM test_table
        | WHERE time < TIMESTAMP '2018-02-01' AND time > TIMESTAMP '2018-01-01'
        | GROUP BY a
        | HAVING
        |  ((lagTime - t) > INTERVAL '2:00:00' AND extract_hour(t) >= 8 AND extract_hour(t) <= 18) OR
        |  ((lagTime - t) > INTERVAL '4:00:00' AND (extract_hour(t) > 18 OR extract_hour(t) < 8))
      """.stripMargin) { q =>
      q.table.value.name shouldEqual "test_table"
      q.filter.value shouldBe and(
        lt(time, const(Time(OffsetDateTime.of(2018, 2, 1, 0, 0, 0, 0, ZoneOffset.UTC)))),
        gt(time, const(Time(OffsetDateTime.of(2018, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC))))
      )
      q.groupBy should contain theSameElementsAs List(dimension(DIM_A))

      val lagTime = lag(time)

      q.fields should contain theSameElementsInOrderAs List(
        dimension(DIM_A) as "a",
        time as "t",
        lagTime as "lagTime"
      )

      val expectedPostFilter = or(
        and(
          gt(
            minus(
              lagTime,
              time
            ),
            const(2 * 3600 * 1000L)
          ),
          ge(extractHour(time), const(8)),
          le(extractHour(time), const(18))
        ),
        and(
          gt(
            minus(
              lagTime,
              time
            ),
            const(4 * 3600 * 1000L)
          ),
          or(
            gt(extractHour(time), const(18)),
            lt(extractHour(time), const(8))
          )
        )
      )

      q.postFilter.value shouldEqual expectedPostFilter
    }
  }

  it should "handle big intervals" in {
    testQuery("""
        | SELECT
        |  a,
        |  time AS t,
        |  lag(time) AS lagTime
        | FROM test_table
        | WHERE time < TIMESTAMP '2018-08-01' AND time >= TIMESTAMP '2018-07-01'
        | GROUP BY a
        | HAVING (lagTime - t) >= INTERVAL '5' DAY
      """.stripMargin) { q =>
      q.table.value.name shouldEqual "test_table"
      q.filter.value shouldBe and(
        lt(time, const(Time(OffsetDateTime.of(2018, 8, 1, 0, 0, 0, 0, ZoneOffset.UTC)))),
        ge(time, const(Time(OffsetDateTime.of(2018, 7, 1, 0, 0, 0, 0, ZoneOffset.UTC))))
      )
      q.groupBy should contain theSameElementsAs List(dimension(DIM_A))

      val t = time as "t"
      val lagTime = lag(time) as "lagTime"

      q.fields should contain theSameElementsInOrderAs List(
        dimension(DIM_A) as "a",
        t,
        lagTime
      )
      q.postFilter.value shouldEqual ge(
        minus(
          lag(time),
          time
        ),
        const(5 * 24 * 3600 * 1000L)
      )
    }
  }

  object GeTime extends GeMatcher[Time]
  object LtTime extends LtMatcher[Time]

  it should "handle period arithmetic" in {
    val now = OffsetDateTime.now(ZoneOffset.UTC).toInstant.toEpochMilli
    testQuery("""
        |SELECT SUM(testField) as sum, day(time) as d FROM test_table
        |  WHERE time >= trunc_day(now() - INTERVAL '3' MONTH) AND TIME < trunc_day(now())
        |  GROUP BY d, a
      """.stripMargin) { q =>
      q.table.value.name shouldEqual "test_table"
      inside(q.filter.value) {
        case AndExpr(Seq(from, to)) =>
          inside(from) {
            case GeTime(
                  te,
                  TruncDayExpr(TimeMinusPeriodExpr(ConstantExpr(t, _), ConstantExpr(p, _)))
                ) =>
              te shouldEqual TimeExpr
              t.asInstanceOf[Time].millis shouldEqual (now +- 1000L)
              p shouldEqual PeriodDuration.of(Period.ofMonths(3))
          }
          inside(to) {
            case LtTime(te, TruncDayExpr(ConstantExpr(t, _))) =>
              te shouldEqual TimeExpr
              t.asInstanceOf[Time].millis shouldEqual (now +- 1000L)
          }
      }

      q.fields should contain theSameElementsAs Seq(
        sum(metric(TestTableFields.TEST_FIELD)) as "sum",
        truncDay(time) as "d"
      )
      q.groupBy should contain theSameElementsAs Seq(
        truncDay(time),
        dimension(DIM_A)
      )
    }
  }

  it should "handle queries like this" in {
    testQuery("""SELECT
        |sum(CASE WHEN b = 2 THEN 1 ELSE 0) AS salesTicketsCount, day(time) AS d
        |FROM test_table
        |WHERE time >= TIMESTAMP '2018-09-03 14:08:05' AND time < TIMESTAMP '2018-09-03 14:08:17'
        |GROUP BY d;
      """.stripMargin) { q =>
      q.table.value.name shouldEqual "test_table"
      q.fields should contain theSameElementsAs Seq(
        sum(
          condition(
            equ(dimension(TestDims.DIM_B), const(2.toShort)),
            const(BigDecimal(1)),
            const(BigDecimal(0))
          )
        ) as "salesTicketsCount",
        truncDay(time) as "d"
      )
    }
  }

  it should "handle IS NULL and IS NOT NULL conditions" in {
    testQuery("""
        |SELECT sum(testField), day(time) as d FROM test_table
        |  WHERE TestLink_testField IS NULL AND testField2 IS NOT NULL
        |  AND time < TIMESTAMP '2018-08-01' AND time >= TIMESTAMP '2018-07-01'
        |  GROUP BY d, A
      """.stripMargin) { q =>
      q.table.value.name shouldEqual "test_table"
      q.fields should contain theSameElementsAs Seq(
        sum(metric(TestTableFields.TEST_FIELD)) as "sum(testField)",
        truncDay(time) as "d"
      )
      q.filter.value shouldEqual and(
        isNull(link(TestLinks.TEST_LINK, "testField")),
        isNotNull(metric(TestTableFields.TEST_FIELD2)),
        lt(time, const(Time(OffsetDateTime.of(2018, 8, 1, 0, 0, 0, 0, ZoneOffset.UTC)))),
        ge(time, const(Time(OffsetDateTime.of(2018, 7, 1, 0, 0, 0, 0, ZoneOffset.UTC))))
      )
      q.groupBy should contain theSameElementsAs Seq(
        truncDay(time),
        dimension(DIM_A)
      )
    }
  }

  it should "handle IS NULL and IS NOT NULL conditions within CASE" in {
    testQuery(
      """
        |SELECT sum(CASE WHEN testlink_testfield IS NOT NULL THEN testField ELSE 0) as quantity, day(time) as d FROM test_table
        |  WHERE testLink2_testField2 = '464'
        |  AND time < TIMESTAMP '2018-08-01' AND time >= TIMESTAMP '2018-07-01'
        |  GROUP BY d
      """.stripMargin
    ) { q =>
      q.table.value.name shouldEqual "test_table"
      val condExpr = condition(
        isNotNull(link(TestLinks.TEST_LINK, "testField")),
        metric(TestTableFields.TEST_FIELD),
        const(0d)
      )
      q.fields should contain theSameElementsAs Seq(
        sum(condExpr) as "quantity",
        truncDay(time) as "d"
      )
      q.filter.value shouldBe and(
        equ(lower(link(TestLinks.TEST_LINK2, "testField2")), const("464")),
        lt(time, const(Time(OffsetDateTime.of(2018, 8, 1, 0, 0, 0, 0, ZoneOffset.UTC)))),
        ge(time, const(Time(OffsetDateTime.of(2018, 7, 1, 0, 0, 0, 0, ZoneOffset.UTC))))
      )
      q.groupBy should contain theSameElementsAs Seq(
        truncDay(time)
      )
    }
  }

  it should "use different names for a field and functions/aggregations on this field in the same query" in {
    testQuery("""
        |SELECT
        |    sum(testField),
        |    max(testField),
        |    month(time) as d,
        |    a,
        |    b
        |FROM
        |    test_table
        |WHERE
        |    time >= TIMESTAMP '2018-08-01' AND
        |    time < TIMESTAMP '2018-09-01'  AND testField < 50000 AND a = '0000348521023155'
        |group by
        |    d,
        |    B, A
      """.stripMargin) { q =>
      q.fields should contain theSameElementsAs Seq(
        sum(metric(TestTableFields.TEST_FIELD)) as "sum(testField)",
        max(metric(TestTableFields.TEST_FIELD)) as "max(testField)",
        dimension(DIM_A) as "a",
        dimension(DIM_B) as "b",
        truncMonth(time) as "d"
      )

      q.filter.value shouldBe and(
        ge(time, const(Time(OffsetDateTime.of(2018, 8, 1, 0, 0, 0, 0, ZoneOffset.UTC)))),
        lt(time, const(Time(OffsetDateTime.of(2018, 9, 1, 0, 0, 0, 0, ZoneOffset.UTC)))),
        lt(metric(TestTableFields.TEST_FIELD), const(50000d)),
        equ(lower(dimension(DIM_A)), const("0000348521023155"))
      )

      q.groupBy should contain theSameElementsAs Seq(
        truncMonth(time),
        dimension(DIM_B),
        dimension(DIM_A)
      )
    }
  }

  it should "process query with field arithmetics" in {
    testQuery("""
        |SELECT
        |    max(testField - 2 * testField2) as strange_result,
        |    month(time) as d
        |FROM
        |    test_table_2
        |WHERE
        |    time >= TIMESTAMP '2018-08-01' AND
        |    time < TIMESTAMP '2018-09-01'
        |GROUP BY
        |    d
      """.stripMargin) { q =>
      val mult = times(
        const(2d),
        metric(TestTable2Fields.TEST_FIELD2)
      )
      val sub = minus(
        metric(TestTable2Fields.TEST_FIELD),
        double2bigDecimal(mult)
      )
      q.fields should contain theSameElementsAs Seq(
        max(sub) as "strange_result",
        truncMonth(time) as "d"
      )

      q.groupBy should contain theSameElementsAs Seq(
        truncMonth(time)
      )
    }
  }

  it should "process query with field arithmetics on aggregated values" in {
    testQuery("""
        |SELECT
        |    sum(testField3) - 2 * sum(testField) as strange_result,
        |    month(time) as d
        |FROM
        |    test_table_2
        |WHERE
        |    time >= TIMESTAMP '2018-08-01' AND
        |    time < TIMESTAMP '2018-09-01'
        |GROUP BY
        |    d
      """.stripMargin) { q =>
      val l = sum(metric(TestTable2Fields.TEST_FIELD3))
      val r = sum(metric(TestTable2Fields.TEST_FIELD))
      val rr = times(const(BigDecimal(2)), r)
      q.fields should contain theSameElementsAs Seq(
        minus(l, rr) as "strange_result",
        truncMonth(time) as "d"
      )

      q.groupBy should contain theSameElementsAs Seq(
        truncMonth(time)
      )
    }
  }

  it should "not process query with bad field arithmetics" in {
    val q = """
              |SELECT
              |    max(testField) - 2 * testField3 as strange_result,
              |    month(time) as d
              |FROM
              |    test_table_2
              |WHERE
              |    time >= TIMESTAMP '2018-08-01' AND
              |    time < TIMESTAMP '2018-09-01'
              |GROUP BY
              |    d
            """.stripMargin

    testError(q) {
      _ shouldBe "Invalid expression 'max(metric(testField)) - const(2:BigDecimal) * metric(testField3)' for field strange_result"
    }
  }

  it should "cast long to double" in {
    testQuery(
      "SELECT testField + testLongField as plus2 FROM test_table WHERE time >= TIMESTAMP '2018-10-16 17:44:47' " +
        "AND time <= TIMESTAMP '2018-10-16 17:44:51' AND b = 22322"
    ) { q =>
      q.fields should contain theSameElementsAs Seq(
        plus(metric(TestTableFields.TEST_FIELD), long2Double(metric(TestTableFields.TEST_LONG_FIELD))) as "plus2"
      )
    }
  }

  it should "handle unary minus" in {
    testQuery("""SELECT sum(abs(-testLongField)) as abs1,
                |abs(sum(CASE WHEN testField < 40000 THEN -10 + 5 ELSE -testField)) as abs2,
                |testStringField
                |FROM test_table
                |WHERE time >= TIMESTAMP '2019-04-10' AND time <= TIMESTAMP '2019-04-11'
                |AND -testLongField < -100
                |GROUP BY testStringField
                |""".stripMargin) { q =>
      q.table.value shouldEqual TestSchema.testTable
      q.fields should contain theSameElementsInOrderAs Seq(
        sum(abs(minus(metric(TestTableFields.TEST_LONG_FIELD)))) as "abs1",
        abs(
          sum(
            condition(
              lt(metric(TestTableFields.TEST_FIELD), const(40000d)),
              plus(const(BigDecimal(-10)), const(BigDecimal(5))),
              double2bigDecimal(minus(metric(TestTableFields.TEST_FIELD)))
            )
          )
        ) as "abs2",
        metric(TestTableFields.TEST_STRING_FIELD).toField
      )

      q.filter.value shouldEqual and(
        ge(time, const(Time(OffsetDateTime.of(2019, 4, 10, 0, 0, 0, 0, ZoneOffset.UTC)))),
        le(time, const(Time(OffsetDateTime.of(2019, 4, 11, 0, 0, 0, 0, ZoneOffset.UTC)))),
        lt(minus(metric(TestTableFields.TEST_LONG_FIELD)), const(-100L))
      )
    }
  }

  it should "align numeric types" in {
    testQuery("""SELECT testField4 / testField2 as div
        |  FROM test_table_2
        |  WHERE time >= timestamp '2023-01-01' and time < timestamp '2023-02-01'
        |""".stripMargin) { q =>
      q.table.value shouldEqual TestSchema.testTable2
      q.fields should contain theSameElementsInOrderAs Seq(
        divFrac(int2Double(metric(TestTable2Fields.TEST_FIELD4)), metric(TestTable2Fields.TEST_FIELD2)) as "div"
      )
    }
  }

  it should "allow manual casts" in {
    testQuery("""SELECT
        |    sum(cast(testLongField as DOUBLE)) as sum,
        |    cast(day(time) as varchar) as sTime
        |  FROM test_table
        |  WHERE time >= timestamp '2023-02-06' and time < timestamp '2023-02-15'
        |  GROUP BY sTime
        |""".stripMargin) { q =>
      q.table.value shouldEqual TestSchema.testTable
      q.fields should contain theSameElementsInOrderAs Seq(
        sum(long2Double(metric(TestTableFields.TEST_LONG_FIELD))) as "sum",
        x2String(truncDay(time)) as "sTime"
      )
    }
  }

  it should "be able to manual cast constants" in {
    testQuery("""SELECT cast(1 as double) 1d, cast (1 as varchar) 1s, cast(1 as TINYINT) as 1b""") { q =>
      q.table shouldBe empty
      q.fields should contain theSameElementsInOrderAs Seq(
        const(1d) as "1d",
        const("1") as "1s",
        const(1.toByte) as "1b"
      )
    }
  }

  it should "fail on incorrect constant convertions" in {
    testError("""SELECT cast (1000 as tinyint)""") {
      _ shouldEqual "Cannot convert const(1000:BigDecimal) of type DECIMAL to TINYINT"
    }
  }

  it should "handle dimension ids" in {
    testQuery("""SELECT id(A) as a_id, A as a
        |  FROM test_table
        |  WHERE time >= timestamp '2020-07-03' AND time <= timestamp '2020-07-06'
        |        AND id(A) IN ('1','2f','fa')
        |""".stripMargin) { q =>
      q.table.value shouldEqual TestSchema.testTable
      q.fields should contain theSameElementsInOrderAs Seq(
        DimensionIdExpr(DIM_A) as "a_id",
        DimensionExpr(DIM_A) as "a"
      )
      q.filter.value shouldEqual and(
        ge(time, const(Time(OffsetDateTime.of(2020, 7, 3, 0, 0, 0, 0, ZoneOffset.UTC)))),
        le(time, const(Time(OffsetDateTime.of(2020, 7, 6, 0, 0, 0, 0, ZoneOffset.UTC)))),
        in(DimensionIdExpr(DIM_A), Set("1", "2f", "fa"))
      )
    }
  }

  it should "handle id in conditions" in {
    testQuery("""SELECT A
                |  FROM test_table
                |  WHERE time >= timestamp '2020-07-03' AND time <= timestamp '2020-07-06'
                |        AND id(A) = 'ab'
                |""".stripMargin) { q =>
      q.table.value shouldEqual TestSchema.testTable
      q.fields should contain theSameElementsInOrderAs Seq(
        DimensionExpr(DIM_A).toField
      )
      q.filter.value shouldEqual and(
        ge(time, const(Time(OffsetDateTime.of(2020, 7, 3, 0, 0, 0, 0, ZoneOffset.UTC)))),
        le(time, const(Time(OffsetDateTime.of(2020, 7, 6, 0, 0, 0, 0, ZoneOffset.UTC)))),
        equ(DimensionIdExpr(DIM_A), const("ab"))
      )
    }
  }

  it should "not allow to use id on other objects" in {
    val q = """SELECT id(testField), testField
              |  FROM test_table
              |  WHERE time >= timestamp '2020-07-03' AND time <= timestamp '2020-07-06'
              |        AND id(A) IN ('1','2','3')
              |""".stripMargin

    testError(q) { _ shouldEqual "Function id is applicable only to dimensions" }
  }

  it should "fail if field name is unknown" in {
    val q =
      "SELECT foo, id(bar) as b FROM test_table WHERE time >= timestamp '2021-10-27' and time < timestamp '2021-10-28'"

    testError(q) { _ shouldEqual "Unknown field foo. Unknown field bar" }
  }

  it should "fail on field names without table" in {
    testError("SELECT foo as f, 5+6 as b") { _ shouldEqual "Unknown field foo" }
  }

  it should "fail if field name is ambiguous" in {
    val q =
      """SELECT sum(testField) as testField
        |  FROM test_table
        |  WHERE time >= timestamp '2021-10-21' and time <= timestamp '2021-10-22'
        |       AND testField = 'a'""".stripMargin

    testError(q) { _ shouldEqual "Ambiguous field testField" }
  }

  it should "fail if unknown function is used" in {
    testError("SELECT reverse('foo')") { _ shouldEqual "Undefined function reverse" }
  }

  it should "fail if array contains different types" in {
    val q =
      """SELECT testField FROM test_table
        |  WHERE time > timestamp '2021-10-21' and time < timestamp '2021-10-30'
        |  AND containsAny(tokens(testStringField), { 'a', 'b', 3 })""".stripMargin

    testError(q) { _ shouldEqual "All expressions must have same type but: const(3:BigDecimal) has type DECIMAL" }
  }

  it should "fail if non boolean expression in where" in {
    val q =
      """SELECT testField from test_table
        |  WHERE time > timestamp '2021-10-21' and time < timestamp '2021-10-30' and testField + 1""".stripMargin

    testError(q) { _ shouldEqual "metric(testField) + const(1.0:Double) has type DOUBLE, but BOOLEAN is required" }
  }

  it should "handle standard health check" in {
    testQuery("SELECT 1 as one") { q =>
      q.table shouldBe empty
      q.fields should contain theSameElementsAs List(const(BigDecimal(1)) as "one")
    }
  }

  it should "handle conditions without tables" in {
    testQuery("SELECT 10 / 2 as five, 5 + 2 as seven WHERE five <= seven") { q =>
      q.table shouldBe empty
      q.fields should contain theSameElementsInOrderAs Seq(
        divFrac(const(BigDecimal(10)), const(BigDecimal(2))) as "five",
        plus(const(BigDecimal(5)), const(BigDecimal(2))) as "seven"
      )
      q.filter.value shouldEqual le(
        divFrac(const(BigDecimal(10)), const(BigDecimal(2))),
        plus(const(BigDecimal(5)), const(BigDecimal(2)))
      )

    }
  }

  it should "transform upsert into data points" in {
    createUpsert("""UPSERT INTO test_table(time, b, a, testField, testStringField)
        |  VALUES(TIMESTAMP '2020-01-02 23:25:40', 21, 'bar', 55, 'baz')""".stripMargin) match {
      case Right(dps) =>
        dps should have size 1
        val dp = dps.head
        dp.table shouldEqual TestSchema.testTable
        dp.time shouldEqual OffsetDateTime.of(2020, 1, 2, 23, 25, 40, 0, ZoneOffset.UTC).toInstant.toEpochMilli
        dp.dimensions shouldEqual Map(TestDims.DIM_B -> 21.toShort, TestDims.DIM_A -> "bar")
        dp.metrics should contain theSameElementsAs Seq(
          MetricValue(TestTableFields.TEST_FIELD, 55d),
          MetricValue(TestTableFields.TEST_STRING_FIELD, "baz")
        )

      case Left(e) => fail(e)
    }
  }

  it should "fail upsert on data type mismatch" in {
    createUpsert("UPSERT INTO test_table (a, time, testField) VALUES (5, 'foo', 'bar')") match {
      case Left(e)  => e shouldEqual "Cannot convert value 'foo' of type VARCHAR to TIMESTAMP"
      case Right(d) => fail(s"Data point $d was created, but shouldn't")
    }
  }

  it should "fail if number is too big for data type" in {
    createUpsert("""UPSERT INTO test_table (time, a, b, testField)
        |   VALUES (TIMESTAMP '2020-04-24 17:45:05', 'foo', 99999, 'bar')""".stripMargin) match {
      case Left(e)  => e shouldEqual "Cannot convert value '99999' of type DECIMAL to SMALLINT"
      case Right(d) => fail(s"Data point $d was created, but shouldn't")
    }
  }

  it should "handle upsert in batch" in {
    val t1 = OffsetDateTime.now().minusDays(1)
    val t2 = t1.plusMinutes(15)
    createUpsert(
      "UPSERT INTO test_table (a, b, time, testField) VALUES (?, ?, ?, ?)",
      Seq(
        Map(
          1 -> parser.TypedValue("aaa"),
          2 -> parser.TypedValue(BigDecimal(12)),
          3 -> parser.TypedValue(Time(t1)),
          4 -> parser.TypedValue(BigDecimal(1.1))
        ),
        Map(
          1 -> parser.TypedValue("ccc"),
          2 -> parser.TypedValue(BigDecimal(34)),
          3 -> parser.TypedValue(Time(t2)),
          4 -> parser.TypedValue(BigDecimal(2.2))
        )
      )
    ) match {
      case Right(dps) =>
        dps should have size 2
        val dp1 = dps(0)
        dp1.table shouldEqual TestSchema.testTable
        dp1.time shouldEqual t1.toInstant.toEpochMilli
        dp1.dimensions shouldEqual Map(TestDims.DIM_B -> 12, TestDims.DIM_A -> "aaa")
        dp1.metrics shouldEqual Seq(MetricValue(TestTableFields.TEST_FIELD, 1.1d))

        val dp2 = dps(1)
        dp2.table shouldEqual TestSchema.testTable
        dp2.time shouldEqual t2.toInstant.toEpochMilli
        dp2.dimensions shouldEqual Map(TestDims.DIM_B -> 34, TestDims.DIM_A -> "ccc")
        dp2.metrics shouldEqual Seq(MetricValue(TestTableFields.TEST_FIELD, 2.2d))

      case Left(e) => fail(e)
    }
  }

  it should "support upsert with multiple values" in {
    val t1 = LocalDateTime.of(2020, 1, 19, 23, 10, 31)
    val t2 = LocalDateTime.of(2020, 1, 19, 23, 11, 2)
    val t3 = LocalDateTime.of(2020, 1, 19, 23, 11, 33)
    createUpsert("""UPSERT INTO test_table (a, b, time, testField) VALUES
        |  ('a', 12, TIMESTAMP '2020-01-19 23:10:31', 1.5),
        |  ('c', 34, TIMESTAMP '2020-01-19 23:11:02', 3),
        |  ('e', 56, TIMESTAMP '2020-01-19 23:11:33', 321.5) """.stripMargin) match {
      case Right(dps) =>
        dps should have size 3

        val dp1 = dps(0)
        dp1.table shouldEqual TestSchema.testTable
        dp1.time shouldEqual t1.atOffset(ZoneOffset.UTC).toInstant.toEpochMilli
        dp1.dimensions shouldEqual Map(TestDims.DIM_B -> 12, TestDims.DIM_A -> "a")
        dp1.metrics shouldEqual Seq(MetricValue(TestTableFields.TEST_FIELD, 1.5d))

        val dp2 = dps(1)
        dp2.table shouldEqual TestSchema.testTable
        dp2.time shouldEqual t2.atOffset(ZoneOffset.UTC).toInstant.toEpochMilli
        dp2.dimensions shouldEqual Map(TestDims.DIM_B -> 34, TestDims.DIM_A -> "c")
        dp2.metrics shouldEqual Seq(MetricValue(TestTableFields.TEST_FIELD, 3d))

        val dp3 = dps(2)
        dp3.table shouldEqual TestSchema.testTable
        dp3.time shouldEqual t3.atOffset(ZoneOffset.UTC).toInstant.toEpochMilli
        dp3.dimensions shouldEqual Map(TestDims.DIM_B -> 56, TestDims.DIM_A -> "e")
        dp3.metrics shouldEqual Seq(MetricValue(TestTableFields.TEST_FIELD, 321.5d))

      case Left(msg) => fail(msg)
    }
  }

  it should "fail whole batch if there is incorrect element" in {
    val t1 = OffsetDateTime.now().minusDays(1)
    val t2 = t1.plusMinutes(15)
    createUpsert(
      "UPSERT INTO test_table (a, b, time, testField) VALUES (?, ?, ?, ?)",
      Seq(
        Map(
          1 -> parser.TypedValue("aaa"),
          2 -> parser.TypedValue(BigDecimal(33)),
          3 -> parser.TypedValue(Time(t1)),
          4 -> parser.TypedValue(BigDecimal(1.1))
        ),
        Map(
          1 -> parser.TypedValue("ccc"),
          2 -> parser.TypedValue(BigDecimal(66)),
          3 -> parser.TypedValue(Time(t2)),
          4 -> parser.TypedValue("2.2")
        )
      )
    ) match {
      case Left(msg) => msg shouldEqual "Cannot convert value '2.2' of type VARCHAR to DOUBLE"
      case Right(d)  => fail(s"Data points $d were created, but shouldn't")
    }
  }

  it should "fail if upserting external field" in {
    createUpsert(
      "UPSERT INTO test_table (a, b, time, testField, testLink_testfield) VALUES (?, ?, ?, ?, ?)",
      Seq(
        Map(
          1 -> parser.TypedValue("aaa"),
          2 -> parser.TypedValue("bbb"),
          3 -> parser.TypedValue(Time(OffsetDateTime.now())),
          4 -> parser.TypedValue(BigDecimal(1.1)),
          5 -> parser.TypedValue("ccc")
        )
      )
    ) match {
      case Left(msg) => msg shouldEqual "External link field testLink_testfield cannot be upserted"
      case Right(d)  => fail(s"Data points $d were created, but shouldn't")
    }
  }

  private def createQuery(sql: String, params: Map[Int, parser.Value] = Map.empty): Either[String, Query] = {
    SqlParser.parse(sql) flatMap {
      case s: parser.Select => sqlQueryProcessor.createQuery(s, params)
      case x                => Left(s"Select expected but got $x")
    }
  }

  private def createUpsert(
      sql: String,
      params: Seq[Map[Int, parser.Value]] = Seq.empty
  ): Either[String, Seq[DataPoint]] = {
    SqlParser.parse(sql) flatMap {
      case u: parser.Upsert => sqlQueryProcessor.createDataPoints(u, params)
      case x                => Left(s"Upsert expected but got $x")
    }
  }

  private def testQuery(query: String)(check: Query => Any) = {
    inside(createQuery(query)) {
      case Right(q: Query) => check(q)
      case Right(s)        => fail(s"Query expected but got $s")
      case Left(msg)       => fail(msg)
    }
  }

  private def testError(query: String)(check: String => Any) = {
    inside(createQuery(query)) {
      case Left(msg) => check(msg)
      case Right(s)  => fail(s"Error expected but got ${s}")
    }
  }
}

package org.yupana.core.sql

import org.joda.time.{ DateTime, DateTimeZone, LocalDateTime, Period }
import org.scalatest.{ FlatSpec, Inside, Matchers, OptionValues }
import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.api.schema.Dimension
import org.yupana.api.types._
import org.yupana.core.{ TestDims, TestLinks, TestSchema, TestTable2Fields, TestTableFields }

class SqlQueryProcessorTest extends FlatSpec with Matchers with Inside with OptionValues {

  import org.yupana.api.query.syntax.All._

  private val sqlParser = new parser.SqlParser
  private val sqlQueryProcessor = new SqlQueryProcessor(TestSchema.schema)

  private def createQuery(sql: String, params: Map[Int, parser.Value] = Map.empty): Either[String, Query] = {
    sqlParser.parse(sql).right flatMap {
      case s: parser.Select => sqlQueryProcessor.createQuery(s, params)
      case x                => fail(s"Select expected but got $x")
    }
  }

  val TAG_A = Dimension("TAG_A")
  val TAG_B = Dimension("TAG_B")

  val TAG_X = Dimension("TAG_X")
  val TAG_Y = Dimension("TAG_Y")

  "SqlQueryProcessor" should "create queries" in {
    testQuery("""SELECT MAX(testField) FROM test_table
        |   WHERE time >= TIMESTAMP '2017-06-12' AND time < TIMESTAMP '2017-06-30' and tag_a = '223322'
        |   GROUP BY day(time)""".stripMargin) { x =>
      x.table.name shouldEqual "test_table"
      x.filter shouldEqual and(
        ge[Time](time, const(Time(new DateTime(2017, 6, 12, 0, 0, DateTimeZone.UTC)))),
        lt[Time](time, const(Time(new DateTime(2017, 6, 30, 0, 0, DateTimeZone.UTC)))),
        equ(dimension(TAG_A), const("223322"))
      )
      x.groupBy should contain theSameElementsAs Seq[Expression](truncDay(time))
      x.fields should contain theSameElementsInOrderAs List(
        max(metric(TestTableFields.TEST_FIELD)) as "max(testField)"
      )
    }
  }

  it should "have no name collisions when different aggregations are applied to the same field" in {
    testQuery("""
        | SELECT max(testField), min(testField), sum(testField) as sum, tag_b as i, count(TAG_B) FROM test_table
        |   WHERE time >= TIMESTAMP '2018-01-01' and time < TIMESTAMP '2018-01-30'
        |   GROUP BY day(time), i
        | """.stripMargin) { q =>
      q.table.name shouldEqual "test_table"
      q.filter shouldBe and(
        ge(time, const(Time(new DateTime(2018, 1, 1, 0, 0, DateTimeZone.UTC)))),
        lt(time, const(Time(new DateTime(2018, 1, 30, 0, 0, DateTimeZone.UTC))))
      )
      q.groupBy should contain theSameElementsAs List(dimension(TAG_B), truncDay(time))
      q.fields should contain theSameElementsInOrderAs List(
        max(metric(TestTableFields.TEST_FIELD)) as "max(testField)",
        min(metric(TestTableFields.TEST_FIELD)) as "min(testField)",
        sum(metric(TestTableFields.TEST_FIELD)) as "sum",
        dimension(TAG_B) as "i",
        count(dimension(TAG_B)) as "count(TAG_B)"
      )
    }
  }

  it should "support <= and > for time" in {
    testQuery(
      """SELECT testField2 FROM test_table
        |  WHERE time > {ts '2017-06-12'} and time <= { ts '2017-06-13' }
      """.stripMargin
    ) { x =>
      x.table.name shouldEqual "test_table"
      x.filter shouldBe and(
        gt(time, const(Time(new DateTime(2017, 6, 12, 0, 0, DateTimeZone.UTC)))),
        le(time, const(Time(new DateTime(2017, 6, 13, 0, 0, DateTimeZone.UTC))))
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
      x.table.name shouldEqual "test_table"
      x.filter shouldEqual and(
        ge(time, const(Time(new DateTime(2017, 8, 23, 0, 0, DateTimeZone.UTC)))),
        lt(time, const(Time(new DateTime(2017, 8, 23, 17, 41, 0, 123, DateTimeZone.UTC))))
      )
      x.fields should contain theSameElementsInOrderAs List(
        metric(TestTableFields.TEST_FIELD) as "testField"
      )
    }
  }

  it should "support time aggregations in field list" in {
    testQuery("""SELECT COUNT(testField), day(time) AS d FROM test_table
        |  WHERE time >= TIMESTAMP '2017-8-1' AND TIME < TIMESTAMP '2017-08-08' AND tag_b = 'простокваша'
        |  GROUP BY day(time)
      """.stripMargin) { x =>
      x.table.name shouldEqual "test_table"
      x.filter shouldEqual and(
        ge(time, const(Time(new DateTime(2017, 8, 1, 0, 0, DateTimeZone.UTC)))),
        lt(time, const(Time(new DateTime(2017, 8, 8, 0, 0, DateTimeZone.UTC)))),
        equ(dimension(TAG_B), const("простокваша"))
      )
      x.groupBy should contain theSameElementsAs Seq[Expression](truncDay(time))
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
      q.table.name shouldEqual "test_table"
      q.filter shouldBe and(
        ge(time, const(Time(new DateTime(2017, 10, 18, 0, 0, DateTimeZone.UTC)))),
        le(time, const(Time(new DateTime(2017, 10, 28, 0, 0, DateTimeZone.UTC))))
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
        |SELECT "receipt"."time" AS "time", "receipt"."tag_a" as "tag_a"
        | FROM "test_table"
        | WHERE (("receipt"."time" >= {ts '2017-10-30 00:00:00'}) AND ("receipt"."time" <= {ts '2017-11-01 00:00:00'}))
        | GROUP BY "receipt"."time", tag_a
      """.stripMargin
    ) { x =>
      x.table.name shouldEqual "test_table"
      x.filter shouldBe and(
        ge(time, const(Time(new DateTime(2017, 10, 30, 0, 0, DateTimeZone.UTC)))),
        le(time, const(Time(new DateTime(2017, 11, 1, 0, 0, DateTimeZone.UTC))))
      )
      x.groupBy should contain theSameElementsAs Seq(time, dimension(TAG_A))
      x.fields should contain theSameElementsAs Seq(
        time as "time",
        dimension(TAG_A) as "tag_a"
      )
    }
  }

  it should "support grouping by tags" in {
    testQuery(
      """SELECT SUM(testField) as sum, day(time) as d FROM test_table
        |  WHERE time >= TIMESTAMP '2017-8-1' AND TIME < TIMESTAMP '2017-08-08 10:30:00'
        |  GROUP BY d, tag_a
      """.stripMargin
    ) { x =>
      x.table.name shouldEqual "test_table"
      x.filter shouldBe and(
        ge(time, const(Time(new DateTime(2017, 8, 1, 0, 0, DateTimeZone.UTC)))),
        lt(time, const(Time(new DateTime(2017, 8, 8, 10, 30, DateTimeZone.UTC))))
      )
      x.groupBy should contain theSameElementsAs Seq(dimension(TAG_A), truncDay(time))
      x.fields should contain theSameElementsInOrderAs List(
        sum(metric(TestTableFields.TEST_FIELD)) as "sum",
        truncDay(time) as "d"
      )
    }
  }

  it should "support filter by external link value" in {
    testQuery(
      """SELECT SUM(testField), day(time) AS d, TestLink_testField as word FROM test_table
        |  WHERE time >= TIMESTAMP '2017-8-1' AND TIME < TIMESTAMP '2017-08-08' AND word = 'простокваша' and TAG_A = '12345'
        |  GROUP BY day(time), word
      """.stripMargin
    ) { x =>
      x.table.name shouldEqual "test_table"
      x.filter shouldEqual and(
        ge(time, const(Time(new DateTime(2017, 8, 1, 0, 0, DateTimeZone.UTC)))),
        lt(time, const(Time(new DateTime(2017, 8, 8, 0, 0, DateTimeZone.UTC)))),
        equ(link(TestLinks.TEST_LINK, "testField"), const("простокваша")),
        equ(dimension(TAG_A), const("12345"))
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
      q.table.name shouldEqual "test_table"
      q.filter shouldBe and(
        ge(time, const(Time(new DateTime(2017, 8, 1, 0, 0, DateTimeZone.UTC)))),
        lt(time, const(Time(new DateTime(2017, 8, 8, 0, 0, DateTimeZone.UTC)))),
        gt(long2BigDecimal(metric(TestTableFields.TEST_LONG_FIELD)), const(BigDecimal(1000)))
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
      q.table.name shouldEqual "test_table"
      q.filter shouldBe and(
        ge(time, const(Time(new DateTime(2017, 8, 1, 0, 0, DateTimeZone.UTC)))),
        lt(time, const(Time(new DateTime(2017, 8, 8, 0, 0, DateTimeZone.UTC)))),
        gt(double2bigDecimal(metric(TestTableFields.TEST_FIELD)), const(BigDecimal(10)))
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
        | SELECT SUM(testField), day(time) as d, tag_b from test_table
        |  WHERE time >= TIMESTAMP '2018-03-26' AND time < TIMESTAMP '2018-03-27' AND TAG_A IN ( '123', '456', '789')
        |  GROUP BY d, tag_b
      """.stripMargin
    ) { q =>
      q.table.name shouldEqual "test_table"
      q.filter shouldBe and(
        ge(time, const(Time(new DateTime(2018, 3, 26, 0, 0, DateTimeZone.UTC)))),
        lt(time, const(Time(new DateTime(2018, 3, 27, 0, 0, DateTimeZone.UTC)))),
        in(dimension(TAG_A), Set("123", "456", "789"))
      )
      q.groupBy should contain theSameElementsAs List(dimension(TAG_B), truncDay(time))
      q.fields should contain theSameElementsInOrderAs List(
        sum(metric(TestTableFields.TEST_FIELD)) as "sum(testField)",
        truncDay(time) as "d",
        dimension(TAG_B) as "tag_b"
      )
    }
  }

  it should "support IN conditions for values" in {
    testQuery(
      """
        | SELECT SUM(testField), day(time) as d, tag_b from test_table
        |  WHERE time >= TIMESTAMP '2018-03-26' AND time < TIMESTAMP '2018-03-27' AND testField2 IN (123, 456, 789)
        |  GROUP BY d, TAG_B
      """.stripMargin
    ) { q =>
      q.table.name shouldEqual "test_table"
      q.filter shouldBe and(
        ge(time, const(Time(new DateTime(2018, 3, 26, 0, 0, DateTimeZone.UTC)))),
        lt(time, const(Time(new DateTime(2018, 3, 27, 0, 0, DateTimeZone.UTC)))),
        in(metric(TestTableFields.TEST_FIELD2), Set(123d, 456d, 789d))
      )
      q.groupBy should contain theSameElementsAs List(dimension(TAG_B), truncDay(time))
      q.fields should contain theSameElementsInOrderAs List(
        sum(metric(TestTableFields.TEST_FIELD)) as "sum(testField)",
        truncDay(time) as "d",
        dimension(TAG_B) as "tag_b"
      )
    }
  }

  it should "support NOT IN conditions" in {
    testQuery("""
        | SELECT MIN(testField) tf, trunc_month(time) m FROM test_table
        |   WHERE time >= TIMESTAMP '2019-03-30' and time < TIMESTAMP '2019-03-31' AND testField2 NOT IN (5, 6, 7)
        |   GROUP BY m
      """.stripMargin) { q =>
      q.table.name shouldEqual "test_table"
      q.filter shouldEqual and(
        ge(time, const(Time(new DateTime(2019, 3, 30, 0, 0, DateTimeZone.UTC)))),
        lt(time, const(Time(new DateTime(2019, 3, 31, 0, 0, DateTimeZone.UTC)))),
        notIn(metric(TestTableFields.TEST_FIELD2), Set(5d, 6d, 7d))
      )
      q.fields should contain theSameElementsInOrderAs List(
        min(metric(TestTableFields.TEST_FIELD)) as "tf",
        truncMonth(time) as "m"
      )
      q.groupBy should contain theSameElementsInOrderAs List(truncMonth(time))
    }
  }

  it should "support functions as conditions" in {
    testQuery(
      """
        |SELECT tag_a, array_to_string(tokens(tag_a))
        |  FROM test_table
        |  WHERE time >= timestamp '2019-03-14' and time < TIMESTAMP '2019-03-15' and contains_any(tokens(tag_a), tokens('вода'))
      """.stripMargin
    ) { q =>
      q.table.name shouldEqual "test_table"
      q.fields should contain theSameElementsInOrderAs List(
        dimension(TAG_A) as "tag_a",
        function(UnaryOperation.arrayToString[String], function(UnaryOperation.tokens, dimension(TAG_A))) as "array_to_string(tokens(tag_a))"
      )
      q.filter shouldBe and(
        ge(time, const(Time(new DateTime(2019, 3, 14, 0, 0, DateTimeZone.UTC)))),
        lt(time, const(Time(new DateTime(2019, 3, 15, 0, 0, DateTimeZone.UTC)))),
        bi(
          BinaryOperation.containsAny[String],
          function(UnaryOperation.tokens, dimension(TAG_A)),
          function(UnaryOperation.tokens, const("вода"))
        )
      )
    }
  }

  it should "support functions of arrays" in {
    testQuery("""
        |SELECT
        |  tag_b,
        |  case
        |    when contains_any(tokens(tag_a), tokens('крыжовник')) then 'зеленые'
        |    when contains_any(tokens(tag_a), tokens('клубника', 'малина')) then 'красные'
        |    when contains_any(tokens(tag_a), tokens('черника', 'ежевика', 'ирга')) then 'черные'
        |    else 'прочие' as color,
        |  sum(testField)
        |FROM test_table
        |WHERE time >= timestamp '2019-03-14' AND time < timestamp '2019-03-26' AND TestLink_testField = 'ягода'
        |GROUP BY tag_b, color
      """.stripMargin) { q =>
      q.table.name shouldEqual "test_table"

      val colorExpr = condition(
        bi(
          BinaryOperation.containsAny[String],
          function(UnaryOperation.tokens, dimension(TAG_A)),
          function(UnaryOperation.tokens, const("крыжовник"))
        ),
        const("зеленые"),
        condition(
          bi(
            BinaryOperation.containsAny[String],
            function(UnaryOperation.tokens, dimension(TAG_A)),
            function(UnaryOperation.tokenizeArray, array(const("клубника"), const("малина")))
          ),
          const("красные"),
          condition(
            bi(
              BinaryOperation.containsAny[String],
              function(UnaryOperation.tokens, dimension(TAG_A)),
              function(UnaryOperation.tokenizeArray, array(const("черника"), const("ежевика"), const("ирга")))
            ),
            const("черные"),
            const("прочие")
          )
        )
      )

      q.fields should contain theSameElementsInOrderAs Seq(
        dimension(TAG_B) as "tag_b",
        colorExpr as "color",
        sum(metric(TestTableFields.TEST_FIELD)) as "sum(testField)"
      )

      q.filter shouldEqual and(
        ge(time, const(Time(new DateTime(2019, 3, 14, 0, 0, DateTimeZone.UTC)))),
        lt(time, const(Time(new DateTime(2019, 3, 26, 0, 0, DateTimeZone.UTC)))),
        equ(link(TestLinks.TEST_LINK, "testField"), const("ягода"))
      )

      q.groupBy should contain theSameElementsAs List(dimension(TAG_B), colorExpr)
    }
  }

  it should "substitute passed placeholders values" in {
    val statement =
      """SELECT SUM(TestField), month(time) as m, tag_b FROM test_table
        | WHERE time >= ? and time < ? AND tag_a = ?
        | GROUP BY m, tag_b
      """.stripMargin

    val from = new LocalDateTime(2017, 9, 1, 0, 0)
    val to = new LocalDateTime(2017, 9, 15, 0, 0)

    inside(
      createQuery(
        statement,
        Map(1 -> parser.TimestampValue(from), 2 -> parser.TimestampValue(to), 3 -> parser.StringValue("123456789"))
      )
    ) {
      case Right(q) =>
        q.table.name shouldEqual "test_table"
        q.filter shouldBe and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          equ(dimension(TAG_A), const("123456789"))
        )
        q.groupBy should contain theSameElementsAs List(dimension(TAG_B), truncMonth(time))
        q.fields should contain theSameElementsInOrderAs List(
          sum(metric(TestTableFields.TEST_FIELD)) as "sum(TestField)",
          truncMonth(time) as "m",
          dimension(TAG_B) as "tag_b"
        )
      case Left(msg) => fail(msg)
    }
  }

  it should "handle parenthesis" in {
    testQuery(
      """
        |SELECT "test_table_2"."TAG_X" AS "tagX", "test_table_2"."time" AS "time"
        |  FROM "test_table_2"
        |  WHERE (("receipt"."time" >= {ts '2017-10-23 00:00:00'}) AND (("receipt"."time" <= {ts '2017-11-02 00:00:00'}) AND ("receipt"."TAG_X" = '0001388410039121')))
        |  GROUP BY "receipt"."tag_X",
        |    "receipt"."time"
    """.stripMargin
    ) { q =>
      q.table.name shouldEqual "test_table_2"
      q.filter shouldBe and(
        ge(time, const(Time(new DateTime(2017, 10, 23, 0, 0, DateTimeZone.UTC)))),
        le(time, const(Time(new DateTime(2017, 11, 2, 0, 0, DateTimeZone.UTC)))),
        equ(dimension(TAG_X), const("0001388410039121"))
      )
      q.groupBy should contain theSameElementsAs List(dimension(TAG_X), time)
      q.fields should contain theSameElementsInOrderAs List(
        dimension(TAG_X) as "tagX",
        time as "time"
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
        |     TAG_A
        |   FROM test_table
        |   WHERE time >= TIMESTAMP '2017-01-01' AND time < TIMESTAMP '2017-02-01'
        |   GROUP BY d, TAG_A
        | ) "Query"
      """.stripMargin) { q =>
      q.table.name shouldEqual "test_table"
      q.filter shouldBe and(
        ge(time, const(Time(new DateTime(2017, 1, 1, 0, 0, DateTimeZone.UTC)))),
        lt(time, const(Time(new DateTime(2017, 2, 1, 0, 0, DateTimeZone.UTC))))
      )
      q.groupBy should contain theSameElementsAs List(dimension(TAG_A), truncDay(time))
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
        Map(1 -> parser.StringValue("Test me"), 2 -> parser.TimestampValue(new LocalDateTime(2018, 1, 23, 16, 44, 20)))
      )
    ) {
      case Right(q) =>
        q.table.name shouldEqual "test_table"
        q.filter shouldBe and(
          ge(time, const(Time(new DateTime(2018, 1, 1, 0, 0, DateTimeZone.UTC)))),
          lt(time, const(Time(new DateTime(2018, 1, 23, 16, 44, 20, DateTimeZone.UTC))))
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
        | SELECT count(TAG_A) as count_tag_a, max(testLink3_testField3_2), day(time)
        |   FROM test_table
        |   WHERE time > TIMESTAMP '2017-11-01' AND time < TIMESTAMP '2017-12-01'
        |   GROUP BY day(time)
      """.stripMargin) { q =>
      q.table.name shouldEqual "test_table"
      q.filter shouldBe and(
        gt(time, const(Time(new DateTime(2017, 11, 1, 0, 0, DateTimeZone.UTC)))),
        lt(time, const(Time(new DateTime(2017, 12, 1, 0, 0, DateTimeZone.UTC))))
      )
      q.groupBy should contain theSameElementsAs Seq(truncDay(time))
      q.fields should contain theSameElementsInOrderAs List(
        count(dimension(TAG_A)) as "count_tag_a",
        max(link(TestLinks.TEST_LINK3, "testField3_2")) as "max(testLink3_testField3_2)",
        truncDay(time) as "day(time)"
      )
    }
  }

  it should "support named binary functions" in {
    testQuery("""
        |SELECT tag_a, array_to_string(tokens(tag_a)), contains_any(tokens(tag_a), tokens('вода')) as is_water
        |  FROM test_table
        |  WHERE time >= timestamp '2019-03-14' and time < TIMESTAMP '2019-03-15'
      """.stripMargin) { q =>
      q.table.name shouldEqual "test_table"
      q.fields should contain theSameElementsInOrderAs List(
        dimension(TAG_A) as "tag_a",
        function(UnaryOperation.arrayToString[String], function(UnaryOperation.tokens, dimension(TAG_A))) as "array_to_string(tokens(tag_a))",
        bi(
          BinaryOperation.containsAny[String],
          function(UnaryOperation.tokens, dimension(TAG_A)),
          function(UnaryOperation.tokens, const("вода"))
        ) as "is_water"
      )
      q.filter shouldBe and(
        ge(time, const(Time(new DateTime(2019, 3, 14, 0, 0, DateTimeZone.UTC)))),
        lt(time, const(Time(new DateTime(2019, 3, 15, 0, 0, DateTimeZone.UTC))))
      )
    }
  }

  it should "support window functions" in {
    testQuery("""
        | SELECT tag_a, lag(time), lag(testField) as lag_totalSum
        |   FROM test_table
        |   WHERE time > TIMESTAMP '2017-11-01' AND time < TIMESTAMP '2017-12-01'
        |   GROUP BY tag_a
      """.stripMargin) { q =>
      q.table.name shouldEqual "test_table"
      q.filter shouldBe and(
        gt(time, const(Time(new DateTime(2017, 11, 1, 0, 0, DateTimeZone.UTC)))),
        lt(time, const(Time(new DateTime(2017, 12, 1, 0, 0, DateTimeZone.UTC))))
      )
      q.groupBy should contain theSameElementsAs Set(dimension(TAG_A))
      q.fields should contain theSameElementsInOrderAs List(
        dimension(TAG_A) as "tag_a",
        windowFunction(WindowOperation.lag[Time], time) as "lag(time)",
        windowFunction(WindowOperation.lag[Double], metric(TestTableFields.TEST_FIELD)) as "lag_totalSum"
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
        |   WHERE time >= TIMESTAMP '2018-1-1' AND time < TIMESTAMP '2018-2-1' AND TAG_X = '1234567890'
      """.stripMargin) { q =>
      q.table.name shouldEqual "test_table_2"
      q.filter shouldBe and(
        ge(time, const(Time(new DateTime(2018, 1, 1, 0, 0, DateTimeZone.UTC)))),
        lt(time, const(Time(new DateTime(2018, 2, 1, 0, 0, DateTimeZone.UTC)))),
        equ(dimension(TAG_X), const("1234567890"))
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
      q.table.name shouldEqual "test_table"
      q.filter shouldBe and(
        ge(time, const(Time(new DateTime(2018, 1, 1, 0, 0, DateTimeZone.UTC)))),
        lt(time, const(Time(new DateTime(2018, 2, 1, 0, 0, DateTimeZone.UTC))))
      )
      q.groupBy shouldBe Seq(time)
      q.fields should contain theSameElementsInOrderAs List(
        time as "time",
        sum(
          condition(
            gt(double2bigDecimal(metric(TestTableFields.TEST_FIELD)), const(BigDecimal(10))),
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
        |     WHEN tag_y = '1' THEN 1
        |     ELSE 0
        |  ) AS count,
        |   sum(case
        |     WHEN tag_y = '1' THEN testField
        |     ELSE 0
        |  ) AS sum
        |  FROM test_table_2
        |  WHERE time >= TIMESTAMP '2018-1-1' and time < TIMESTAMP '2018-2-1'
        |  GROUP BY time
      """.stripMargin) { q =>
      q.table.name shouldEqual "test_table_2"
      q.filter shouldBe and(
        ge(time, const(Time(new DateTime(2018, 1, 1, 0, 0, DateTimeZone.UTC)))),
        lt(time, const(Time(new DateTime(2018, 2, 1, 0, 0, DateTimeZone.UTC))))
      )
      q.groupBy shouldBe Seq(time)
      q.fields should contain theSameElementsInOrderAs List(
        time as "time",
        sum(
          condition(
            equ(dimension(TAG_Y), const("1")),
            const(BigDecimal(1)),
            const(BigDecimal(0))
          )
        ) as "count",
        sum(
          condition(
            equ(dimension(TAG_Y), const("1")),
            metric(TestTable2Fields.TEST_FIELD),
            const(BigDecimal(0))
          )
        ) as "sum"
      )
    }
  }

  it should "support having expressions" in {
    testQuery("""
        | SELECT
        |  tag_a,
        |  time AS t,
        |  lag(time) AS lagTime
        | FROM test_table
        | WHERE time < TIMESTAMP '2018-02-01' AND time > TIMESTAMP '2018-01-01'
        | GROUP BY tag_a
        | HAVING
        |  ((lagTime - t) > INTERVAL '2:00:00' AND extract_hour(t) >= 8 AND extract_hour(t) <= 18) OR
        |  ((lagTime - t) > INTERVAL '4:00:00' AND (extract_hour(t) > 18 OR extract_hour(t) < 8))
      """.stripMargin) { q =>
      q.table.name shouldEqual "test_table"
      q.filter shouldBe and(
        lt(time, const(Time(new DateTime(2018, 2, 1, 0, 0, DateTimeZone.UTC)))),
        gt(time, const(Time(new DateTime(2018, 1, 1, 0, 0, DateTimeZone.UTC))))
      )
      q.groupBy should contain theSameElementsAs List(dimension(TAG_A))

      val lagTime = windowFunction(WindowOperation.lag[Time], time).asInstanceOf[Expression.Aux[Time]]

      q.fields should contain theSameElementsInOrderAs List(
        dimension(TAG_A) as "tag_a",
        time as "t",
        lagTime as "lagTime"
      )

      val expectedPostFilter = or(
        and(
          gt(
            bi(
              BinaryOperation.timeMinusTime,
              lagTime,
              time
            ),
            const(2 * 3600 * 1000L)
          ),
          ge(int2bigDecimal(extractHour(time)), const(BigDecimal(8))),
          le(int2bigDecimal(extractHour(time)), const(BigDecimal(18)))
        ),
        and(
          gt(
            bi(
              BinaryOperation.timeMinusTime,
              lagTime,
              time
            ),
            const(4 * 3600 * 1000L)
          ),
          or(
            gt(int2bigDecimal(extractHour(time)), const(BigDecimal(18))),
            lt(int2bigDecimal(extractHour(time)), const(BigDecimal(8)))
          )
        )
      )

      q.postFilter.value shouldEqual expectedPostFilter
    }
  }

  it should "handle big intervals" in {
    testQuery("""
        | SELECT
        |  tag_a,
        |  time AS t,
        |  lag(time) AS lagTime
        | FROM test_table
        | WHERE time < TIMESTAMP '2018-08-01' AND time >= TIMESTAMP '2018-07-01'
        | GROUP BY tag_a
        | HAVING (lagTime - t) >= INTERVAL '5' DAY
      """.stripMargin) { q =>
      q.table.name shouldEqual "test_table"
      q.filter shouldBe and(
        lt(time, const(Time(new DateTime(2018, 8, 1, 0, 0, 0, 0, DateTimeZone.UTC)))),
        ge(time, const(Time(new DateTime(2018, 7, 1, 0, 0, 0, 0, DateTimeZone.UTC))))
      )
      q.groupBy should contain theSameElementsAs List(dimension(TAG_A))

      val t = time as "t"
      val lagTime = windowFunction(WindowOperation.lag[Time], time) as "lagTime"

      q.fields should contain theSameElementsInOrderAs List(
        dimension(TAG_A) as "tag_a",
        t,
        lagTime
      )
      q.postFilter.value shouldEqual ge(
        bi(
          BinaryOperation.timeMinusTime,
          lagTime.expr.asInstanceOf[Expression.Aux[Time]],
          t.expr.asInstanceOf[Expression.Aux[Time]]
        ),
        const(5 * 24 * 3600 * 1000L)
      )
    }
  }

  it should "handle period arithmetic" in {
    val now = new DateTime(DateTimeZone.UTC).getMillis
    testQuery("""
        |SELECT SUM(testField) as sum, day(time) as d FROM test_table
        |  WHERE time >= trunc_day(now() - INTERVAL '3' MONTH) AND TIME < trunc_day(now())
        |  GROUP BY d, tag_a
      """.stripMargin) { q =>
      q.table.name shouldEqual "test_table"
      inside(q.filter) {
        case AndExpr(Seq(from, to)) =>
          inside(from) {
            case BinaryOperationExpr(
                cmp,
                te,
                UnaryOperationExpr(uo, BinaryOperationExpr(op, ConstantExpr(t), ConstantExpr(p)))
                ) =>
              cmp.name shouldEqual BinaryOperation.ge[Time].name
              te shouldEqual TimeExpr
              uo shouldEqual UnaryOperation.truncDay
              op.name shouldEqual "-"
              t.asInstanceOf[Time].millis shouldEqual (now +- 1000L)
              p shouldEqual Period.months(3)
          }
          inside(to) {
            case BinaryOperationExpr(cmp, te, UnaryOperationExpr(uo, ConstantExpr(t))) =>
              cmp.name shouldEqual BinaryOperation.lt[Time].name
              te shouldEqual TimeExpr
              uo shouldEqual UnaryOperation.truncDay
              t.asInstanceOf[Time].millis shouldEqual (now +- 1000L)
          }
      }

      q.fields should contain theSameElementsAs Seq(
        sum(metric(TestTableFields.TEST_FIELD)) as "sum",
        truncDay(time) as "d"
      )
      q.groupBy should contain theSameElementsAs Seq(
        truncDay(time),
        dimension(TAG_A)
      )
    }
  }

  it should "handle queries like this" in {
    testQuery("""SELECT
        |sum(CASE WHEN tag_b = '2' THEN 1 ELSE 0) AS salesTicketsCount, day(time) AS d
        |FROM test_table
        |WHERE time >= TIMESTAMP '2018-09-03 14:08:05' AND time < TIMESTAMP '2018-09-03 14:08:17'
        |GROUP BY d;
      """.stripMargin) { q =>
      q.table.name shouldEqual "test_table"
      q.fields should contain theSameElementsAs Seq(
        sum(
          condition(
            equ(dimension(TestDims.TAG_B), const("2")),
            const(BigDecimal(1)),
            const(BigDecimal(0))
          )
        ) as "salesTicketsCount",
        truncDay(time) as "d"
      )
    }
  }

  it should "contains camel case in catalog" in {
    testQuery("""
        | SELECT DynamicLink_someField FROM test_table_2
        |   WHERE time >= TIMESTAMP '2018-01-01' and time < TIMESTAMP '2018-01-30'
        | """.stripMargin) { q =>
      q.table.name shouldEqual "test_table_2"
      q.filter shouldBe and(
        ge(time, const(Time(new DateTime(2018, 1, 1, 0, 0, DateTimeZone.UTC)))),
        lt(time, const(Time(new DateTime(2018, 1, 30, 0, 0, DateTimeZone.UTC))))
      )
      q.groupBy shouldBe empty
      q.fields should contain theSameElementsInOrderAs List(
        link(TestLinks.DYNAMIC_LINK, "someField") as "DynamicLink_someField"
      )
    }
  }

  it should "handle IS NULL and IS NOT NULL conditions" in {
    testQuery("""
        |SELECT sum(testField), day(time) as d FROM test_table
        |  WHERE TestLink_testField IS NULL AND testField2 IS NOT NULL
        |  AND time < TIMESTAMP '2018-08-01' AND time >= TIMESTAMP '2018-07-01'
        |  GROUP BY d, TAG_A
      """.stripMargin) { q =>
      q.table.name shouldEqual "test_table"
      q.fields should contain theSameElementsAs Seq(
        sum(metric(TestTableFields.TEST_FIELD)) as "sum(testField)",
        truncDay(time) as "d"
      )
      q.filter shouldEqual and(
        isNull(link(TestLinks.TEST_LINK, "testField")),
        isNotNull(metric(TestTableFields.TEST_FIELD2)),
        lt(time, const(Time(new DateTime(2018, 8, 1, 0, 0, DateTimeZone.UTC)))),
        ge(time, const(Time(new DateTime(2018, 7, 1, 0, 0, DateTimeZone.UTC))))
      )
      q.groupBy should contain theSameElementsAs Seq(
        truncDay(time),
        dimension(TAG_A)
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
      q.table.name shouldEqual "test_table"
      val condExpr = condition(
        isNotNull(link(TestLinks.TEST_LINK, "testField")),
        double2bigDecimal(metric(TestTableFields.TEST_FIELD)),
        const(BigDecimal(0))
      )
      q.fields should contain theSameElementsAs Seq(
        sum(condExpr) as "quantity",
        truncDay(time) as "d"
      )
      q.filter shouldBe and(
        equ(link(TestLinks.TEST_LINK2, "testField2"), const("464")),
        lt(time, const(Time(new DateTime(2018, 8, 1, 0, 0, DateTimeZone.UTC)))),
        ge(time, const(Time(new DateTime(2018, 7, 1, 0, 0, DateTimeZone.UTC))))
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
        |    tag_a,
        |    tag_b
        |FROM
        |    test_table
        |WHERE
        |    time >= TIMESTAMP '2018-08-01' AND
        |    time < TIMESTAMP '2018-09-01'  AND testField < 50000 AND tag_a = '0000348521023155'
        |group by
        |    d,
        |    TAG_B, TAG_A
      """.stripMargin) { q =>
      q.fields should contain theSameElementsAs Seq(
        sum(metric(TestTableFields.TEST_FIELD)) as "sum(testField)",
        max(metric(TestTableFields.TEST_FIELD)) as "max(testField)",
        dimension(TAG_A) as "tag_a",
        dimension(TAG_B) as "tag_b",
        truncMonth(time) as "d"
      )

      q.filter shouldBe and(
        ge(time, const(Time(new DateTime(2018, 8, 1, 0, 0, DateTimeZone.UTC)))),
        lt(time, const(Time(new DateTime(2018, 9, 1, 0, 0, DateTimeZone.UTC)))),
        lt[BigDecimal](double2bigDecimal(metric(TestTableFields.TEST_FIELD)), const(BigDecimal(50000))),
        equ[String](dimension(TAG_A), const("0000348521023155"))
      )

      q.groupBy should contain theSameElementsAs Seq(
        truncMonth(time),
        dimension(TAG_B),
        dimension(TAG_A)
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
      val mult = bi(
        BinaryOperation.multiply[BigDecimal](DataType.fracDt),
        const(BigDecimal(2)),
        double2bigDecimal(metric(TestTable2Fields.TEST_FIELD2))
      )
      val minus = bi(BinaryOperation.minus[BigDecimal](mult.dataType), metric(TestTable2Fields.TEST_FIELD), mult)
      q.fields should contain theSameElementsAs Seq(
        max(minus) as "strange_result",
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
      val rr = bi(BinaryOperation.multiply[BigDecimal](r.dataType), const(BigDecimal(2)), r)
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

    inside(createQuery(q)) {
      case Left(msg) =>
        msg shouldBe "Invalid expression 'max(metric(testField)) - const(2) * metric(testField3)' for field strange_result"
    }
  }

  it should "cast long to double" in {
    testQuery(
      "SELECT testField + testLongField as plus2 FROM test_table WHERE time >= TIMESTAMP '2018-10-16 17:44:47' " +
        "AND time <= TIMESTAMP '2018-10-16 17:44:51' AND tag_b = 'фальш-камера поворотная elro'"
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
      q.table shouldEqual TestSchema.testTable
      q.fields should contain theSameElementsInOrderAs Seq(
        sum(abs(minus(metric(TestTableFields.TEST_LONG_FIELD)))) as "abs1",
        abs(
          sum(
            condition(
              lt(double2bigDecimal(metric(TestTableFields.TEST_FIELD)), const(BigDecimal(40000))),
              plus(const(BigDecimal(-10)), const(BigDecimal(5))),
              double2bigDecimal(minus(metric(TestTableFields.TEST_FIELD)))
            )
          )
        ) as "abs2",
        metric(TestTableFields.TEST_STRING_FIELD).toField
      )

      q.filter shouldEqual and(
        ge(time, const(Time(new DateTime(2019, 4, 10, 0, 0, DateTimeZone.UTC)))),
        le(time, const(Time(new DateTime(2019, 4, 11, 0, 0, DateTimeZone.UTC)))),
        lt[BigDecimal](long2BigDecimal(minus(metric(TestTableFields.TEST_LONG_FIELD))), const(BigDecimal(-100)))
      )
    }
  }

  private def testQuery(query: String)(check: Query => Any) = {
    inside(createQuery(query)) {
      case Right(q)  => check(q)
      case Left(msg) => fail(msg)
    }
  }
}

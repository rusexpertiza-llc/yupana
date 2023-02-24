package org.yupana.core.sql.parser

import org.scalactic.source
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.Inside
import fastparse._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.threeten.extra.PeriodDuration

import java.time.format.DateTimeFormatter
import java.time.{ Duration, OffsetDateTime, Period, ZoneOffset }

class SqlParserTest extends AnyFlatSpec with Matchers with Inside with ParsedValues with TableDrivenPropertyChecks {

  "Value parser" should "parse strings" in {
    parse("''", ValueParser.value(_)).value shouldEqual StringValue("")
    parse("'test me'", ValueParser.value(_)).value shouldEqual StringValue("test me")
    parse("' with spaces '", ValueParser.value(_)).value shouldEqual StringValue(" with spaces ")
  }

  it should "support escaped text" in {
    parse("'slash \\\\'", ValueParser.value(_)).value shouldEqual StringValue("slash \\")
    parse("'\\'escaped\\' quotes'", ValueParser.value(_)).value shouldEqual StringValue("'escaped' quotes")
    parse("'multi\\nline'", ValueParser.value(_)).value shouldEqual StringValue("multi\nline")
    parse("'multi\\n\\rline'", ValueParser.value(_)).value shouldEqual StringValue("multi\n\rline")
    parse("'col1\\tcol2'", ValueParser.value(_)).value shouldEqual StringValue("col1\tcol2")
    parse("'test\\g'", ValueParser.value(_)).error should include("found \"g'")
  }

  it should "parse integer numbers" in {
    parse("1234567", ValueParser.value(_)).value shouldEqual NumericValue(1234567)
  }

  it should "parse decimal values" in {
    parse("1234567.89", ValueParser.value(_)).value shouldEqual NumericValue(1234567.89)
  }

  it should "parse timestamps" in {
    parse("TIMESTAMP '2017-08-23 12:44:02.000'", ValueParser.value(_)).value shouldEqual TimestampValue(
      OffsetDateTime.of(2017, 8, 23, 12, 44, 2, 0, ZoneOffset.UTC)
    )

    parse("TIMESTAMP '2017-08-23'", ValueParser.value(_)).value shouldEqual TimestampValue(
      OffsetDateTime.of(2017, 8, 23, 0, 0, 0, 0, ZoneOffset.UTC)
    )

    parse("TIMESTAMP '2018-08-4 22:25:51.03'", ValueParser.value(_)).value shouldEqual TimestampValue(
      OffsetDateTime.of(2018, 8, 4, 22, 25, 51, 30, ZoneOffset.UTC)
    )
  }

  it should "support alternative timestamp syntax" in {
    parse("{TS '2017-10-31 00:00:00' }", ValueParser.value(_)).value shouldEqual TimestampValue(
      OffsetDateTime.of(2017, 10, 31, 0, 0, 0, 0, ZoneOffset.UTC)
    )
  }

  it should "support simple format intervals" in {
    parse("INTERVAL '3:15:20'", ValueParser.value(_)).value shouldEqual PeriodValue(
      PeriodDuration.of(Duration.ofHours(3).plusMinutes(15).plusSeconds(20))
    )

    parse("INTERVAL '0:6:50.123'", ValueParser.value(_)).value shouldEqual PeriodValue(
      PeriodDuration.of(Duration.ofMinutes(6).plusSeconds(50).plusMillis(123))
    )

    parse("INTERVAL '10 12:00:00'", ValueParser.value(_)).value shouldEqual PeriodValue(
      PeriodDuration.of(Period.ofDays(10), Duration.ofHours(12))
    )
  }

  it should "parse booleans" in {
    parse("true", ValueParser.value(_)).value shouldEqual BooleanValue(true)
    parse("FALSE", ValueParser.value(_)).value shouldEqual BooleanValue(false)
  }

  it should "support single field SQL intervals" in {
    val cases = Table(
      ("SQL", "Period"),
      ("INTERVAL '2' YEAR", PeriodDuration.of(Period.ofYears(2))),
      ("INTERVAL '3' MONTH", PeriodDuration.of(Period.ofMonths(3))),
      ("INTERVAL '6' DAY", PeriodDuration.of(Period.ofDays(6))),
      ("INTERVAL '150' HOUR", PeriodDuration.of(Duration.ofHours(150))),
      ("INTERVAL '5' minute", PeriodDuration.of(Duration.ofMinutes(5))),
      ("INTERVAL '33' SECOND", PeriodDuration.of(Duration.ofSeconds(33))),
      ("INTERVAL '5.66' SECOND", PeriodDuration.of(Duration.ofSeconds(5).plusMillis(660)))
    )

    forAll(cases) { (sql, period) =>
      parse(sql, ValueParser.value(_)).value shouldEqual PeriodValue(period)
    }
  }

  it should "support multi field SQL intervals" in {
    val cases = Table(
      ("SQL", "period"),
      ("INTERVAL '1-10' YEAR TO MONTH", PeriodDuration.of(Period.ofYears(1).plusMonths(10))),
      ("INTERVAL '10:15' HOUR TO MINUTE", PeriodDuration.of(Duration.ofHours(10).plusMinutes(15))),
      ("INTERVAL '400 5' DAY TO HOUR", PeriodDuration.of(Period.ofDays(400), Duration.ofHours(5))),
      ("INTERVAL '10:22' MINUTE TO SECOND", PeriodDuration.of(Duration.ofMinutes(10).plusSeconds(22))),
      (
        "INTERVAL '5:10:22.6' hour to second",
        PeriodDuration.of(Duration.ofHours(5).plusMinutes(10).plusSeconds(22).plusMillis(600))
      ),
      ("INTERVAL '8-10 15' MONTH to HOUR", PeriodDuration.of(Period.ofMonths(8).plusDays(10), Duration.ofHours(15))),
      ("INTERVAL '2-11-15' year to DAY", PeriodDuration.of(Period.ofYears(2).plusMonths(11).plusDays(15))),
      (
        "INTERVAL '4 5:12:10.222' day to second",
        PeriodDuration.of(Period.ofDays(4), Duration.ofHours(5).plusMinutes(12).plusSeconds(10).plusMillis(222))
      )
    )

    forAll(cases) { (sql, period) =>
      parse(sql, ValueParser.value(_)).value shouldEqual PeriodValue(period)
    }
  }

  "SqlParser" should "parse simple SQL select statements" in {

    val statement = "SELECT sum, quantity FROM items WHERE time >= 54321 AND time < 939393"

    parsed(statement) {
      case Select(Some(schema), SqlFieldList(fields), Some(condition), groupings, None, None) =>
        schema shouldEqual "items"
        fields should contain theSameElementsAs List(SqlField(FieldName("sum")), SqlField(FieldName("quantity")))
        condition shouldEqual And(
          Seq(
            Ge(FieldName("time"), Constant(NumericValue(54321))),
            Lt(FieldName("time"), Constant(NumericValue(939393)))
          )
        )
        groupings shouldBe empty
    }
  }

  it should "support all fields selector" in {
    val statement = "SELECT * FROM items WHERE time >= 12345678 AND time < 23456789"

    parsed(statement) {
      case Select(Some(schema), SqlFieldsAll, Some(condition), groupings, None, None) =>
        schema shouldEqual "items"
        condition shouldEqual And(
          Seq(
            Ge(FieldName("time"), Constant(NumericValue(12345678))),
            Lt(FieldName("time"), Constant(NumericValue(23456789)))
          )
        )
        groupings shouldBe empty
    }
  }

  it should "support parentheses in conditions" in {
    val statement =
      "SELECT sum, quantity FROM items WHERE ((time >= 12345678) AND ((time < 23456789) and kkmId = '123456'))"

    parsed(statement) {
      case Select(Some(schema), SqlFieldList(fields), Some(condition), groupings, None, None) =>
        schema shouldEqual "items"
        fields should contain theSameElementsAs List(SqlField(FieldName("sum")), SqlField(FieldName("quantity")))
        condition shouldEqual And(
          Seq(
            Ge(FieldName("time"), Constant(NumericValue(12345678))),
            And(
              Seq(
                Lt(FieldName("time"), Constant(NumericValue(23456789))),
                Eq(FieldName("kkmId"), Constant(StringValue("123456")))
              )
            )
          )
        )
        groupings shouldBe empty
    }
  }

  it should "support or in conditions" in {
    val statement = "SELECT foo FROM bar WHERE a > 5 AND a < 10 OR a >= 30 AND a <= 40 OR (b = 10 OR b = 42)"

    parsed(statement) {
      case Select(Some(schema), SqlFieldList(fields), Some(condition), Nil, None, None) =>
        schema shouldEqual "bar"
        fields should contain theSameElementsAs List(SqlField(FieldName("foo")))
        condition shouldEqual Or(
          Seq(
            And(Seq(Gt(FieldName("a"), Constant(NumericValue(5))), Lt(FieldName("a"), Constant(NumericValue(10))))),
            And(Seq(Ge(FieldName("a"), Constant(NumericValue(30))), Le(FieldName("a"), Constant(NumericValue(40))))),
            Or(Seq(Eq(FieldName("b"), Constant(NumericValue(10))), Eq(FieldName("b"), Constant(NumericValue(42)))))
          )
        )
    }
  }

  it should "support 'IN' in conditions" in {
    val statement = "SELECT foo FROM bar WHERE a > 10 OR b IN ( 'aaa', 'bbb' ) AND c = 8"

    parsed(statement) {
      case Select(Some(schema), SqlFieldList(fields), Some(condition), Nil, None, None) =>
        schema shouldEqual "bar"
        fields should contain theSameElementsAs List(SqlField(FieldName("foo")))
        condition shouldEqual Or(
          Seq(
            Gt(FieldName("a"), Constant(NumericValue(10))),
            And(
              Seq(
                In(FieldName("b"), Seq(StringValue("aaa"), StringValue("bbb"))),
                Eq(FieldName("c"), Constant(NumericValue(8)))
              )
            )
          )
        )
    }
  }

  it should "support 'NOT IN' in conditions" in {
    parsed("SELECT foo FROM bar WHERE x NOT IN (1,2,3) and z = 12") {
      case Select(Some(schema), SqlFieldList(fields), Some(condition), Nil, None, None) =>
        schema shouldEqual "bar"
        fields should contain theSameElementsAs List(SqlField(FieldName("foo")))
        condition shouldEqual And(
          Seq(
            NotIn(FieldName("x"), Seq(NumericValue(1), NumericValue(2), NumericValue(3))),
            Eq(FieldName("z"), Constant(NumericValue(12)))
          )
        )
    }
  }

  it should "support 'IS NULL' condition" in {
    val statement = "SELECT foo FROM bar WHERE a > 10 AND c IS NULL"

    parsed(statement) {
      case Select(Some(schema), SqlFieldList(fields), Some(condition), Nil, None, None) =>
        schema shouldEqual "bar"
        fields should contain theSameElementsAs List(SqlField(FieldName("foo")))
        condition shouldEqual And(
          Seq(
            Gt(FieldName("a"), Constant(NumericValue(10))),
            IsNull(FieldName("c"))
          )
        )
    }
  }

  it should "support 'IS NOT NULL' condition" in {
    val statement = "SELECT foo FROM bar WHERE a > 10 AND c IS NOT NULL"

    parsed(statement) {
      case Select(Some(schema), SqlFieldList(fields), Some(condition), Nil, None, None) =>
        schema shouldEqual "bar"
        fields should contain theSameElementsAs List(SqlField(FieldName("foo")))
        condition shouldEqual And(
          Seq(
            Gt(FieldName("a"), Constant(NumericValue(10))),
            IsNotNull(FieldName("c"))
          )
        )
    }
  }

  it should "support 'IS NOT NULL' withing 'CASE' expression" in {
    val statement = "SELECT sum(CASE WHEN foo IS NOT NULL THEN quantity ELSE 0) as quantity FROM bar"

    parsed(statement) {
      case Select(Some(schema), SqlFieldList(fields), None, Nil, None, None) =>
        fields should have size 1
        val f = fields.head
        f match {
          case SqlField(FunctionCall("sum", Case(conditionalValues, default) :: Nil), Some("quantity")) =>
            conditionalValues should have size 1
            val (c, v) = conditionalValues.head
            c shouldBe IsNotNull(FieldName("foo"))
            v shouldBe FieldName("quantity")
            default shouldBe Constant(NumericValue(BigDecimal(0)))

          case _ => fail(s"Unexpected field $f")
        }
    }
  }

  it should "support between conditions" in {
    val statement =
      """SELECT sum(quantity) FROM items WHERE time BETWEEN TIMESTAMP '2021-03-09' AND TIMESTAMP '2021-03-10' AND
         | sum BETWEEN 1000 AND 10000""".stripMargin

    parsed(statement) {
      case Select(Some(schema), SqlFieldList(fields), Some(condition), Nil, None, None) =>
        schema shouldEqual "items"
        fields should contain theSameElementsAs List(SqlField(FunctionCall("sum", FieldName("quantity") :: Nil)))
        condition shouldEqual And(
          Seq(
            BetweenCondition(
              FieldName("time"),
              TimestampValue(OffsetDateTime.of(2021, 3, 9, 0, 0, 0, 0, ZoneOffset.UTC)),
              TimestampValue(OffsetDateTime.of(2021, 3, 10, 0, 0, 0, 0, ZoneOffset.UTC))
            ),
            BetweenCondition(
              FieldName("sum"),
              NumericValue(1000),
              NumericValue(10000)
            )
          )
        )
    }
  }

  it should "handle boolean operations priority" in {
    parsed("""SELECT x, y
        |  FROM table
        |  WHERE x > 100 = y + 50 <= 1000
        |""".stripMargin) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), Nil, None, None) =>
        table shouldEqual "table"
        fields should contain theSameElementsInOrderAs List(SqlField(FieldName("x")), SqlField(FieldName("y")))
        condition shouldEqual Eq(
          Gt(FieldName("x"), Constant(NumericValue(100))),
          Le(Plus(FieldName("y"), Constant(NumericValue(50))), Constant(NumericValue(1000)))
        )
    }
  }

  it should "support boolean operations as a fields" in {
    parsed("""SELECT quantity > 100 as more_100, quantity / 2 < 1000 as less_1000
        |  FROM items
        |""".stripMargin) {
      case Select(Some(table), SqlFieldList(fields), None, Nil, None, None) =>
        table shouldEqual "items"
        fields should contain theSameElementsInOrderAs List(
          SqlField(Gt(FieldName("quantity"), Constant(NumericValue(100))), Some("more_100")),
          SqlField(
            Lt(Divide(FieldName("quantity"), Constant(NumericValue(2))), Constant(NumericValue(1000))),
            Some("less_1000")
          )
        )

    }
  }

  it should "parse SQL statements with grouping" in {
    val statement = "SELECT SUM(quantity), day(time) FROM items WHERE item = 'биг мак' GROUP BY day(time);"

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), groupings, None, None) =>
        table shouldEqual "items"
        condition shouldEqual Eq(FieldName("item"), Constant(StringValue("биг мак")))
        fields should contain theSameElementsAs List(
          SqlField(FunctionCall("sum", FieldName("quantity") :: Nil)),
          SqlField(FunctionCall("day", FieldName("time") :: Nil))
        )
        groupings should contain(FunctionCall("day", FieldName("time") :: Nil))
    }
  }

  it should "parse SQL statements with 'AS' aliases" in {
    val statement = "SELECT SUM(quantity), day(time) as d FROM items WHERE (quantity < 2.5) GROUP BY d"
    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), groupings, None, None) =>
        table shouldEqual "items"
        condition shouldEqual Lt(FieldName("quantity"), Constant(NumericValue(2.5)))
        fields should contain theSameElementsAs List(
          SqlField(FunctionCall("sum", FieldName("quantity") :: Nil)),
          SqlField(FunctionCall("day", FieldName("time") :: Nil), Some("d"))
        )
        groupings should contain(FieldName("d"))
    }
  }

  it should "require space between expression and alias" in {
    errorMessage("SELECT 2x2") {
      case msg =>
        msg should include(
          """Expect ("." | "*" | "/" | "+" | "-" | ">=" | ">" | "<=" | "<" | "IN" | "NOT" | "IS" | "BETWEEN" | "=" | "<>" | "!=" | "AND" | "OR" | [ \t\n] | "," | "FROM" | "WHERE" | "GROUP" | "HAVING" | "LIMIT" | ";" | end-of-input), but got "x2""""
        )
    }
  }

  it should "parse SQL statements with aliases" in {
    val statement = "SELECT SUM(quantity), day(time) d FROM tickets WHERE sum <= 1000 GROUP BY d "
    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), groupings, None, None) =>
        table shouldEqual "tickets"
        condition shouldEqual Le(FieldName("sum"), Constant(NumericValue(1000)))
        fields should contain theSameElementsAs List(
          SqlField(FunctionCall("sum", FieldName("quantity") :: Nil)),
          SqlField(FunctionCall("day", FieldName("time") :: Nil), Some("d"))
        )
        groupings should contain(FieldName("d"))
    }
  }

  it should "parse field aliases" in {
    val statement = "select quantity as q, sum s from items where q >= 10"

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), groupings, None, None) =>
        table shouldEqual "items"
        fields should contain theSameElementsAs List(
          SqlField(FieldName("quantity"), Some("q")),
          SqlField(FieldName("sum"), Some("s"))
        )
        condition shouldEqual Ge(FieldName("q"), Constant(NumericValue(10)))
        groupings shouldBe empty
    }
  }

  it should "support parentheses around fields" in {
    val statement = "select (a), (b) as c, d, (f(g)), (h(j)) as k from foo"

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), None, Nil, None, None) =>
        table shouldEqual "foo"
        fields should contain theSameElementsInOrderAs List(
          SqlField(FieldName("a")),
          SqlField(FieldName("b"), Some("c")),
          SqlField(FieldName("d")),
          SqlField(FunctionCall("f", FieldName("g") :: Nil)),
          SqlField(FunctionCall("h", FieldName("j") :: Nil), Some("k"))
        )
    }
  }

  it should "find field aliases in groupings" in {
    val statement = "SELECT SUM(quantity) sum, name n FROM tickets WHERE n <> 'картошка' GROUP BY n"

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), groupings, None, None) =>
        table shouldEqual "tickets"
        fields should contain theSameElementsAs List(
          SqlField(FunctionCall("sum", FieldName("quantity") :: Nil), Some("sum")),
          SqlField(FieldName("name"), Some("n"))
        )
        condition shouldEqual Ne(FieldName("n"), Constant(StringValue("картошка")))
        groupings should contain(FieldName("n"))
    }
  }

  it should "parse function calls in conditions" in {
    val statement = "SELECT foo FROM bar WHERE day(time) = 28"

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), groupings, None, None) =>
        table shouldEqual "bar"
        fields should contain theSameElementsAs List(SqlField(FieldName("foo")))
        condition shouldEqual Eq(FunctionCall("day", FieldName("time") :: Nil), Constant(NumericValue(28)))
        groupings shouldBe empty
    }
  }

  it should "parse multiline queries" in {
    val statement =
      """SELECT field FROM foo
        |  WHERE bar <= 5
        |  GROUP BY baz
      """.stripMargin

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), groupings, None, None) =>
        table shouldEqual "foo"
        fields should contain(SqlField(FieldName("field")))
        condition shouldEqual Le(FieldName("bar"), Constant(NumericValue(5)))
        groupings should contain(FieldName("baz"))
    }
  }

  it should "handle placeholders" in {
    val statement =
      """SELECT field FROM foo
        | WHERE bar >= ? AND baz < ?
      """.stripMargin

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), groupings, None, None) =>
        table shouldEqual "foo"
        fields should contain(SqlField(FieldName("field")))
        condition shouldEqual And(
          Seq(
            Ge(FieldName("bar"), Constant(Placeholder(1))),
            Lt(FieldName("baz"), Constant(Placeholder(2)))
          )
        )
        groupings shouldBe empty
    }
  }

  it should "allow put field names in double quotes" in {
    val statement = """SELECT "field" as "f" from "table" WHERE "f" = 'hello'"""

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), groupings, None, None) =>
        table shouldEqual "table"
        fields should contain theSameElementsAs List(SqlField(FieldName("field"), Some("f")))
        condition shouldEqual Eq(FieldName("f"), Constant(StringValue("hello")))
        groupings shouldBe empty
    }
  }

  it should "support table names for fields" in {
    val statement = """SELECT "table"."field" as "field" FROM "table""""

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), None, groupings, None, None) =>
        table shouldEqual "table"
        fields should contain theSameElementsAs List(SqlField(FieldName("field"), Some("field")))
        groupings shouldBe empty
    }
  }

  it should "support limits" in {
    val statement = """SELECT foo FROM bar WHERE qux = 5 LIMIT 10"""

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), groupings, None, Some(limit)) =>
        table shouldEqual "bar"
        fields should contain theSameElementsAs Seq(SqlField(FieldName("foo")))
        condition shouldEqual Eq(FieldName("qux"), Constant(NumericValue(5)))
        groupings shouldBe empty
        limit shouldEqual 10
    }
  }

  it should "handle both groupings and limits" in {
    val statement = """SELECT foo FROM bar WHERE qux = 5 GROUP BY time, q LIMIT 10"""

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), groupings, None, Some(limit)) =>
        table shouldEqual "bar"
        fields should contain theSameElementsAs Seq(SqlField(FieldName("foo")))
        condition shouldEqual Eq(FieldName("qux"), Constant(NumericValue(5)))
        groupings should contain theSameElementsAs Seq(FieldName("time"), FieldName("q"))
        limit shouldEqual 10
    }
  }

  it should "support table name in function calls" in {
    val statement = """SELECT day("receipt"."time") AS "time" , "receipt"."cashSum" AS "cashSum"
                      |  FROM "receipt"
                      |  WHERE "receipt"."time" >= TIMESTAMP '2017-10-01' AND "receipt"."time" < TIMESTAMP '2017-10-30'
                      |    GROUP BY "time"""".stripMargin

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), groupings, None, None) =>
        table shouldEqual "receipt"
        fields should contain theSameElementsInOrderAs Seq(
          SqlField(FunctionCall("day", FieldName("time") :: Nil), Some("time")),
          SqlField(FieldName("cashSum"), Some("cashSum"))
        )

        condition shouldEqual And(
          Seq(
            Ge(FieldName("time"), Constant(TimestampValue(OffsetDateTime.of(2017, 10, 1, 0, 0, 0, 0, ZoneOffset.UTC)))),
            Lt(FieldName("time"), Constant(TimestampValue(OffsetDateTime.of(2017, 10, 30, 0, 0, 0, 0, ZoneOffset.UTC))))
          )
        )

        groupings should contain theSameElementsAs Seq(FieldName("time"))
    }
  }

  it should "support dummy nested select" in {
    val statement =
      """SELECT * FROM (
        |  SELECT foo, barbarian as bar FROM qux WHERE mode != 'test'
        |)
      """.stripMargin

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), Nil, None, None) =>
        table shouldEqual "qux"
        fields should contain theSameElementsInOrderAs List(
          SqlField(FieldName("foo")),
          SqlField(FieldName("barbarian"), Some("bar"))
        )
        condition shouldEqual Ne(FieldName("mode"), Constant(StringValue("test")))
    }
  }

  it should "support nested select with alias" in {
    val statement =
      """
        | SELECT * FROM (
        |   SELECT day(time) as d, sum(quantity) as quantity, sum(sum) as sum
        |     FROM kkm_items
        |     WHERE time >= TIMESTAMP '2017-11-01' AND time < TIMESTAMP '2017-12-20' AND KkmsRetailPlaceOrgCatalog_orgInn = '7706091500'
        |     group by d
        | ) "Custom_SQL_Query"
      """.stripMargin

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), groupings, None, None) =>
        table shouldEqual "kkm_items"
        fields should contain theSameElementsInOrderAs List(
          SqlField(FunctionCall("day", FieldName("time") :: Nil), Some("d")),
          SqlField(FunctionCall("sum", FieldName("quantity") :: Nil), Some("quantity")),
          SqlField(FunctionCall("sum", FieldName("sum") :: Nil), Some("sum"))
        )
        condition shouldEqual And(
          Seq(
            Ge(FieldName("time"), Constant(TimestampValue(OffsetDateTime.of(2017, 11, 1, 0, 0, 0, 0, ZoneOffset.UTC)))),
            Lt(
              FieldName("time"),
              Constant(TimestampValue(OffsetDateTime.of(2017, 12, 20, 0, 0, 0, 0, ZoneOffset.UTC)))
            ),
            Eq(FieldName("KkmsRetailPlaceOrgCatalog_orgInn"), Constant(StringValue("7706091500")))
          )
        )
        groupings should contain theSameElementsAs List(FieldName("d"))
    }
  }

  it should "support nested select with specified field names" in {
    val statement =
      """
        | SELECT "Custom_SQL_Query"."d" AS "d",
        |  "Custom_SQL_Query"."quantity" AS "amount"
        | FROM (
        |   SELECT day(time) as d, sum(quantity) as quantity, sum(sum) as sum
        |   FROM kkm_items
        |   WHERE time >= TIMESTAMP '2017-11-01' AND time < TIMESTAMP '2017-12-20' AND KkmsRetailPlaceOrgCatalog_orgInn = '7706091500'
        |   group by d
        | ) "Custom_SQL_Query";
      """.stripMargin

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), groupings, None, None) =>
        table shouldEqual "kkm_items"
        fields should contain theSameElementsInOrderAs List(
          SqlField(FunctionCall("day", FieldName("time") :: Nil), Some("d")),
          SqlField(FunctionCall("sum", FieldName("quantity") :: Nil), Some("amount"))
        )
        condition shouldEqual And(
          Seq(
            Ge(FieldName("time"), Constant(TimestampValue(OffsetDateTime.of(2017, 11, 1, 0, 0, 0, 0, ZoneOffset.UTC)))),
            Lt(
              FieldName("time"),
              Constant(TimestampValue(OffsetDateTime.of(2017, 12, 20, 0, 0, 0, 0, ZoneOffset.UTC)))
            ),
            Eq(FieldName("KkmsRetailPlaceOrgCatalog_orgInn"), Constant(StringValue("7706091500")))
          )
        )
        groupings should contain theSameElementsAs List(FieldName("d"))
    }
  }

  it should "support constants in queries" in {
    val statement =
      """
        |SELECT 1 AS "Number_of_Records",
        |  "Custom_SQL_Query"."d" AS "d",
        |  "Custom_SQL_Query"."quantity" AS "quantity",
        |  "Custom_SQL_Query"."sum" AS "sum"
        |FROM (
        |  SELECT day(time) as d, sum(quantity) as quantity, sum(sum) as sum
        |  FROM kkm_items
        |  WHERE time >= TIMESTAMP '2017-11-01' AND time < TIMESTAMP '2017-11-20' AND KkmsRetailPlaceOrgCatalog_orgInn = '7706091500'
        |  group by d
        |) "Custom_SQL_Query"
      """.stripMargin

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), groupings, None, None) =>
        table shouldEqual "kkm_items"
        fields should contain theSameElementsInOrderAs List(
          SqlField(Constant(NumericValue(1)), Some("Number_of_Records")),
          SqlField(FunctionCall("day", FieldName("time") :: Nil), Some("d")),
          SqlField(FunctionCall("sum", FieldName("quantity") :: Nil), Some("quantity")),
          SqlField(FunctionCall("sum", FieldName("sum") :: Nil), Some("sum"))
        )
        condition shouldEqual And(
          Seq(
            Ge(FieldName("time"), Constant(TimestampValue(OffsetDateTime.of(2017, 11, 1, 0, 0, 0, 0, ZoneOffset.UTC)))),
            Lt(
              FieldName("time"),
              Constant(TimestampValue(OffsetDateTime.of(2017, 11, 20, 0, 0, 0, 0, ZoneOffset.UTC)))
            ),
            Eq(FieldName("KkmsRetailPlaceOrgCatalog_orgInn"), Constant(StringValue("7706091500")))
          )
        )
        groupings should contain theSameElementsAs List(FieldName("d"))
    }
  }

  it should "support expressions in outer queries" in {
    parsed("""
        |SELECT foo + bar as baz, qux
        |  FROM (SELECT 1 - sum / quantity as foo, test FROM table WHERE x > 100)
      """.stripMargin) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), Nil, None, None) =>
        table shouldEqual "table"
        fields should contain theSameElementsInOrderAs List(
          SqlField(
            Plus(
              Minus(Constant(NumericValue(1)), Divide(FieldName("sum"), FieldName("quantity"))),
              FieldName("bar")
            ),
            Some("baz")
          ),
          SqlField(FieldName("qux"))
        )

        condition shouldEqual Gt(FieldName("x"), Constant(NumericValue(100)))
    }
  }

  it should "support case when construction" in {
    val statement =
      """
        | SELECT (CASE
        |   WHEN x < 10 THEN 0
        |   WHEN x >= 10 and x < 100 THEN 1
        |   ELSE 2) AS logx
        | FROM foo
        |   WHERE y = 5
      """.stripMargin

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), Nil, None, None) =>
        table shouldEqual "foo"
        fields should contain theSameElementsAs List(
          SqlField(
            Case(
              Seq(
                (Lt(FieldName("x"), Constant(NumericValue(10))), Constant(NumericValue(0))),
                (
                  And(
                    Seq(Ge(FieldName("x"), Constant(NumericValue(10))), Lt(FieldName("x"), Constant(NumericValue(100))))
                  ),
                  Constant(NumericValue(1))
                )
              ),
              Constant(NumericValue(2))
            ),
            Some("logx")
          )
        )
        condition shouldEqual Eq(FieldName("y"), Constant(NumericValue(5)))
    }
  }

  it should "parse case when inside function calls" in {
    val statement =
      """
        | SELECT item, SUM(CASE
        |   WHEN x < 10 THEN 0
        |   ELSE 1) AS more_than_ten
        | FROM foo
        | WHERE more_than_ten >= 5
        | GROUP BY item
      """.stripMargin

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), groupings, None, None) =>
        table shouldEqual "foo"

        fields should contain theSameElementsInOrderAs List(
          SqlField(FieldName("item")),
          SqlField(
            FunctionCall(
              "sum",
              Case(
                Seq((Lt(FieldName("x"), Constant(NumericValue(10))), Constant(NumericValue(0)))),
                Constant(NumericValue(1))
              ) :: Nil
            ),
            Some("more_than_ten")
          )
        )

        condition shouldEqual Ge(FieldName("more_than_ten"), Constant(NumericValue(5)))

        groupings should contain theSameElementsAs List(FieldName("item"))
    }
  }

  it should "support fields inside case when results" in {
    val statement =
      """
        | SELECT item, SUM(CASE WHEN x = 'type_one' THEN y ELSE 0) AS sum_type_one
        | FROM foo
        | WHERE more_than_ten >= 5
        | GROUP BY item
      """.stripMargin

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), groupings, None, None) =>
        table shouldEqual "foo"

        fields should contain theSameElementsInOrderAs List(
          SqlField(FieldName("item")),
          SqlField(
            FunctionCall(
              "sum",
              Case(
                Seq((Eq(FieldName("x"), Constant(StringValue("type_one"))), FieldName("y"))),
                Constant(NumericValue(0))
              ) :: Nil
            ),
            Some("sum_type_one")
          )
        )

        condition shouldEqual Ge(FieldName("more_than_ten"), Constant(NumericValue(5)))

        groupings should contain theSameElementsAs List(FieldName("item"))
    }
  }

  it should "parse window function calls" in {
    val statement = "SELECT lag(time) FROM bar WHERE day(time) = 28"

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), groupings, None, None) =>
        table shouldEqual "bar"
        fields should contain theSameElementsAs List(SqlField(FunctionCall("lag", FieldName("time") :: Nil)))
        condition shouldEqual Eq(FunctionCall("day", FieldName("time") :: Nil), Constant(NumericValue(28)))
        groupings shouldBe empty
    }
  }

  it should "support having" in {
    val statement =
      """
        |SELECT
        |   kkmId,
        |   time AS t,
        |   lag(time) AS l,
        |   hour(time) as h
        | FROM receipt
        | WHERE time < TIMESTAMP '2018-1-11' AND time > TIMESTAMP '2018-1-1'
        | GROUP BY kkmId
        | HAVING
        |   ((l - t) > '2:00:00' AND h >= 8 AND h <= 18) OR
        |   ((l - t) > '4:00:00' AND (h > 18 OR h < 8))
        | LIMIT 10
      """.stripMargin

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), groupings, Some(having), Some(limit)) =>
        table shouldEqual "receipt"
        fields should contain theSameElementsInOrderAs List(
          SqlField(FieldName("kkmId")),
          SqlField(FieldName("time"), Some("t")),
          SqlField(FunctionCall("lag", FieldName("time") :: Nil), Some("l")),
          SqlField(FunctionCall("hour", FieldName("time") :: Nil), Some("h"))
        )
        condition shouldEqual And(
          Seq(
            Lt(FieldName("time"), Constant(TimestampValue(OffsetDateTime.of(2018, 1, 11, 0, 0, 0, 0, ZoneOffset.UTC)))),
            Gt(FieldName("time"), Constant(TimestampValue(OffsetDateTime.of(2018, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC))))
          )
        )
        groupings should contain theSameElementsAs List(FieldName("kkmId"))
        having shouldEqual Or(
          Seq(
            And(
              Seq(
                Gt(Minus(FieldName("l"), FieldName("t")), Constant(StringValue("2:00:00"))),
                Ge(FieldName("h"), Constant(NumericValue(8))),
                Le(FieldName("h"), Constant(NumericValue(18)))
              )
            ),
            And(
              Seq(
                Gt(Minus(FieldName("l"), FieldName("t")), Constant(StringValue("4:00:00"))),
                Or(
                  Seq(
                    Gt(FieldName("h"), Constant(NumericValue(18))),
                    Lt(FieldName("h"), Constant(NumericValue(8)))
                  )
                )
              )
            )
          )
        )
        limit shouldEqual 10
    }
  }

  it should "support now call" in {
    val statement =
      """
        | SELECT kkmId, day(time) d, sum(quantity)
        | FROM itemsKkm
        | WHERE time < now() AND time >= now() - INTERVAL '1' month AND item = 'конфета \'Чупа-чупс\''
        | GROUP BY kkmId, d
        |""".stripMargin

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), groupings, None, None) =>
        table shouldEqual "itemsKkm"
        fields should contain theSameElementsInOrderAs Seq(
          SqlField(FieldName("kkmId")),
          SqlField(FunctionCall("day", FieldName("time") :: Nil), Some("d")),
          SqlField(FunctionCall("sum", FieldName("quantity") :: Nil))
        )

        condition shouldEqual And(
          Seq(
            Lt(FieldName("time"), FunctionCall("now", Nil)),
            Ge(
              FieldName("time"),
              Minus(FunctionCall("now", Nil), Constant(PeriodValue(PeriodDuration.of(Period.ofMonths(1)))))
            ),
            Eq(FieldName("item"), Constant(StringValue("конфета 'Чупа-чупс'")))
          )
        )

        groupings should contain theSameElementsInOrderAs Seq(
          FieldName("kkmId"),
          FieldName("d")
        )
    }
  }

  it should "support plus" in {
    val statement =
      """
        | SELECT kkmId, (cardSum + cashSum) as total
        | FROM itemsKkm
        | WHERE time < now() AND time >= now() - INTERVAL '1' month
        | GROUP BY kkmId
        |""".stripMargin

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), groupings, None, None) =>
        fields should contain theSameElementsInOrderAs Seq(
          SqlField(FieldName("kkmId")),
          SqlField(Plus(FieldName("cardSum"), FieldName("cashSum")), Some("total"))
        )
    }
  }

  it should "support arithmetic on aggregations" in {
    val statement =
      """
        | SELECT day, sum(cashSum) + min(cardSum) as wtf
        | FROM itemsKkm
        | WHERE time < now() AND time >= now() - INTERVAL '1' month
        | GROUP BY day
        |""".stripMargin

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), groupings, None, None) =>
        fields should contain theSameElementsAs Seq(
          SqlField(FieldName("day")),
          SqlField(
            Plus(FunctionCall("sum", FieldName("cashSum") :: Nil), FunctionCall("min", FieldName("cardSum") :: Nil)),
            Some("wtf")
          )
        )
    }
  }

  it should "support quite complex arithmetical expressions in" in {
    val statement =
      """
        | SELECT kkmId, ((cardSum + cashSum) * 5 - whatever / 1.3) / 52 as wtf
        | FROM itemsKkm
        | WHERE time < now() AND time >= now() - INTERVAL '1' month
        | GROUP BY kkmId
        |""".stripMargin

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), groupings, None, None) =>
        fields should contain theSameElementsInOrderAs Seq(
          SqlField(FieldName("kkmId")),
          SqlField(
            Divide(
              Minus(
                Multiply(
                  Plus(FieldName("cardSum"), FieldName("cashSum")),
                  Constant(NumericValue(5))
                ),
                Divide(FieldName("whatever"), Constant(NumericValue(1.3)))
              ),
              Constant(NumericValue(52))
            ),
            Some("wtf")
          )
        )
        fields(1).expr.proposedName.get shouldBe "cardSum_plus_cashSum_mul_5_minus_whatever_div_1.3_div_52"
    }
  }

  it should "support array literals" in {
    val sql =
      """
        | SELECT a, b
        |   FROM table
        |   WHERE containsAll(x, {1,2,3})
        | """.stripMargin

    parsed(sql) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), Nil, None, None) =>
        table shouldEqual "table"
        fields should contain theSameElementsInOrderAs List(SqlField(FieldName("a")), SqlField(FieldName("b")))
        condition shouldEqual FunctionCall(
          "containsall",
          List(FieldName("x"), SqlArray(Seq(NumericValue(1), NumericValue(2), NumericValue(3))))
        )
    }
  }

  it should "parse SHOW TABLES statements" in {
    SqlParser.parse("SHOW TABLES") shouldBe Right(ShowTables)
  }

  it should "parse SHOW COLUMNS statements" in {
    SqlParser.parse("SHOW COLUMNS FROM some_table") shouldBe Right(ShowColumns("some_table"))
  }

  it should "parse SHOW FUNCTIONS statements" in {
    SqlParser.parse("SHOW FUNCTIONS FOR TIMESTAMP") shouldBe Right(ShowFunctions("TIMESTAMP"))
  }

  it should "parse SHOW QUERIES statements" in {
    SqlParser.parse("SHOW QUERIES") shouldBe Right(ShowQueryMetrics(None, None))
  }

  it should "parse SHOW QUERIES statements with limit" in {
    SqlParser.parse("SHOW QUERIES LIMIT 1") shouldBe Right(ShowQueryMetrics(None, Some(1)))
  }

  it should "parse SHOW QUERIES statements with query_id" in {
    SqlParser.parse("SHOW QUERIES WHERE QUERY_ID = '1'") shouldBe Right(
      ShowQueryMetrics(Some(MetricsFilter(queryId = Some("1"))), None)
    )
  }

  it should "parse SHOW QUERIES statements with state" in {
    SqlParser.parse("SHOW QUERIES WHERE STATE = 'RUNNING'") shouldBe Right(
      ShowQueryMetrics(Some(MetricsFilter(state = Some("RUNNING"))), None)
    )
  }

  it should "parse KILL QUERY statements with query_id" in {
    SqlParser.parse("KILL QUERY WHERE QUERY_ID = '1'") shouldBe Right(
      KillQuery(MetricsFilter(queryId = Some("1")))
    )
  }

  it should "parse DELETE QUERIES statements with query_id" in {
    SqlParser.parse("DELETE QUERIES WHERE QUERY_ID = '1'") shouldBe Right(
      DeleteQueryMetrics(MetricsFilter(queryId = Some("1")))
    )
  }

  it should "parse DELETE QUERIES statements with state" in {
    SqlParser.parse("DELETE QUERIES WHERE STATE = 'FINISHED'") shouldBe Right(
      DeleteQueryMetrics(MetricsFilter(state = Some("FINISHED")))
    )
  }

  it should "parse SHOW UPDATES_INTERVALS statements" in {
    val f = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val t = OffsetDateTime.now(ZoneOffset.UTC).withNano(0)
    SqlParser.parse(
      s"""SHOW UPDATES_INTERVALS
         |  WHERE table = 'receipt'
         |    AND updated_at BETWEEN TIMESTAMP '${t.format(f)}'
         |      AND TIMESTAMP '${t.format(f)}'""".stripMargin
    ) shouldBe Right(
      ShowUpdatesIntervals(
        Some(
          And(
            Seq(
              Eq(FieldName("table"), Constant(StringValue("receipt"))),
              BetweenCondition(FieldName("updated_at"), TimestampValue(t), TimestampValue(t))
            )
          )
        )
      )
    )
    SqlParser.parse(
      s"""SHOW UPDATES_INTERVALS""".stripMargin
    ) shouldBe Right(
      ShowUpdatesIntervals(None)
    )
    SqlParser.parse(
      s"""SHOW UPDATES_INTERVALS
         |  WHERE table = 'receipt'
         |    AND 'somebody' = updated_by
         |    AND updated_at BETWEEN TIMESTAMP '${t.format(f)}'
         |      AND TIMESTAMP '${t.format(f)}'""".stripMargin
    ) shouldBe Right(
      ShowUpdatesIntervals(
        Some(
          And(
            Seq(
              Eq(FieldName("table"), Constant(StringValue("receipt"))),
              Eq(Constant(StringValue("somebody")), FieldName("updated_by")),
              BetweenCondition(FieldName("updated_at"), TimestampValue(t), TimestampValue(t))
            )
          )
        )
      )
    )
  }

  it should "support functions as conditions" in {
    val statement =
      """
        |SELECT value from table_x WHERE isValid(value) and isTheSame(toArray(foo), toArray('bar'))
      """.stripMargin

    parsed(statement) {
      case Select(Some(table), SqlFieldList(fields), Some(condition), Nil, None, None) =>
        table shouldEqual "table_x"
        fields should contain theSameElementsAs Seq(SqlField(FieldName("value")))
        condition shouldEqual And(
          Seq(
            FunctionCall("isvalid", List(FieldName("value"))),
            FunctionCall(
              "isthesame",
              List(
                FunctionCall("toarray", List(FieldName("foo"))),
                FunctionCall("toarray", List(Constant(StringValue("bar"))))
              )
            )
          )
        )
    }
  }

  it should "support unary minus" in {
    parsed("""SELECT abs(sum(-quantity)) as abs1,
             |abs(sum(CASE WHEN sum < 40000 THEN -10 + (-5) ELSE 1)) as abs2,
             |abs(count(taxNo)) as abs3,
             |abs(sum(CASE WHEN operation_type = '0' THEN -sum WHEN operation_type = '2' THEN -5 + sum ELSE -10)) as abs4
             |FROM items_kkm
             |WHERE time >= TIMESTAMP '2019-04-10' AND time <= TIMESTAMP '2019-04-11'
             |AND -quantity < -100
             |""".stripMargin) {

      case Select(Some(table), SqlFieldList(fields), Some(condition), Nil, None, None) =>
        table shouldEqual "items_kkm"
        fields should contain theSameElementsInOrderAs Seq(
          SqlField(FunctionCall("abs", List(FunctionCall("sum", List(UMinus(FieldName("quantity")))))), Some("abs1")),
          SqlField(
            FunctionCall(
              "abs",
              List(
                FunctionCall(
                  "sum",
                  List(
                    Case(
                      Seq(
                        Lt(FieldName("sum"), Constant(NumericValue(40000))) -> Plus(
                          UMinus(Constant(NumericValue(10))),
                          UMinus(Constant(NumericValue(5)))
                        )
                      ),
                      Constant(NumericValue(1))
                    )
                  )
                )
              )
            ),
            Some("abs2")
          ),
          SqlField(FunctionCall("abs", List(FunctionCall("count", List(FieldName("taxNo"))))), Some("abs3")),
          SqlField(
            FunctionCall(
              "abs",
              List(
                FunctionCall(
                  "sum",
                  List(
                    Case(
                      Seq(
                        Eq(FieldName("operation_type"), Constant(StringValue("0"))) -> UMinus(FieldName("sum")),
                        Eq(FieldName("operation_type"), Constant(StringValue("2"))) -> Plus(
                          UMinus(Constant(NumericValue(5))),
                          FieldName("sum")
                        )
                      ),
                      UMinus(Constant(NumericValue(10)))
                    )
                  )
                )
              )
            ),
            Some("abs4")
          )
        )
        condition shouldEqual And(
          Seq(
            Ge(FieldName("time"), Constant(TimestampValue(OffsetDateTime.of(2019, 4, 10, 0, 0, 0, 0, ZoneOffset.UTC)))),
            Le(FieldName("time"), Constant(TimestampValue(OffsetDateTime.of(2019, 4, 11, 0, 0, 0, 0, ZoneOffset.UTC)))),
            Lt(UMinus(FieldName("quantity")), UMinus(Constant(NumericValue(100))))
          )
        )
    }
  }

  it should "support unary minus in IN" in {
    parsed("""SELECT field FROM table WHERE id IN (1, 2, -3)""") {
      case Select(Some(table), SqlFieldList(fields), Some(condition), Nil, None, None) =>
        table shouldEqual "table"
        fields should contain theSameElementsInOrderAs Seq(SqlField(FieldName("field")))
        condition shouldEqual In(FieldName("id"), Seq(NumericValue(1), NumericValue(2), NumericValue(-3)))
    }

  }

  it should "parse selects without table" in {
    parsed("""SELECT field, sum(sum) sum WHERE time > TIMESTAMP '2017-01-03'""") {
      case Select(None, SqlFieldList(fields), Some(condition), Nil, None, None) =>
        fields should contain theSameElementsInOrderAs Seq(
          SqlField(FieldName("field")),
          SqlField(FunctionCall("sum", FieldName("sum") :: Nil), Some("sum"))
        )
        condition shouldEqual Gt(
          FieldName("time"),
          Constant(TimestampValue(OffsetDateTime.of(2017, 1, 3, 0, 0, 0, 0, ZoneOffset.UTC)))
        )
    }
  }

  it should "handle UPSERT statements" in {
    parsed("""UPSERT INTO foo (bar, baz) VALUES ('bar value', 42);""") {
      case Upsert(s, fs, vs) =>
        s shouldEqual "foo"
        fs should contain theSameElementsInOrderAs List("bar", "baz")
        vs should contain theSameElementsInOrderAs List(
          List(Constant(StringValue("bar value")), Constant(NumericValue(42)))
        )
    }
  }

  it should "handle UPSERT with multiple values" in {
    parsed("""UPSERT INTO foo (bar, baz) VALUES ('abc', 1), ('def', 2);""") {
      case Upsert(s, fs, vs) =>
        s shouldEqual "foo"
        fs should contain theSameElementsInOrderAs List("bar", "baz")
        vs should contain theSameElementsInOrderAs List(
          List(Constant(StringValue("abc")), Constant(NumericValue(1))),
          List(Constant(StringValue("def")), Constant(NumericValue(2)))
        )
    }
  }

  it should "check that the number of values is the same as fields" in {
    errorMessage("""UPSERT INTO foo (bar, baz) VALUES ('abc', 1), ('fail', 4, 'me'), ('def', 2);""") {
      case msg =>
        msg should include("""Expect <2 expressions>, but got "('fail', 4"""")
    }
  }

  it should "produce error on unknown statements" in {
    errorMessage("INSERT 'foo' INTO table;") {
      case msg =>
        msg should include("""Expect ("SELECT" | "UPSERT" | "SHOW" | "KILL" | "DELETE"), but got "INSERT""")
    }
  }

  it should "produce error on unknown show" in {
    errorMessage("SHOW cartoons") {
      case msg =>
        msg should include(
          """Expect ("COLUMNS" | "TABLES" | "QUERIES" | "FUNCTIONS" | "UPDATES_INTERVALS"), but got "cartoons"""
        )
    }
  }

  it should "it should properly identify error position" in {
    errorMessage("""SELECT x + y z - 1 from y""") {
      case msg =>
        msg should include(
          """Expect ("," | "FROM" | "WHERE" | "GROUP" | "HAVING" | "LIMIT" | ";" | end-of-input), but got "- 1"""
        )
    }
  }

  def parsed[U](statement: String)(pf: PartialFunction[Statement, U])(implicit pos: source.Position): U = {
    inside(SqlParser.parse(statement)) {
      case Right(x)  => pf(x)
      case Left(msg) => fail(msg)
    }
  }

  def errorMessage[U](statement: String)(pf: PartialFunction[String, U]): U = {
    inside(SqlParser.parse(statement)) {
      case Left(msg) => pf(msg)
      case Right(s)  => fail(s"Error message expected, byt got statement: $s")
    }
  }
}

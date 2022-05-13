package org.yupana.api.query

import org.joda.time.{ DateTimeZone, LocalDateTime }
import org.yupana.api.Time
import org.yupana.api.schema.{ DictionaryDimension, HashDimension, Metric, RawDimension, Table }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QueryTest extends AnyFlatSpec with Matchers {

  import syntax.All._

  val DIM_A = RawDimension[Int]("A")
  val DIM_B = DictionaryDimension("B")
  val DIM_C = HashDimension[String, Int]("C", _.hashCode)
  val FIELD = Metric[Double]("F", 1)

  val testTable = new Table(
    id = 1,
    name = "test_table",
    rowTimeSpan = 24 * 60 * 60 * 1000,
    dimensionSeq = Seq(DIM_A, DIM_B, DIM_C),
    metrics = Seq(FIELD),
    externalLinks = Seq.empty,
    new LocalDateTime(2020, 4, 6, 15, 16).toDateTime(DateTimeZone.UTC).getMillis
  )

  "Query" should "have human readable toString implementation" in {
    val query = Query(
      Some(testTable),
      Seq(
        dimension(DIM_A) as "DIM_A",
        dimension(DIM_B).toField,
        dimension(DIM_C) as "CCC",
        sum(metric(FIELD)) as "SUM_FIELD",
        condition(
          ge(double2bigDecimal(metric(FIELD)), const(BigDecimal(100))),
          const(1),
          const(0)
        ) as "IS_BIG"
      ),
      Some(
        and(
          gt(time, const(Time(12345))),
          lt(time, const(Time(23456))),
          in(dimension(DIM_A), Set(1, 2, 3, 5, 8))
        )
      ),
      Seq(dimension(DIM_A), dimension(DIM_B)),
      Some(100),
      Some(gt(sum(metric(FIELD)), const(1000d)))
    )

    query.toString shouldEqual
      s"""Query(
        |  query_id: ${query.id}
        |  FIELDS:
        |    dim(A) as DIM_A
        |    dim(B) as B
        |    dim(C) as CCC
        |    sum(metric(F)) as SUM_FIELD
        |    IF (double2decimal(metric(F)) >= const(100:BigDecimal)) THEN const(1:Integer) ELSE const(0:Integer) as IS_BIG
        |  FROM: test_table
        |  FILTER:
        |    (time() > const(1970-01-01T00:00:12.345Z:Time) AND time() < const(1970-01-01T00:00:23.456Z:Time) AND dim(A) IN (5, 1, 2, 3, 8))
        |  GROUP BY: dim(A), dim(B)
        |  LIMIT: 100
        |  POSTFILTER:
        |    sum(metric(F)) > const(1000.0:Double)
        |)""".stripMargin
  }

  it should "support simple queries in toString" in {
    val query = Query(
      None,
      Seq(plus(const(2), const(3)) as "five"),
      None
    )

    query.toString shouldEqual
      s"""Query(
         |  query_id: ${query.id}
         |  FIELDS:
         |    const(2:Integer) + const(3:Integer) as five
         |)""".stripMargin
  }
}

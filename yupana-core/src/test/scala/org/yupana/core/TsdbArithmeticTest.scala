package org.yupana.core

import java.util.Properties

import org.joda.time.format.DateTimeFormat
import org.joda.time.{ DateTime, DateTimeZone, LocalDateTime }
import org.scalatest._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.yupana.api.Time
import org.yupana.api.query.LinkExpr
import org.yupana.api.schema.LinkField
import org.yupana.core.cache.CacheFactory
import org.yupana.core.model.InternalQuery
import org.yupana.core.utils.SparseTable

class TsdbArithmeticTest
    extends FlatSpec
    with Matchers
    with TsdbMocks
    with OptionValues
    with TableDrivenPropertyChecks
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  import org.yupana.api.query.syntax.All._

  private val format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

  override protected def beforeAll(): Unit = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("app.properties"))
    CacheFactory.init(properties, "test")
  }

  override def beforeEach(): Unit = {
    CacheFactory.flushCaches()
  }

  val from: DateTime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
  val to: DateTime = from.plusDays(1)

  private def timeBounds(from: DateTime = from, to: DateTime = to, and: Boolean = true): String = {
    s" ${if (and) "AND" else "WHERE"} time >= TIMESTAMP '${from.toString(format)}' AND time < TIMESTAMP '${to.toString(format)}' "
  }

  "TSDB" should "execute query with arithmetic (no aggregations)" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT testField + testField2 as some_sum FROM test_table WHERE A = 'taga'" + timeBounds()
    val query = createQuery(sql)

    val pointTime = from.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2), time),
          and(
            equ(lower(dimension(TestDims.DIM_A)), const("taga")),
            ge(time, const(Time(from))),
            lt(time, const(Time(to)))
          )
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime))
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .set(metric(TestTableFields.TEST_FIELD2), 2d)
            .buildAndReset(),
          b.set(time, Time(pointTime))
            .set(metric(TestTableFields.TEST_FIELD), 3d)
            .set(metric(TestTableFields.TEST_FIELD2), 4d)
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query).iterator

    val r1 = rows.next()
    r1.get[Double]("some_sum") shouldBe 3d

    val r2 = rows.next()
    r2.get[Double]("some_sum") shouldBe 7d

    rows.hasNext shouldBe false
  }

  it should "execute query with arithmetic on aggregated values" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT sum(testField) as stf," +
      " sum(testField) * max(testField2) / 2 as mult FROM test_table WHERE A = 'taga'" +
      timeBounds() + " GROUP BY A"
    val query = createQuery(sql)

    val pointTime = from.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2), dimension(TestDims.DIM_A), time),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            equ(lower(dimension(TestDims.DIM_A)), const("taga"))
          )
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime))
            .set(dimension(TestDims.DIM_A), "taga")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .set(metric(TestTableFields.TEST_FIELD2), 2d)
            .buildAndReset(),
          b.set(time, Time(pointTime))
            .set(dimension(TestDims.DIM_A), "taga")
            .set(metric(TestTableFields.TEST_FIELD), 3d)
            .set(metric(TestTableFields.TEST_FIELD2), 4d)
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query).iterator

    val r1 = rows.next()
    r1.get[Double]("stf") shouldBe 4d
    r1.get[Double]("mult") shouldBe 8d

    rows.hasNext shouldBe false
  }

  it should "execute query with arithmetic inside CASE" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mock[ExternalLinkService[TestLinks.TestLink]]
    tsdb.registerExternalLink(TestLinks.TEST_LINK, testCatalogServiceMock)

    val sql = "SELECT TestLink_testField as address, A, sum(testField) as sum_receiptCount, " +
      "sum(CASE WHEN testStringField = '2' THEN testField2 " +
      "WHEN testStringField = '3' THEN (0 - testField2) " +
      "ELSE 0) as totalSum " +
      "FROM test_table " +
      "WHERE A in ('0000270761025003') " + timeBounds() + " GROUP BY A, address"
    val query = createQuery(sql)

    (testCatalogServiceMock.setLinkedValues _)
      .expects(*, *, Set(link(TestLinks.TEST_LINK, "testField")).asInstanceOf[Set[LinkExpr[_]]])
      .onCall((qc, datas, _) =>
        setCatalogValueByTag(
          qc,
          datas,
          TestLinks.TEST_LINK,
          SparseTable("0000270761025003" -> Map("testField" -> "some-val"))
        )
      )

    val pointTime = from.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(
            metric(TestTableFields.TEST_FIELD),
            metric(TestTableFields.TEST_FIELD2),
            metric(TestTableFields.TEST_STRING_FIELD),
            dimension(TestDims.DIM_A),
            time
          ),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(lower(dimension(TestDims.DIM_A)), Set("0000270761025003"))
          )
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime))
            .set(dimension(TestDims.DIM_A), "0000270761025003")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .set(metric(TestTableFields.TEST_FIELD2), 2d)
            .set(metric(TestTableFields.TEST_STRING_FIELD), "3")
            .buildAndReset(),
          b.set(time, Time(pointTime))
            .set(dimension(TestDims.DIM_A), "0000270761025003")
            .set(metric(TestTableFields.TEST_FIELD), 3d)
            .set(metric(TestTableFields.TEST_FIELD2), 4d)
            .set(metric(TestTableFields.TEST_STRING_FIELD), "3")
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query).iterator

    val r1 = rows.next()
    r1.get[Double]("totalSum") shouldBe -6d

    rows.hasNext shouldBe false
  }

  it should "execute query like this (do not calculate arithmetic on aggregated fields when evaluating each data row)" in withTsdbMock {
    (tsdb, tsdbDaoMock) =>
      val sql = "SELECT (count(testField) + count(testField2)) as plus4, " +
        "(distinct_count(testField) + distinct_count(testField2)) as plus5 " +
        "FROM test_table " + timeBounds(and = false) + " GROUP BY day(time)"
      val query = createQuery(sql)

      val pointTime = from.getMillis + 10

      (tsdbDaoMock.query _)
        .expects(
          InternalQuery(
            TestSchema.testTable,
            Set(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2), time),
            and(ge(time, const(Time(from))), lt(time, const(Time(to))))
          ),
          *,
          *
        )
        .onCall((_, b, _) =>
          Iterator(
            b.set(time, Time(pointTime))
              .set(metric(TestTableFields.TEST_FIELD), 1d)
              .set(metric(TestTableFields.TEST_FIELD2), 2d)
              .buildAndReset(),
            b.set(time, Time(pointTime))
              .set(metric(TestTableFields.TEST_FIELD), 1d)
              .set(metric(TestTableFields.TEST_FIELD2), 4d)
              .buildAndReset()
          )
        )

      val rows = tsdb.query(query).iterator

      val r1 = rows.next()
      r1.get[Long]("plus4") shouldBe 4
      r1.get[Long]("plus5") shouldBe 3

      rows.hasNext shouldBe false
  }

  it should "execute query like this (be able to cast long to double)" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT testField + testLongField as plus2 " +
      "FROM test_table " + timeBounds(and = false)
    val query = createQuery(sql)

    val pointTime = from.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_LONG_FIELD), time),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime))
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .set(metric(TestTableFields.TEST_LONG_FIELD), 3L)
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query).iterator

    val r1 = rows.next()
    r1.get[Double]("plus2") shouldBe 4d

    rows.hasNext shouldBe false
  }

  it should "handle arithmetic in having" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT time, lag(time) as lag_time, A " +
      "FROM test_table " + timeBounds(and = false) +
      "HAVING (time - lag_time) >= INTERVAL '10' SECOND"
    val query = createQuery(sql)

    val pointTime = from.getMillis + 10
    val pointTime2 = pointTime + 10 * 1000

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(dimension(TestDims.DIM_A), time),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime)).set(dimension(TestDims.DIM_A), "0000270761025003").buildAndReset(),
          b.set(time, Time(pointTime2)).set(dimension(TestDims.DIM_A), "0000270761025003").buildAndReset()
        )
      )

    val rows = tsdb.query(query).iterator

    val r1 = rows.next()
    r1.get[String]("A") shouldBe "0000270761025003"
    r1.get[Time]("time") shouldBe Time(pointTime2)
    r1.get[Time]("lag_time") shouldBe Time(pointTime)

    rows.hasNext shouldBe false
  }

  it should "handle string arithmetic in having" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT lag(testStringField) AS lag_operator, testStringField as operator " +
      "FROM test_table " +
      timeBounds(and = false) +
      "HAVING ((operator + lag_operator) <> 'MayorovaBlatov')"
    val query = createQuery(sql)

    val pointTime = from.getMillis + 10
    val pointTime2 = pointTime + 10 * 1000
    val pointTime3 = pointTime + 10 * 2000

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(metric(TestTableFields.TEST_STRING_FIELD), time),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime))
            .set(metric(TestTableFields.TEST_STRING_FIELD), "Mayorova")
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(metric(TestTableFields.TEST_STRING_FIELD), "Blatov")
            .buildAndReset(),
          b.set(time, Time(pointTime3))
            .set(metric(TestTableFields.TEST_STRING_FIELD), "Mayorova")
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query).iterator.toList
    rows should have size 1
    val row = rows.head
    row.get[String]("operator") shouldBe "Blatov"
    row.get[String]("lag_operator") shouldBe "Mayorova"
  }

  it should "handle arithmetic with window functions" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT testField, lag(testField), testField + lag(testField) as plus2 " +
      "FROM test_table " + timeBounds(and = false) + " HAVING lag(testField) IS NOT NULL "
    val query = createQuery(sql)

    val pointTime = from.getMillis + 10
    val pointTime2 = pointTime + 10 * 1000

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(metric(TestTableFields.TEST_FIELD), time),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime)).set(metric(TestTableFields.TEST_FIELD), 1d).buildAndReset(),
          b.set(time, Time(pointTime2)).set(metric(TestTableFields.TEST_FIELD), 5d).buildAndReset()
        )
      )

    val rows = tsdb.query(query).iterator.toList

    val r1 = rows.head
    r1.get[Double]("testField") shouldBe 5d
    r1.get[Double]("lag(testField)") shouldBe 1d
    r1.get[Double]("plus2") shouldBe 6d

    rows should have size 1
  }

  it should "support arithmetic on external links" in withTsdbMock { (tsdb, tsdbDaoMock) =>

    val link5 = mockCatalogService(tsdb, TestLinks.TEST_LINK5)
    val sql = "SELECT TestLink5_testField5D + 5 AS plus5 FROM test_table " + timeBounds(and = false)
    val query = createQuery(sql)

    val pointTime = from.getMillis + 10
    val pointTime2 = pointTime + 10 * 1000

    val doubleLinkExpr = link[Double](TestLinks.TEST_LINK5, LinkField[Double]("testField5D"))

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(dimension(TestDims.DIM_B), time),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Some(Time(pointTime))).set(dimension(TestDims.DIM_B), 12).buildAndReset(),
          b.set(time, Some(Time(pointTime2))).set(dimension(TestDims.DIM_B), 15).buildAndReset()
        )
      )

    (link5.setLinkedValues _)
      .expects(*, *, *)
      .onCall((idx, rs, _) => rs.foreach(r => r.set(idx, doubleLinkExpr, 15.23)))

    val rows = tsdb.query(query).iterator.toList

    val r1 = rows.head
    r1.get[Double]("plus5") shouldBe 20.23

    rows should have size 2
  }
}

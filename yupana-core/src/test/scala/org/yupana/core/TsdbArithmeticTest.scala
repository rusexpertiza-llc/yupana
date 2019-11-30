package org.yupana.core

import java.util.Properties

import org.joda.time.format.DateTimeFormat
import org.joda.time.{ DateTime, DateTimeZone, LocalDateTime }
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, OptionValues }
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.prop.TableDrivenPropertyChecks
import org.yupana.api.Time
import org.yupana.core.cache.CacheFactory
import org.yupana.core.model.InternalQuery
import org.yupana.core.utils.SparseTable

class TsdbArithmeticTest
    extends AnyFlatSpec
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
    val sql = "SELECT testField + testField2 as some_sum FROM test_table WHERE TAG_A = 'taga'" + timeBounds()
    val query = createQuery(sql)

    val pointTime = from.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2), time),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))), equ(dimension(TestDims.TAG_A), const("taga")))
        ),
        *,
        *
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime)))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .set(metric(TestTableFields.TEST_FIELD2), Some(2d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime)))
              .set(metric(TestTableFields.TEST_FIELD), Some(3d))
              .set(metric(TestTableFields.TEST_FIELD2), Some(4d))
              .buildAndReset()
          )
      )

    val rows = tsdb.query(query).iterator

    val r1 = rows.next()
    r1.fieldValueByName[Double]("some_sum").value shouldBe 3d

    val r2 = rows.next()
    r2.fieldValueByName[Double]("some_sum").value shouldBe 7d

    rows.hasNext shouldBe false
  }

  it should "execute query with arithmetic on aggregated values" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT sum(testField) as stf," +
      " sum(testField) * max(testField2) / 2 as mult FROM test_table WHERE TAG_A = 'taga'" +
      timeBounds() + " GROUP BY TAG_A"
    val query = createQuery(sql)

    val pointTime = from.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2), dimension(TestDims.TAG_A), time),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))), equ(dimension(TestDims.TAG_A), const("taga")))
        ),
        *,
        *
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime)))
              .set(dimension(TestDims.TAG_A), Some("taga"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .set(metric(TestTableFields.TEST_FIELD2), Some(2d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime)))
              .set(dimension(TestDims.TAG_A), Some("taga"))
              .set(metric(TestTableFields.TEST_FIELD), Some(3d))
              .set(metric(TestTableFields.TEST_FIELD2), Some(4d))
              .buildAndReset()
          )
      )

    val rows = tsdb.query(query).iterator

    val r1 = rows.next()
    r1.fieldValueByName[Double]("stf").value shouldBe 4d
    r1.fieldValueByName[Double]("mult").value shouldBe 8d

    rows.hasNext shouldBe false
  }

  it should "execute query with arithmetic inside CASE" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mock[ExternalLinkService[TestLinks.TestLink]]
    tsdb.registerExternalLink(TestLinks.TEST_LINK, testCatalogServiceMock)

    val sql = "SELECT TestLink_testField as address, TAG_A, sum(testField) as sum_receiptCount, " +
      "sum(CASE WHEN testStringField = '2' THEN testField2 " +
      "WHEN testStringField = '3' THEN (0 - testField2) " +
      "ELSE 0) as totalSum " +
      "FROM test_table " +
      "WHERE TAG_A in ('0000270761025003') " + timeBounds() + " GROUP BY TAG_A, address"
    val query = createQuery(sql)

    (testCatalogServiceMock.setLinkedValues _)
      .expects(*, *, Set(link(TestLinks.TEST_LINK, "testField")))
      .onCall(
        (qc, datas, _) =>
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
            dimension(TestDims.TAG_A),
            time
          ),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(dimension(TestDims.TAG_A), Set("0000270761025003"))
          )
        ),
        *,
        *
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime)))
              .set(dimension(TestDims.TAG_A), Some("0000270761025003"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .set(metric(TestTableFields.TEST_FIELD2), Some(2d))
              .set(metric(TestTableFields.TEST_STRING_FIELD), Some("3"))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime)))
              .set(dimension(TestDims.TAG_A), Some("0000270761025003"))
              .set(metric(TestTableFields.TEST_FIELD), Some(3d))
              .set(metric(TestTableFields.TEST_FIELD2), Some(4d))
              .set(metric(TestTableFields.TEST_STRING_FIELD), Some("3"))
              .buildAndReset()
          )
      )

    val rows = tsdb.query(query).iterator

    val r1 = rows.next()
    r1.fieldValueByName[Double]("totalSum").value shouldBe -6d

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
        .onCall(
          (_, b, _) =>
            Iterator(
              b.set(time, Some(Time(pointTime)))
                .set(metric(TestTableFields.TEST_FIELD), Some(1d))
                .set(metric(TestTableFields.TEST_FIELD2), Some(2d))
                .buildAndReset(),
              b.set(time, Some(Time(pointTime)))
                .set(metric(TestTableFields.TEST_FIELD), Some(1d))
                .set(metric(TestTableFields.TEST_FIELD2), Some(4d))
                .buildAndReset()
            )
        )

      val rows = tsdb.query(query).iterator

      val r1 = rows.next()
      r1.fieldValueByName[Long]("plus4").value shouldBe 4
      r1.fieldValueByName[Long]("plus5").value shouldBe 3

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
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime)))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .set(metric(TestTableFields.TEST_LONG_FIELD), Some(3L))
              .buildAndReset()
          )
      )

    val rows = tsdb.query(query).iterator

    val r1 = rows.next()
    r1.fieldValueByName[Double]("plus2").value shouldBe 4d

    rows.hasNext shouldBe false
  }

  it should "handle arithmetic in having" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT time, lag(time) as lag_time, TAG_A " +
      "FROM test_table " + timeBounds(and = false) +
      "HAVING (time - lag_time) >= INTERVAL '10' SECOND"
    val query = createQuery(sql)

    val pointTime = from.getMillis + 10
    val pointTime2 = pointTime + 10 * 1000

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(dimension(TestDims.TAG_A), time),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        *
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime))).set(dimension(TestDims.TAG_A), Some("0000270761025003")).buildAndReset(),
            b.set(time, Some(Time(pointTime2))).set(dimension(TestDims.TAG_A), Some("0000270761025003")).buildAndReset()
          )
      )

    val rows = tsdb.query(query).iterator

    val r1 = rows.next()
    r1.fieldValueByName[String]("TAG_A").value shouldBe "0000270761025003"
    r1.fieldValueByName[Time]("time").value shouldBe Time(pointTime2)
    r1.fieldValueByName[Time]("lag_time").value shouldBe Time(pointTime)

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
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime)))
              .set(metric(TestTableFields.TEST_STRING_FIELD), Some("Mayorova"))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(metric(TestTableFields.TEST_STRING_FIELD), Some("Blatov"))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime3)))
              .set(metric(TestTableFields.TEST_STRING_FIELD), Some("Mayorova"))
              .buildAndReset()
          )
      )

    val rows = tsdb.query(query).iterator.toList
    rows should have size 1
    val row = rows.head
    row.fieldValueByName[String]("operator").value shouldBe "Blatov"
    row.fieldValueByName[String]("lag_operator").value shouldBe "Mayorova"
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
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime))).set(metric(TestTableFields.TEST_FIELD), Some(1d)).buildAndReset(),
            b.set(time, Some(Time(pointTime2))).set(metric(TestTableFields.TEST_FIELD), Some(5d)).buildAndReset()
          )
      )

    val rows = tsdb.query(query).iterator.toList

    val r1 = rows.head
    r1.fieldValueByName[Double]("testField").value shouldBe 5d
    r1.fieldValueByName[Double]("lag(testField)").value shouldBe 1d
    r1.fieldValueByName[Double]("plus2").value shouldBe 6d

    rows should have size 1
  }
}

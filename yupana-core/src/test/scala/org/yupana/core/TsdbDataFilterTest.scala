package org.yupana.core

import java.util.Properties

import org.yupana.core.cache.CacheFactory
import org.yupana.core.model.InternalQuery
import org.yupana.core.utils.SparseTable
import org.joda.time.format.DateTimeFormat
import org.joda.time.{ DateTime, DateTimeZone, LocalDateTime }
import org.scalatest._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.yupana.api.Time
import org.yupana.api.query.{ DimensionExpr, Expression }

class TsdbDataFilterTest
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
  private def timeBounds(from: DateTime = from, to: DateTime = to) =
    s" AND time >= TIMESTAMP '${from.toString(format)}' AND time < TIMESTAMP '${to.toString(format)}'"

  "TSDB" should "execute query with filter by values" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT time AS time_time, testField, TAG_A, TAG_B FROM test_table WHERE testField = 1012" + timeBounds()
    val query = createQuery(sql)

    val pointTime = from.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression](
            time,
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.TAG_A),
            dimension(TestDims.TAG_B)
          ),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            equ(double2bigDecimal(metric(TestTableFields.TEST_FIELD)), const(BigDecimal(1012)))
          )
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Some(Time(pointTime)))
            .set(metric(TestTableFields.TEST_FIELD), Some(1012d))
            .set(dimension(TestDims.TAG_A), Some("test1"))
            .set(dimension(TestDims.TAG_B), Some("test2"))
            .buildAndReset(),
          b.set(time, Some(Time(pointTime + 100)))
            .set(metric(TestTableFields.TEST_FIELD), Some(1013d))
            .set(dimension(TestDims.TAG_A), Some("test1"))
            .set(dimension(TestDims.TAG_B), Some("test2"))
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head

    row.fieldValueByName[Time]("time_time").value shouldBe Time(pointTime)
    row.fieldValueByName[Double]("testField").value shouldBe 1012d
    row.fieldValueByName[String]("TAG_A").value shouldBe "test1"
    row.fieldValueByName[String]("TAG_B").value shouldBe "test2"
  }

  it should "execute query with filter by values and tags" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT time AS time_time, abs(testField) AS abs_test_field, TAG_A, TAG_B FROM test_table " +
      "WHERE testField = 1012 AND TAG_B = 'VB1'" + timeBounds()
    val query = createQuery(sql)

    val pointTime = from.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression](
            time,
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.TAG_A),
            dimension(TestDims.TAG_B)
          ),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            equ(double2bigDecimal(metric(TestTableFields.TEST_FIELD)), const(BigDecimal(1012))),
            equ(dimension(TestDims.TAG_B), const("VB1"))
          )
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Some(Time(pointTime)))
            .set(metric(TestTableFields.TEST_FIELD), Some(1012d))
            .set(dimension(TestDims.TAG_A), Some("test1"))
            .set(dimension(TestDims.TAG_B), Some("VB1"))
            .buildAndReset(),
          b.set(time, Some(Time(pointTime + 100)))
            .set(metric(TestTableFields.TEST_FIELD), Some(1013d))
            .set(dimension(TestDims.TAG_A), Some("test1"))
            .set(dimension(TestDims.TAG_B), Some("VB1"))
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head

    row.fieldValueByName[Time]("time_time").value shouldBe Time(pointTime)
    row.fieldValueByName[Double]("abs_test_field").value shouldBe 1012d
    row.fieldValueByName[String]("TAG_A").value shouldBe "test1"
    row.fieldValueByName[String]("TAG_B").value shouldBe "VB1"
  }

  it should "execute query with filter by values not presented in query.fields" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT time AS time_time, TAG_A, TAG_B FROM test_table WHERE testField <= 1012" + timeBounds()
    val query = createQuery(sql)

    val pointTime = from.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression](
            time,
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.TAG_A),
            dimension(TestDims.TAG_B)
          ),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            le(double2bigDecimal(metric(TestTableFields.TEST_FIELD)), const(BigDecimal(1012)))
          )
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Some(Time(pointTime)))
            .set(metric(TestTableFields.TEST_FIELD), Some(1012d))
            .set(dimension(TestDims.TAG_A), Some("test1"))
            .set(dimension(TestDims.TAG_B), Some("test2"))
            .buildAndReset(),
          b.set(time, Some(Time(pointTime + 100)))
            .set(metric(TestTableFields.TEST_FIELD), Some(1013d))
            .set(dimension(TestDims.TAG_A), Some("test1"))
            .set(dimension(TestDims.TAG_B), Some("test2"))
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head

    row.fieldValueByName[Time]("time_time").value shouldBe Time(pointTime)
    an[NoSuchElementException] should be thrownBy row.fieldValueByName[Double]("testField")
    row.fieldValueByName[String]("TAG_A").value shouldBe "test1"
    row.fieldValueByName[String]("TAG_B").value shouldBe "test2"
  }

  it should "execute query with filter by values comparing two ValueExprs" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT time AS time_time, TAG_A, TAG_B FROM test_table WHERE testField != testField2" + timeBounds()
    val query = createQuery(sql)

    val pointTime = from.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression](
            time,
            metric(TestTableFields.TEST_FIELD2),
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.TAG_A),
            dimension(TestDims.TAG_B)
          ),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            neq(metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2))
          )
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Some(Time(pointTime)))
            .set(metric(TestTableFields.TEST_FIELD), Some(1012d))
            .set(metric(TestTableFields.TEST_FIELD2), Some(1013d))
            .set(dimension(TestDims.TAG_A), Some("test11"))
            .set(dimension(TestDims.TAG_B), Some("test12"))
            .buildAndReset(),
          b.set(time, Some(Time(pointTime + 100)))
            .set(metric(TestTableFields.TEST_FIELD), Some(1013d))
            .set(metric(TestTableFields.TEST_FIELD2), Some(1013d))
            .set(dimension(TestDims.TAG_A), Some("test1"))
            .set(dimension(TestDims.TAG_B), Some("test2"))
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head

    row.fieldValueByName[Time]("time_time").value shouldBe Time(pointTime)
    an[NoSuchElementException] should be thrownBy row.fieldValueByName[Double]("testField")
    row.fieldValueByName[String]("TAG_A").value shouldBe "test11"
    row.fieldValueByName[String]("TAG_B").value shouldBe "test12"
  }

  it should "support IN for values" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT time, TAG_A, TAG_B, testField as F1 FROM test_table WHERE F1 IN (1012, 1014)" + timeBounds()
    val query = createQuery(sql)

    val pointTime = from.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression](
            time,
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.TAG_A),
            dimension(TestDims.TAG_B)
          ),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(metric(TestTableFields.TEST_FIELD), Set(1012d, 1014d))
          )
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Some(Time(pointTime)))
            .set(metric(TestTableFields.TEST_FIELD), Some(1012d))
            .set(dimension(TestDims.TAG_A), Some("test1"))
            .set(dimension(TestDims.TAG_B), Some("test2"))
            .buildAndReset(),
          b.set(time, Some(Time(pointTime)))
            .set(metric(TestTableFields.TEST_FIELD), Some(1014d))
            .set(dimension(TestDims.TAG_A), Some("test1"))
            .set(dimension(TestDims.TAG_B), Some("test2"))
            .buildAndReset()
        )
      )

    val iterator = tsdb.query(query).iterator

    val r1 = iterator.next()

    r1.fieldValueByName[Time]("time").value shouldBe Time(pointTime)
    r1.fieldValueByName[Double]("F1").value shouldBe 1012d
    r1.fieldValueByName[String]("TAG_A").value shouldBe "test1"
    r1.fieldValueByName[String]("TAG_B").value shouldBe "test2"

    val r2 = iterator.next()

    r2.fieldValueByName[Time]("time").value shouldBe Time(pointTime)
    r2.fieldValueByName[Double]("F1").value shouldBe 1014d
    r2.fieldValueByName[String]("TAG_A").value shouldBe "test1"
    r2.fieldValueByName[String]("TAG_B").value shouldBe "test2"

    iterator.hasNext shouldBe false
  }

  it should "support AND for values, catalogs and tags" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK2)

    val sql = "SELECT time, TAG_A, TAG_B, testField as F1 FROM test_table " +
      "WHERE F1 IN (1012, 1014) AND testStringField != 'Str@!' AND TestLink2_testField2 = 'Str@!Ster'" +
      timeBounds()
    val query = createQuery(sql)

    (testCatalogServiceMock.condition _)
      .expects(
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          in(metric(TestTableFields.TEST_FIELD), Set(1012d, 1014d)),
          neq(metric(TestTableFields.TEST_STRING_FIELD), const("Str@!")),
          equ(link(TestLinks.TEST_LINK2, "testField2"), const("Str@!Ster"))
        )
      )
      .returning(
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          in(metric(TestTableFields.TEST_FIELD), Set(1012d, 1014d)),
          neq(metric(TestTableFields.TEST_STRING_FIELD), const("Str@!")),
          in(dimension(TestDims.TAG_A), Set("test1"))
        )
      )

    val pointTime = from.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression](
            time,
            metric(TestTableFields.TEST_STRING_FIELD),
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.TAG_A),
            dimension(TestDims.TAG_B)
          ),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(metric(TestTableFields.TEST_FIELD), Set(1012d, 1014d)),
            neq(metric(TestTableFields.TEST_STRING_FIELD), const("Str@!")),
            in(dimension(TestDims.TAG_A), Set("test1"))
          )
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Some(Time(pointTime)))
            .set(metric(TestTableFields.TEST_FIELD), Some(1012d))
            .set(metric(TestTableFields.TEST_STRING_FIELD), Some("asdsadasd"))
            .set(dimension(TestDims.TAG_A), Some("test1"))
            .set(dimension(TestDims.TAG_B), Some("test2"))
            .buildAndReset(),
          b.set(time, Some(Time(pointTime)))
            .set(metric(TestTableFields.TEST_FIELD), Some(1012d))
            .set(metric(TestTableFields.TEST_STRING_FIELD), Some("Str@!"))
            .set(dimension(TestDims.TAG_A), Some("test1"))
            .set(dimension(TestDims.TAG_B), Some("test2"))
            .buildAndReset(),
          b.set(time, Some(Time(pointTime)))
            .set(metric(TestTableFields.TEST_FIELD), Some(1013d))
            .set(dimension(TestDims.TAG_A), Some("test1"))
            .set(dimension(TestDims.TAG_B), Some("test2"))
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head

    row.fieldValueByName[Time]("time").value shouldBe Time(pointTime)
    row.fieldValueByName[Double]("F1").value shouldBe 1012d
    row.fieldValueByName[String]("TAG_A").value shouldBe "test1"
    row.fieldValueByName[String]("TAG_B").value shouldBe "test2"
  }

  it should "support IS NULL for catalog fields" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK)

    val sql = "SELECT day(time) AS t, testField, TAG_A, TAG_B " +
      "FROM test_table WHERE TestLink_testField IS NULL" + timeBounds()
    val query = createQuery(sql)

    val condition = and(
      ge(time, const(Time(from))),
      lt(time, const(Time(to))),
      isNull(link(TestLinks.TEST_LINK, "testField"))
    )

    (testCatalogServiceMock.condition _).expects(condition).returning(condition)

    (testCatalogServiceMock.setLinkedValues _)
      .expects(*, *, Set(link(TestLinks.TEST_LINK, "testField")))
      .onCall((qc, datas, _) =>
        setCatalogValueByTag(
          qc,
          datas,
          TestLinks.TEST_LINK,
          SparseTable("test2a" -> Map("testField" -> "some-value"))
        )
      )

    val pointTime1 = from.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression](
            time,
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.TAG_A),
            dimension(TestDims.TAG_B)
          ),
          condition
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Some(Time(pointTime1)))
            .set(metric(TestTableFields.TEST_FIELD), Some(10d))
            .set(dimension(TestDims.TAG_A), Some("test1a"))
            .set(dimension(TestDims.TAG_B), Some("test2b"))
            .buildAndReset(),
          b.set(time, Some(Time(pointTime1)))
            .set(metric(TestTableFields.TEST_FIELD), Some(30d))
            .set(dimension(TestDims.TAG_A), Some("test2a"))
            .set(dimension(TestDims.TAG_B), Some("test3b"))
            .buildAndReset()
        )
      )

    val results = tsdb.query(query).toList
    results should have size 1

    val r1 = results.head
    r1.fieldValueByName[Time]("t").value shouldBe Time(from.withMillisOfDay(0).getMillis)
    r1.fieldValueByName[Double]("testField").value shouldBe 10d
    r1.fieldValueByName[String]("TAG_A").value shouldBe "test1a"
    r1.fieldValueByName[String]("TAG_B").value shouldBe "test2b"
  }

  it should "support IS NOT NULL for catalog fields" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK)

    val sql = "SELECT day(time) AS t, testField, TAG_A, TAG_B, TestLink_testField AS ctf " +
      "FROM test_table WHERE ctf IS NOT NULL" + timeBounds()
    val query = createQuery(sql)

    val condition = and(
      ge(time, const(Time(from))),
      lt(time, const(Time(to))),
      isNotNull(link(TestLinks.TEST_LINK, "testField"))
    )

    (testCatalogServiceMock.condition _).expects(condition).returning(condition)

    (testCatalogServiceMock.setLinkedValues _)
      .expects(*, *, Set(link(TestLinks.TEST_LINK, "testField")))
      .onCall((qc, datas, _) =>
        setCatalogValueByTag(
          qc,
          datas,
          TestLinks.TEST_LINK,
          SparseTable("test2a" -> Map("testField" -> "some-value"))
        )
      )

    val pointTime1 = from.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression](
            time,
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.TAG_A),
            dimension(TestDims.TAG_B)
          ),
          condition
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Some(Time(pointTime1)))
            .set(metric(TestTableFields.TEST_FIELD), Some(10d))
            .set(dimension(TestDims.TAG_A), Some("test1a"))
            .set(dimension(TestDims.TAG_B), Some("test2b"))
            .buildAndReset(),
          b.set(time, Some(Time(pointTime1)))
            .set(metric(TestTableFields.TEST_FIELD), Some(30d))
            .set(dimension(TestDims.TAG_A), Some("test2a"))
            .set(dimension(TestDims.TAG_B), Some("test3b"))
            .buildAndReset()
        )
      )

    val results = tsdb.query(query).toList
    results should have size 1

    val r1 = results.head
    r1.fieldValueByName[Time]("t").value shouldBe Time(from.withMillisOfDay(0).getMillis)
    r1.fieldValueByName[Double]("testField").value shouldBe 30d
    r1.fieldValueByName[String]("TAG_A").value shouldBe "test2a"
    r1.fieldValueByName[String]("TAG_B").value shouldBe "test3b"
    r1.fieldValueByName[String]("ctf").value shouldBe "some-value"
  }

  it should "support IS NULL and IS NOT NULL for catalog fields within AND among other conditions" in withTsdbMock {
    (tsdb, tsdbDaoMock) =>
      val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK)
      val testCatalogServiceMock2 = mockCatalogService(tsdb, TestLinks.TEST_LINK2)

      val sql = "SELECT day(time) AS t, testField, TAG_A, TAG_B, TestLink2_testField2 AS cf2 " +
        "FROM test_table " +
        "WHERE TestLink_testField IS NULL AND cf2 IS NOT NULL AND testField >= 1000 AND TAG_A != 'test1' AND TAG_B = 'testB2'" + timeBounds()
      val query = createQuery(sql)

      val condition = and(
        ge(time, const(Time(from))),
        lt(time, const(Time(to))),
        isNull(link(TestLinks.TEST_LINK, "testField")),
        isNotNull(link(TestLinks.TEST_LINK2, "testField2")),
        ge(double2bigDecimal(metric(TestTableFields.TEST_FIELD)), const(BigDecimal(1000))),
        neq(dimension(TestDims.TAG_A), const("test1")),
        equ(dimension(TestDims.TAG_B), const("testB2"))
      )

      (testCatalogServiceMock.condition _).expects(condition).returning(condition)
      (testCatalogServiceMock2.condition _).expects(condition).returning(condition)

      (testCatalogServiceMock.setLinkedValues _)
        .expects(*, *, Set(link(TestLinks.TEST_LINK, "testField")))
        .onCall((qc, datas, _) =>
          setCatalogValueByTag(
            qc,
            datas,
            TestLinks.TEST_LINK,
            SparseTable("test2a" -> Map("testField" -> "some-value"))
          )
        )

      (testCatalogServiceMock2.setLinkedValues _)
        .expects(*, *, Set(link(TestLinks.TEST_LINK2, "testField2")))
        .onCall((qc, datas, _) =>
          setCatalogValueByTag(
            qc,
            datas,
            TestLinks.TEST_LINK2,
            SparseTable("test1a" -> Map("testField2" -> "c2-value"), "test2a" -> Map("testField2" -> "some-value"))
          )
        )

      val pointTime1 = from.getMillis + 10

      (tsdbDaoMock.query _)
        .expects(
          InternalQuery(
            TestSchema.testTable,
            Set[Expression](
              time,
              metric(TestTableFields.TEST_FIELD),
              dimension(TestDims.TAG_A),
              dimension(TestDims.TAG_B)
            ),
            condition
          ),
          *,
          *
        )
        .onCall((_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime1)))
              .set(metric(TestTableFields.TEST_FIELD), Some(1001d))
              .set(dimension(TestDims.TAG_A), Some("test2a"))
              .set(dimension(TestDims.TAG_B), Some("testB2"))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime1 + 10)))
              .set(metric(TestTableFields.TEST_FIELD), Some(1002d))
              .set(dimension(TestDims.TAG_A), Some("test2a"))
              .set(dimension(TestDims.TAG_B), Some("testB2"))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime1 + 10)))
              .set(metric(TestTableFields.TEST_FIELD), Some(103d))
              .set(dimension(TestDims.TAG_A), Some("test2a"))
              .set(dimension(TestDims.TAG_B), Some("testB2"))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime1 + 10)))
              .set(metric(TestTableFields.TEST_FIELD), Some(1003d))
              .set(dimension(TestDims.TAG_A), Some("test1a"))
              .set(dimension(TestDims.TAG_B), Some("testB2"))
              .buildAndReset()
          )
        )

      val results = tsdb.query(query).toList
      results should have size 1

      val r1 = results.head
      r1.fieldValueByName[Time]("t").value shouldBe Time(from.withMillisOfDay(0).getMillis)
      r1.fieldValueByName[Double]("testField").value shouldBe 1003d
      r1.fieldValueByName[String]("TAG_A").value shouldBe "test1a"
      r1.fieldValueByName[String]("TAG_B").value shouldBe "testB2"
  }

  it should "support IS NULL and IS NOT NULL inside CASE" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK)
    val testCatalogServiceMock2 = mockCatalogService(tsdb, TestLinks.TEST_LINK2)

    val sql = "SELECT day(time) AS d, sum(CASE WHEN TestLink_testField IS NOT NULL THEN testField ELSE 0) as quantity " +
      "FROM test_table " +
      "WHERE TestLink2_testField2 = 'test2'" + timeBounds() + " GROUP BY d"
    val query = createQuery(sql)

    (testCatalogServiceMock.setLinkedValues _)
      .expects(*, *, Set(link(TestLinks.TEST_LINK, "testField")))
      .onCall((qc, datas, _) =>
        setCatalogValueByTag(qc, datas, TestLinks.TEST_LINK, SparseTable("test1a" -> Map("testField" -> "c1-value")))
      )

    (testCatalogServiceMock2.condition _)
      .expects(
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          equ(link(TestLinks.TEST_LINK2, "testField2"), const("test2"))
        )
      )
      .returning(
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          in(DimensionExpr(TestDims.TAG_A), Set("test1a", "test2a"))
        )
      )

    val pointTime1 = from.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression](time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.TAG_A)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(dimension(TestDims.TAG_A), Set("test1a", "test2a"))
          )
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Some(Time(pointTime1)))
            .set(metric(TestTableFields.TEST_FIELD), Some(1011d))
            .set(dimension(TestDims.TAG_A), Some("test1a"))
            .buildAndReset(),
          b.set(time, Some(Time(pointTime2)))
            .set(metric(TestTableFields.TEST_FIELD), Some(3001d))
            .set(dimension(TestDims.TAG_A), Some("test2a"))
            .buildAndReset()
        )
      )

    val results = tsdb.query(query).toList
    results should have size 1

    val r1 = results.head
    r1.fieldValueByName[Time]("d").value shouldBe Time(from.withMillisOfDay(0).getMillis)
    r1.fieldValueByName[Double]("quantity").value shouldBe 1011d
  }

  it should "filter before calculation if possible" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT time, testField3 / testField2 as div FROM test_table_2 WHERE testField2 <> 0" + timeBounds()
    val query = createQuery(sql)

    val pointTime = from.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable2,
          Set[Expression](time, metric(TestTable2Fields.TEST_FIELD2), metric(TestTable2Fields.TEST_FIELD3)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            neq(double2bigDecimal(metric(TestTable2Fields.TEST_FIELD2)), const(BigDecimal(0)))
          )
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Some(Time(pointTime)))
            .set(metric(TestTable2Fields.TEST_FIELD2), Some(0d))
            .set(metric(TestTable2Fields.TEST_FIELD3), Some(BigDecimal(5)))
            .buildAndReset()
        )
      )

    val results = tsdb.query(query).toList
    results should have size 0
  }
}

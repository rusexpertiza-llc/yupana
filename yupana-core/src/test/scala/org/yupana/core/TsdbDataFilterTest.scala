package org.yupana.core

import java.util.Properties
import org.scalatest._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query.{ Expression, LinkExpr, Original, Replace }
import org.yupana.api.schema.LinkField
import org.yupana.core.cache.CacheFactory
import org.yupana.core.model.InternalQuery
import org.yupana.core.utils.SparseTable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{ LocalDateTime, OffsetDateTime, ZoneOffset }
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

class TsdbDataFilterTest
    extends AnyFlatSpec
    with Matchers
    with TsdbMocks
    with OptionValues
    with TableDrivenPropertyChecks
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  import org.yupana.api.query.syntax.All._

  private val format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  override protected def beforeAll(): Unit = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("app.properties"))
    CacheFactory.init(properties)
  }

  override def beforeEach(): Unit = {
    CacheFactory.flushCaches()
  }

  val from: OffsetDateTime = LocalDateTime.of(2017, 10, 15, 12, 57).atOffset(ZoneOffset.UTC)
  val to: OffsetDateTime = from.plusDays(1)
  private def timeBounds(from: OffsetDateTime = from, to: OffsetDateTime = to) =
    s" AND time >= TIMESTAMP '${from.format(format)}' AND time < TIMESTAMP '${to.format(format)}'"

  "TSDB" should "execute query with filter by values" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT time AS time_time, testField, A, B FROM test_table WHERE testField = 1012" + timeBounds()
    val query = createQuery(sql)

    val pointTime = from.toInstant.toEpochMilli + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression[_]](
            time,
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.DIM_A),
            dimension(TestDims.DIM_B)
          ),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            equ(metric(TestTableFields.TEST_FIELD), const(1012d))
          )
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime))
            .set(metric(TestTableFields.TEST_FIELD), 1012d)
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), 2.toShort)
            .buildAndReset(),
          b.set(time, Time(pointTime + 100))
            .set(metric(TestTableFields.TEST_FIELD), 1013d)
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), 2.toShort)
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head

    row.get[Time]("time_time") shouldBe Time(pointTime)
    row.get[Double]("testField") shouldBe 1012d
    row.get[String]("A") shouldBe "test1"
    row.get[Short]("B") shouldBe 2.toShort
  }

  it should "execute query with filter by values and tags" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT time AS time_time, abs(testField) AS abs_test_field, A, B FROM test_table " +
      "WHERE testField = 1012 AND B = 31" + timeBounds()
    val query = createQuery(sql)

    val pointTime = from.toInstant.toEpochMilli + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression[_]](
            time,
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.DIM_A),
            dimension(TestDims.DIM_B)
          ),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            equ(metric(TestTableFields.TEST_FIELD), const(1012d)),
            equ(dimension(TestDims.DIM_B), const(31.toShort))
          )
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime))
            .set(metric(TestTableFields.TEST_FIELD), 1012d)
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), 31.toShort)
            .buildAndReset(),
          b.set(time, Time(pointTime + 100))
            .set(metric(TestTableFields.TEST_FIELD), 1013d)
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), 31.toShort)
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head

    row.get[Time]("time_time") shouldBe Time(pointTime)
    row.get[Double]("abs_test_field") shouldBe 1012d
    row.get[String]("A") shouldBe "test1"
    row.get[Short]("B") shouldBe 31
  }

  it should "execute query with filter by values not presented in query.fields" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT time AS time_time, A, B FROM test_table WHERE testField <= 1012" + timeBounds()
    val query = createQuery(sql)

    val pointTime = from.toInstant.toEpochMilli + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(
            time,
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.DIM_A),
            dimension(TestDims.DIM_B)
          ),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            le(metric(TestTableFields.TEST_FIELD), const(1012d))
          )
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime))
            .set(metric(TestTableFields.TEST_FIELD), 1012d)
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), 2.toShort)
            .buildAndReset(),
          b.set(time, Time(pointTime + 100))
            .set(metric(TestTableFields.TEST_FIELD), 1013d)
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), 2.toShort)
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head

    row.get[Time]("time_time") shouldBe Time(pointTime)
    an[NoSuchElementException] should be thrownBy row.get[Double]("testField")
    row.get[String]("A") shouldBe "test1"
    row.get[Short]("B") shouldBe 2
  }

  it should "execute query with filter by values comparing two ValueExprs" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT time AS time_time, A, B FROM test_table WHERE testField != testField2" + timeBounds()
    val query = createQuery(sql)

    val pointTime = from.toInstant.toEpochMilli + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(
            time,
            metric(TestTableFields.TEST_FIELD2),
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.DIM_A),
            dimension(TestDims.DIM_B)
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
          b.set(time, Time(pointTime))
            .set(metric(TestTableFields.TEST_FIELD), 1012d)
            .set(metric(TestTableFields.TEST_FIELD2), 1013d)
            .set(dimension(TestDims.DIM_A), "test11")
            .set(dimension(TestDims.DIM_B), "test12")
            .buildAndReset(),
          b.set(time, Time(pointTime + 100))
            .set(metric(TestTableFields.TEST_FIELD), 1013d)
            .set(metric(TestTableFields.TEST_FIELD2), 1013d)
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), "test2")
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head

    row.get[Time]("time_time") shouldBe Time(pointTime)
    an[NoSuchElementException] should be thrownBy row.get[Double]("testField")
    row.get[String]("A") shouldBe "test11"
    row.get[String]("B") shouldBe "test12"
  }

  it should "support IN for values" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT time, A, B, testField as F1 FROM test_table WHERE F1 IN (1012, 1014)" + timeBounds()
    val query = createQuery(sql)

    val pointTime = from.toInstant.toEpochMilli + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(
            time,
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.DIM_A),
            dimension(TestDims.DIM_B)
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
          b.set(time, Time(pointTime))
            .set(metric(TestTableFields.TEST_FIELD), 1012d)
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), "test2")
            .buildAndReset(),
          b.set(time, Time(pointTime))
            .set(metric(TestTableFields.TEST_FIELD), 1014d)
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), "test2")
            .buildAndReset()
        )
      )

    val iterator = tsdb.query(query)

    val r1 = iterator.next()

    r1.get[Time]("time") shouldBe Time(pointTime)
    r1.get[Double]("F1") shouldBe 1012d
    r1.get[String]("A") shouldBe "test1"
    r1.get[String]("B") shouldBe "test2"

    val r2 = iterator.next()

    r2.get[Time]("time") shouldBe Time(pointTime)
    r2.get[Double]("F1") shouldBe 1014d
    r2.get[String]("A") shouldBe "test1"
    r2.get[String]("B") shouldBe "test2"

    iterator.hasNext shouldBe false
  }

  it should "support NOT IN for values" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT time, A, B, testField as F1 FROM test_table WHERE F1 NOT IN (123, 456)" + timeBounds()
    val query = createQuery(sql)

    val pointTime = from.toInstant.toEpochMilli + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(
            time,
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.DIM_A),
            dimension(TestDims.DIM_B)
          ),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            notIn(metric(TestTableFields.TEST_FIELD), Set(123d, 456d))
          )
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime))
            .set(metric(TestTableFields.TEST_FIELD), 123d)
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), "test2")
            .buildAndReset(),
          b.set(time, Time(pointTime))
            .set(metric(TestTableFields.TEST_FIELD), 234d)
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), "test2")
            .buildAndReset(),
          b.set(time, Time(pointTime))
            .set(metric(TestTableFields.TEST_FIELD), null)
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), "test2")
            .buildAndReset()
        )
      )

    val iterator = tsdb.query(query)

    val r = iterator.next()

    r.get[Time]("time") shouldBe Time(pointTime)
    r.get[Double]("F1") shouldBe 234d
    r.get[String]("A") shouldBe "test1"
    r.get[String]("B") shouldBe "test2"

    iterator.hasNext shouldBe false
  }

  it should "support AND for values, catalogs and tags" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK2)

    val sql = "SELECT time, A, B, testField as F1 FROM test_table " +
      "WHERE F1 IN (1012, 1014) AND testStringField != 'Str@!' AND TestLink2_testField2 = 'Str@!Ster'" +
      timeBounds()
    val query = createQuery(sql)

    val c = equ(lower(link(TestLinks.TEST_LINK2, "testField2")), const("str@!ster"))
    (testCatalogServiceMock.transformCondition _)
      .expects(
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          in(metric(TestTableFields.TEST_FIELD), Set(1012d, 1014d)),
          neq(lower(metric(TestTableFields.TEST_STRING_FIELD)), const("str@!")),
          equ(lower(link(TestLinks.TEST_LINK2, "testField2")), const("str@!ster"))
        )
      )
      .returning(
        Seq(
          Replace(
            Set(c),
            and(
              in(dimension(TestDims.DIM_A), Set("test1"))
            )
          )
        )
      )

    val pointTime = from.toInstant.toEpochMilli + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(
            time,
            metric(TestTableFields.TEST_STRING_FIELD),
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.DIM_A),
            dimension(TestDims.DIM_B)
          ),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(metric(TestTableFields.TEST_FIELD), Set(1012d, 1014d)),
            neq(lower(metric(TestTableFields.TEST_STRING_FIELD)), const("str@!")),
            in(dimension(TestDims.DIM_A), Set("test1"))
          )
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime))
            .set(metric(TestTableFields.TEST_FIELD), 1012d)
            .set(metric(TestTableFields.TEST_STRING_FIELD), "asdsadasd")
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), "test2")
            .buildAndReset(),
          b.set(time, Time(pointTime))
            .set(metric(TestTableFields.TEST_FIELD), 1012d)
            .set(metric(TestTableFields.TEST_STRING_FIELD), "Str@!")
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), "test2")
            .buildAndReset(),
          b.set(time, Time(pointTime))
            .set(metric(TestTableFields.TEST_FIELD), 1013d)
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), "test2")
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head

    row.get[Time]("time") shouldBe Time(pointTime)
    row.get[Double]("F1") shouldBe 1012d
    row.get[String]("A") shouldBe "test1"
    row.get[String]("B") shouldBe "test2"
  }

  it should "support IS NULL for catalog fields" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK)

    val sql = "SELECT day(time) AS t, testField, A, B " +
      "FROM test_table WHERE TestLink_testField IS NULL" + timeBounds()
    val query = createQuery(sql)

    val condition = and(
      ge(time, const(Time(from))),
      lt(time, const(Time(to))),
      isNull(link(TestLinks.TEST_LINK, "testField"))
    )

    (testCatalogServiceMock.transformCondition _)
      .expects(condition)
      .returning(
        Seq(
          Original(Set(condition))
        )
      )

    (testCatalogServiceMock.setLinkedValues _)
      .expects(*, *, Set(link(TestLinks.TEST_LINK, "testField")).asInstanceOf[Set[LinkExpr[_]]])
      .onCall((qc, datas, _) =>
        setCatalogValueByTag(
          qc,
          datas,
          TestLinks.TEST_LINK,
          SparseTable("test2a" -> Map("testField" -> "some-value"))
        )
      )

    val pointTime1 = from.toInstant.toEpochMilli + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(
            time,
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.DIM_A),
            dimension(TestDims.DIM_B)
          ),
          condition
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1))
            .set(metric(TestTableFields.TEST_FIELD), 10d)
            .set(dimension(TestDims.DIM_A), "test1a")
            .set(dimension(TestDims.DIM_B), "test2b")
            .buildAndReset(),
          b.set(time, Time(pointTime1))
            .set(metric(TestTableFields.TEST_FIELD), 30d)
            .set(dimension(TestDims.DIM_A), "test2a")
            .set(dimension(TestDims.DIM_B), "test3b")
            .buildAndReset()
        )
      )

    val results = tsdb.query(query).toList
    results should have size 1

    val r1 = results.head
    r1.get[Time]("t") shouldBe Time(from.truncatedTo(ChronoUnit.DAYS).toInstant.toEpochMilli)
    r1.get[Double]("testField") shouldBe 10d
    r1.get[String]("A") shouldBe "test1a"
    r1.get[String]("B") shouldBe "test2b"
  }

  it should "support IS NOT NULL for catalog fields" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK)

    val sql = "SELECT hour(time) AS t, testField, A, B, TestLink_testField AS ctf " +
      "FROM test_table WHERE ctf IS NOT NULL" + timeBounds()
    val query = createQuery(sql)

    val condition = and(
      ge(time, const(Time(from))),
      lt(time, const(Time(to))),
      isNotNull(link(TestLinks.TEST_LINK, "testField"))
    )

    (testCatalogServiceMock.transformCondition _).expects(condition).returning(Seq(Original(Set(condition))))

    (testCatalogServiceMock.setLinkedValues _)
      .expects(*, *, Set(link(TestLinks.TEST_LINK, "testField")).asInstanceOf[Set[LinkExpr[_]]])
      .onCall((qc, datas, _) =>
        setCatalogValueByTag(
          qc,
          datas,
          TestLinks.TEST_LINK,
          SparseTable("test2a" -> Map("testField" -> "some-value"))
        )
      )

    val pointTime1 = from.toInstant.toEpochMilli + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(
            time,
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.DIM_A),
            dimension(TestDims.DIM_B)
          ),
          condition
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1))
            .set(metric(TestTableFields.TEST_FIELD), 10d)
            .set(dimension(TestDims.DIM_A), "test1a")
            .set(dimension(TestDims.DIM_B), "test2b")
            .buildAndReset(),
          b.set(time, Time(pointTime1))
            .set(metric(TestTableFields.TEST_FIELD), 30d)
            .set(dimension(TestDims.DIM_A), "test2a")
            .set(dimension(TestDims.DIM_B), "test3b")
            .buildAndReset()
        )
      )

    val results = tsdb.query(query).toList
    results should have size 1

    val r1 = results.head
    r1.get[Time]("t") shouldBe Time(from.truncatedTo(ChronoUnit.HOURS).toInstant.toEpochMilli)
    r1.get[Double]("testField") shouldBe 30d
    r1.get[String]("A") shouldBe "test2a"
    r1.get[String]("B") shouldBe "test3b"
    r1.get[String]("ctf") shouldBe "some-value"
  }

  it should "support IS NULL and IS NOT NULL for catalog fields within AND among other conditions" in withTsdbMock {
    (tsdb, tsdbDaoMock) =>
      val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK)
      val testCatalogServiceMock2 = mockCatalogService(tsdb, TestLinks.TEST_LINK2)

      val sql = "SELECT day(time) AS t, testField, A, B, TestLink2_testField2 AS cf2 " +
        "FROM test_table " +
        "WHERE TestLink_testField IS NULL AND cf2 IS NOT NULL AND testField >= 1000 AND A != 'test1' AND B = 15" + timeBounds()
      val query = createQuery(sql)

      val condition = and(
        ge(time, const(Time(from))),
        lt(time, const(Time(to))),
        isNull(link(TestLinks.TEST_LINK, "testField")),
        isNotNull(link(TestLinks.TEST_LINK2, "testField2")),
        ge(metric(TestTableFields.TEST_FIELD), const(1000d)),
        neq(lower(dimension(TestDims.DIM_A)), const("test1")),
        equ(dimension(TestDims.DIM_B), const(15.toShort))
      )

      (testCatalogServiceMock.transformCondition _)
        .expects(condition)
        .returning(
          Seq(Original(Set(condition)))
        )
      (testCatalogServiceMock2.transformCondition _)
        .expects(condition)
        .returning(
          Seq(Original(Set(condition)))
        )

      (testCatalogServiceMock.setLinkedValues _)
        .expects(*, *, Set(link(TestLinks.TEST_LINK, "testField")).asInstanceOf[Set[LinkExpr[_]]])
        .onCall((qc, datas, _) =>
          setCatalogValueByTag(
            qc,
            datas,
            TestLinks.TEST_LINK,
            SparseTable("test2a" -> Map("testField" -> "some-value"))
          )
        )

      (testCatalogServiceMock2.setLinkedValues _)
        .expects(*, *, Set(link(TestLinks.TEST_LINK2, "testField2")).asInstanceOf[Set[LinkExpr[_]]])
        .onCall((qc, datas, _) =>
          setCatalogValueByTag(
            qc,
            datas,
            TestLinks.TEST_LINK2,
            SparseTable("test1a" -> Map("testField2" -> "c2-value"), "test2a" -> Map("testField2" -> "some-value"))
          )
        )

      val pointTime1 = from.toInstant.toEpochMilli + 10

      (tsdbDaoMock.query _)
        .expects(
          InternalQuery(
            TestSchema.testTable,
            Set(
              time,
              metric(TestTableFields.TEST_FIELD),
              dimension(TestDims.DIM_A),
              dimension(TestDims.DIM_B)
            ),
            condition
          ),
          *,
          *
        )
        .onCall((_, b, _) =>
          Iterator(
            b.set(time, Time(pointTime1))
              .set(metric(TestTableFields.TEST_FIELD), 1001d)
              .set(dimension(TestDims.DIM_A), "test2a")
              .set(dimension(TestDims.DIM_B), 15.toShort)
              .buildAndReset(),
            b.set(time, Time(pointTime1 + 10))
              .set(metric(TestTableFields.TEST_FIELD), 1002d)
              .set(dimension(TestDims.DIM_A), "test2a")
              .set(dimension(TestDims.DIM_B), 15.toShort)
              .buildAndReset(),
            b.set(time, Time(pointTime1 + 10))
              .set(metric(TestTableFields.TEST_FIELD), 103d)
              .set(dimension(TestDims.DIM_A), "test2a")
              .set(dimension(TestDims.DIM_B), 15.toShort)
              .buildAndReset(),
            b.set(time, Time(pointTime1 + 10))
              .set(metric(TestTableFields.TEST_FIELD), 1003d)
              .set(dimension(TestDims.DIM_A), "test1a")
              .set(dimension(TestDims.DIM_B), 15.toShort)
              .buildAndReset()
          )
        )

      val results = tsdb.query(query).toList
      results should have size 1

      val r1 = results.head
      r1.get[Time]("t") shouldBe Time(from.truncatedTo(ChronoUnit.DAYS).toInstant.toEpochMilli)
      r1.get[Double]("testField") shouldBe 1003d
      r1.get[String]("A") shouldBe "test1a"
      r1.get[Short]("B") shouldBe 15.toShort
  }

  it should "support IS NULL and IS NOT NULL inside CASE" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK)
    val testCatalogServiceMock2 = mockCatalogService(tsdb, TestLinks.TEST_LINK2)

    val sql =
      "SELECT day(time) AS d, sum(CASE WHEN TestLink_testField IS NOT NULL THEN testField ELSE 0) as quantity " +
        "FROM test_table " +
        "WHERE TestLink2_testField2 = 'test2'" + timeBounds() + " GROUP BY d"
    val query = createQuery(sql)

    (testCatalogServiceMock.setLinkedValues _)
      .expects(*, *, Set(link(TestLinks.TEST_LINK, "testField")).asInstanceOf[Set[LinkExpr[_]]])
      .onCall((qc, datas, _) =>
        setCatalogValueByTag(qc, datas, TestLinks.TEST_LINK, SparseTable("test1a" -> Map("testField" -> "c1-value")))
      )

    val c = equ(lower(link(TestLinks.TEST_LINK2, "testField2")), const("test2"))
    (testCatalogServiceMock2.transformCondition _)
      .expects(
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          c
        )
      )
      .returning(
        Seq(
          Replace(
            Set(c),
            and(
              in(lower(dimension(TestDims.DIM_A)), Set("test1a", "test2a"))
            )
          )
        )
      )

    val pointTime1 = from.toInstant.toEpochMilli + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.DIM_A)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(lower(dimension(TestDims.DIM_A)), Set("test1a", "test2a"))
          )
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1))
            .set(metric(TestTableFields.TEST_FIELD), 1011d)
            .set(dimension(TestDims.DIM_A), "test1a")
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(metric(TestTableFields.TEST_FIELD), 3001d)
            .set(dimension(TestDims.DIM_A), "test2a")
            .buildAndReset()
        )
      )

    val results = tsdb.query(query).toList
    results should have size 1

    val r1 = results.head
    r1.get[Time]("d") shouldBe Time(from.truncatedTo(ChronoUnit.DAYS).toInstant.toEpochMilli)
    r1.get[Double]("quantity") shouldBe 1011d
  }

  it should "filter before calculation if possible" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT time, testField3 / testField2 as div FROM test_table_2 WHERE testField2 <> 0" + timeBounds()
    val query = createQuery(sql)

    val pointTime = from.toInstant.toEpochMilli + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable2,
          Set(time, metric(TestTable2Fields.TEST_FIELD2), metric(TestTable2Fields.TEST_FIELD3)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            neq(metric(TestTable2Fields.TEST_FIELD2), const(0d))
          )
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime))
            .set(metric(TestTable2Fields.TEST_FIELD2), 0d)
            .set(metric(TestTable2Fields.TEST_FIELD3), BigDecimal(5))
            .buildAndReset()
        )
      )

    val results = tsdb.query(query).toList
    results should have size 0
  }

  it should "support numeric filtering on external links fields" in withTsdbMock { (tsdb, tsdbDaoMock) =>

    val link5 = mockCatalogService(tsdb, TestLinks.TEST_LINK5)
    val sql = "SELECT B FROM test_table WHERE TestLink5_testField5D > 20" + timeBounds()
    val query = createQuery(sql)

    val doubleLinkExpr = link[Double](TestLinks.TEST_LINK5, LinkField[Double]("testField5D"))

    (tsdbDaoMock.query _)
      .expects(*, *, *)
      .onCall((_, b, _) =>
        Iterator(
          b.set(dimension(TestDims.DIM_B), 12.toShort).buildAndReset(),
          b.set(dimension(TestDims.DIM_B), 15.toShort).buildAndReset()
        )
      )

    (link5.setLinkedValues _)
      .expects(*, *, *)
      .onCall((idx, rs, _) =>
        rs.foreach { r =>
          val b = r.get(idx, dimension(TestDims.DIM_B))
          r.set(idx, doubleLinkExpr, if (b == 12) 10.0 else 30.0)
        }
      )

    (link5.transformCondition _).expects(*).onCall((c: Condition) => Seq(Original(Set(c))))

    val rows = tsdb.query(query).toList

    rows.size shouldEqual 1
    val r1 = rows.head
    r1.get[Int]("B") shouldBe 15
  }
}

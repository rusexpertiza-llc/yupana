package org.yupana.core

import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.yupana.api.Time
import org.yupana.api.query.{ Expression, LinkExpr }
import org.yupana.api.schema.LinkField
import org.yupana.core.cache.CacheFactory
import org.yupana.core.model.InternalQuery
import org.yupana.core.settings.Settings
import org.yupana.core.utils.SparseTable
import org.yupana.utils.RussianTokenizer

import java.time.format.DateTimeFormatter
import java.time.{ OffsetDateTime, ZoneOffset }
import java.util.Properties

class TsdbArithmeticTest
    extends AnyFlatSpec
    with Matchers
    with TsdbMocks
    with OptionValues
    with TableDrivenPropertyChecks
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  import org.yupana.api.query.syntax.All._

  private val format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  implicit private val calculator: ConstantCalculator = new ConstantCalculator(RussianTokenizer)

  override protected def beforeAll(): Unit = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("app.properties"))
    CacheFactory.init(Settings(properties))
  }

  override def beforeEach(): Unit = {
    CacheFactory.flushCaches()
  }

  val from: OffsetDateTime = OffsetDateTime.of(2017, 10, 15, 12, 57, 0, 0, ZoneOffset.UTC)
  val to: OffsetDateTime = from.plusDays(1)

  private def timeBounds(from: OffsetDateTime = from, to: OffsetDateTime = to, and: Boolean = true): String = {
    s" ${if (and) "AND" else "WHERE"} time >= TIMESTAMP '${from.format(format)}' AND time < TIMESTAMP '${to.format(format)}' "
  }

  "TSDB" should "execute query with arithmetic (no aggregations)" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT testField + testField2 as some_sum FROM test_table WHERE A = 'taga'" + timeBounds()
    val query = createQuery(sql)

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression[_]](time, metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2)),
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
          b.set(metric(TestTableFields.TEST_FIELD), 1d)
            .set(metric(TestTableFields.TEST_FIELD2), 2d)
            .buildAndReset(),
          b.set(metric(TestTableFields.TEST_FIELD), 3d)
            .set(metric(TestTableFields.TEST_FIELD2), 4d)
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query)

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

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression[_]](
            time,
            metric(TestTableFields.TEST_FIELD),
            metric(TestTableFields.TEST_FIELD2),
            dimension(TestDims.DIM_A)
          ),
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
          b.set(dimension(TestDims.DIM_A), "taga")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .set(metric(TestTableFields.TEST_FIELD2), 2d)
            .buildAndReset(),
          b.set(dimension(TestDims.DIM_A), "taga")
            .set(metric(TestTableFields.TEST_FIELD), 3d)
            .set(metric(TestTableFields.TEST_FIELD2), 4d)
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query)

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

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression[_]](
            time,
            metric(TestTableFields.TEST_FIELD),
            metric(TestTableFields.TEST_FIELD2),
            metric(TestTableFields.TEST_STRING_FIELD),
            dimension(TestDims.DIM_A)
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
          b.set(dimension(TestDims.DIM_A), "0000270761025003")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .set(metric(TestTableFields.TEST_FIELD2), 2d)
            .set(metric(TestTableFields.TEST_STRING_FIELD), "3")
            .buildAndReset(),
          b.set(dimension(TestDims.DIM_A), "0000270761025003")
            .set(metric(TestTableFields.TEST_FIELD), 3d)
            .set(metric(TestTableFields.TEST_FIELD2), 4d)
            .set(metric(TestTableFields.TEST_STRING_FIELD), "3")
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query)

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

      val pointTime = from.toInstant.toEpochMilli + 10

      (tsdbDaoMock.query _)
        .expects(
          InternalQuery(
            TestSchema.testTable,
            Set[Expression[_]](metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_FIELD2), time),
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

      val rows = tsdb.query(query)

      val r1 = rows.next()
      r1.get[Long]("plus4") shouldBe 4
      r1.get[Long]("plus5") shouldBe 3

      rows.hasNext shouldBe false
  }

  it should "calculate count and distinct_count for metric fields when evaluating each data row including null values" in withTsdbMock {
    (tsdb, tsdbDaoMock) =>
      val sql =
        """SELECT 
          |count(testField) c1, 
          |count(testField2) c2,
          |count(testStringField) c3,
          |count(testLongField) c4,
          |count(testBigDecimalField) c5, 
          |distinct_count(testField) dc1, 
          |distinct_count(testField2) dc2,
          |distinct_count(testStringField) dc3,
          |distinct_count(testLongField) dc4,
          |distinct_count(testBigDecimalField) dc5 
          |""".stripMargin +
          "FROM test_table " + timeBounds(and = false) + " GROUP BY day(time)"
      val query = createQuery(sql)

      val pointTime = from.toInstant.toEpochMilli + 10

      (tsdbDaoMock.query _)
        .expects(
          InternalQuery(
            TestSchema.testTable,
            Set(
              metric(TestTableFields.TEST_FIELD),
              metric(TestTableFields.TEST_FIELD2),
              metric(TestTableFields.TEST_STRING_FIELD),
              metric(TestTableFields.TEST_LONG_FIELD),
              metric(TestTableFields.TEST_BIGDECIMAL_FIELD),
              time
            ),
            and(ge(time, const(Time(from))), lt(time, const(Time(to))))
          ),
          *,
          *
        )
        .onCall((_, b, _) =>
          Iterator(
            b.set(time, Time(pointTime))
              .set(metric(TestTableFields.TEST_FIELD), null)
              .set(metric(TestTableFields.TEST_FIELD2), null)
              .set(metric(TestTableFields.TEST_STRING_FIELD), "a")
              .set(metric(TestTableFields.TEST_LONG_FIELD), null)
              .set(metric(TestTableFields.TEST_BIGDECIMAL_FIELD), BigDecimal(1))
              .buildAndReset(),
            b.set(time, Time(pointTime))
              .set(metric(TestTableFields.TEST_FIELD), 1d)
              .set(metric(TestTableFields.TEST_FIELD2), null)
              .set(metric(TestTableFields.TEST_STRING_FIELD), null)
              .set(metric(TestTableFields.TEST_LONG_FIELD), 1L)
              .set(metric(TestTableFields.TEST_BIGDECIMAL_FIELD), null)
              .buildAndReset(),
            b.set(time, Time(pointTime))
              .set(metric(TestTableFields.TEST_FIELD), 2d)
              .set(metric(TestTableFields.TEST_FIELD2), null)
              .set(metric(TestTableFields.TEST_STRING_FIELD), "b")
              .set(metric(TestTableFields.TEST_LONG_FIELD), 1L)
              .set(metric(TestTableFields.TEST_BIGDECIMAL_FIELD), null)
              .buildAndReset(),
            b.set(time, Time(pointTime))
              .set(metric(TestTableFields.TEST_FIELD), null)
              .set(metric(TestTableFields.TEST_FIELD2), null)
              .set(metric(TestTableFields.TEST_STRING_FIELD), null)
              .set(metric(TestTableFields.TEST_LONG_FIELD), 2L)
              .set(metric(TestTableFields.TEST_BIGDECIMAL_FIELD), BigDecimal(2))
              .buildAndReset(),
            b.set(time, Time(pointTime))
              .set(metric(TestTableFields.TEST_FIELD), 1d)
              .set(metric(TestTableFields.TEST_FIELD2), null)
              .set(metric(TestTableFields.TEST_STRING_FIELD), "a")
              .set(metric(TestTableFields.TEST_LONG_FIELD), null)
              .set(metric(TestTableFields.TEST_BIGDECIMAL_FIELD), BigDecimal(1))
              .buildAndReset()
          )
        )

      val rows = tsdb.query(query)

      val r1 = rows.next()
      r1.get[Long]("c1") shouldBe 3
      r1.get[Long]("c2") shouldBe 0
      r1.get[Long]("c3") shouldBe 3
      r1.get[Long]("c4") shouldBe 3
      r1.get[Long]("c5") shouldBe 3
      r1.get[Long]("dc1") shouldBe 2
      r1.get[Long]("dc2") shouldBe 0
      r1.get[Long]("dc3") shouldBe 2
      r1.get[Long]("dc4") shouldBe 2
      r1.get[Long]("dc5") shouldBe 2

      rows.hasNext shouldBe false
  }

  it should "calculate count and distinct_count for dimension fields when evaluating each data rows" in withTsdbMock {
    (tsdb, tsdbDaoMock) =>
      val sql =
        """SELECT
          |count(B) cB,
          |count(A) cA,
          |distinct_count(B) dcB,
          |distinct_count(A) dcA
          |""".stripMargin +
          "FROM test_table " + timeBounds(and = false) + " GROUP BY day(time)"

      val query = createQuery(sql)

      val pointTime = from.toInstant.toEpochMilli + 10

      (tsdbDaoMock.query _)
        .expects(
          InternalQuery(
            TestSchema.testTable,
            Set(
              dimension(TestDims.DIM_B),
              dimension(TestDims.DIM_A),
              time
            ),
            and(ge(time, const(Time(from))), lt(time, const(Time(to))))
          ),
          *,
          *
        )
        .onCall((_, b, _) =>
          Iterator(
            b.set(time, Time(pointTime))
              .set(dimension(TestDims.DIM_B), 1: Short)
              .set(dimension(TestDims.DIM_A), "0000270761025003")
              .buildAndReset(),
            b.set(time, Time(pointTime))
              .set(dimension(TestDims.DIM_B), 2: Short)
              .set(dimension(TestDims.DIM_A), "0000270761025002")
              .buildAndReset(),
            b.set(time, Time(pointTime))
              .set(dimension(TestDims.DIM_B), 1: Short)
              .set(dimension(TestDims.DIM_A), "0000270761025001")
              .buildAndReset(),
            b.set(time, Time(pointTime))
              .set(dimension(TestDims.DIM_B), 1: Short)
              .set(dimension(TestDims.DIM_A), "0000270761025003")
              .buildAndReset()
          )
        )

      val rows = tsdb.query(query)

      val r1 = rows.next()
      r1.get[Long]("cB") shouldBe 4
      r1.get[Long]("cA") shouldBe 4
      r1.get[Long]("dcB") shouldBe 2
      r1.get[Long]("dcA") shouldBe 3

      rows.hasNext shouldBe false
  }

  it should "calculate hll_count for metric fields when evaluating each data row including null field values" in withTsdbMock {
    (tsdb, tsdbDaoMock) =>
      val sql =
        """SELECT 
          |hll_count(testStringField, 0.01) as hllString, 
          |hll_count(testLongField, 0.01) as hllLong, 
          |hll_count(testTimeField, 0.01) as hllTime """.stripMargin +
          "FROM test_table " + timeBounds(and = false) + " GROUP BY day(time)"
      val query = createQuery(sql)

      val pointTime = from.toInstant.toEpochMilli + 10

      (tsdbDaoMock.query _)
        .expects(
          InternalQuery(
            TestSchema.testTable,
            Set(
              metric(TestTableFields.TEST_STRING_FIELD),
              metric(TestTableFields.TEST_LONG_FIELD),
              metric(TestTableFields.TEST_TIME_FIELD),
              time
            ),
            and(ge(time, const(Time(from))), lt(time, const(Time(to))))
          ),
          *,
          *
        )
        .onCall((_, b, _) =>
          Iterator(
            b.set(time, Time(pointTime))
              .set(metric(TestTableFields.TEST_STRING_FIELD), null)
              .set(metric(TestTableFields.TEST_LONG_FIELD), null)
              .set(metric(TestTableFields.TEST_TIME_FIELD), null)
              .buildAndReset(),
            b.set(time, Time(pointTime))
              .set(metric(TestTableFields.TEST_STRING_FIELD), "1d")
              .set(metric(TestTableFields.TEST_LONG_FIELD), 1L)
              .set(metric(TestTableFields.TEST_TIME_FIELD), Time(1L))
              .buildAndReset(),
            b.set(time, Time(pointTime))
              .set(metric(TestTableFields.TEST_STRING_FIELD), null)
              .set(metric(TestTableFields.TEST_LONG_FIELD), 1L)
              .set(metric(TestTableFields.TEST_TIME_FIELD), null)
              .buildAndReset(),
            b.set(time, Time(pointTime))
              .set(metric(TestTableFields.TEST_STRING_FIELD), "2d")
              .set(metric(TestTableFields.TEST_LONG_FIELD), 2L)
              .set(metric(TestTableFields.TEST_TIME_FIELD), Time(2L))
              .buildAndReset(),
            b.set(time, Time(pointTime))
              .set(metric(TestTableFields.TEST_STRING_FIELD), "1d")
              .set(metric(TestTableFields.TEST_LONG_FIELD), null)
              .set(metric(TestTableFields.TEST_TIME_FIELD), Time(1L))
              .buildAndReset()
          )
        )

      val rows = tsdb.query(query)

      val r1 = rows.next()
      r1.get[Long]("hllString") shouldBe 2
      r1.get[Long]("hllLong") shouldBe 2
      r1.get[Long]("hllTime") shouldBe 2

      rows.hasNext shouldBe false
  }

  it should "calculate count, distinct_count and hll_count for metric fields when evaluating each data row with null values" in withTsdbMock {
    (tsdb, tsdbDaoMock) =>
      val sql =
        "SELECT count(testLongField) as c, distinct_count(testLongField) as cd, hll_count(testLongField, 0.01) as ch " +
          "FROM test_table " + timeBounds(and = false) + " GROUP BY day(time)"
      val query = createQuery(sql)

      val pointTime = from.toInstant.toEpochMilli + 10

      (tsdbDaoMock.query _)
        .expects(
          InternalQuery(
            TestSchema.testTable,
            Set(metric(TestTableFields.TEST_LONG_FIELD), time),
            and(ge(time, const(Time(from))), lt(time, const(Time(to))))
          ),
          *,
          *
        )
        .onCall((_, b, _) =>
          Iterator(
            b.set(time, Time(pointTime))
              .set(metric(TestTableFields.TEST_LONG_FIELD), null)
              .buildAndReset(),
            b.set(time, Time(pointTime))
              .set(metric(TestTableFields.TEST_LONG_FIELD), null)
              .buildAndReset(),
            b.set(time, Time(pointTime))
              .set(metric(TestTableFields.TEST_LONG_FIELD), null)
              .buildAndReset()
          )
        )

      val rows = tsdb.query(query)

      val r1 = rows.next()
      r1.get[Long]("c") shouldBe 0
      r1.get[Long]("cd") shouldBe 0
      r1.get[Long]("ch") shouldBe 0

      rows.hasNext shouldBe false
  }

  it should "throwing exception on calling hll_count for metric decimal field with wrong type" in withTsdbMock {
    (_, _) =>
      val sql =
        "SELECT hll_count(testField, 0.01) as ch " +
          "FROM test_table " + timeBounds(and = false) + " GROUP BY day(time)"

      the[Exception] thrownBy createQuery(
        sql
      ) should have message "hll_count is not defined for given datatype: DOUBLE"
  }

  it should "throwing exception on calling hll_count for metric decimal field and std_err less then 0.00003" in withTsdbMock {
    (_, _) =>
      val sql =
        "SELECT hll_count(testLongField, 0.0000029) as ch " +
          "FROM test_table " + timeBounds(and = false) + " GROUP BY day(time)"

      the[Exception] thrownBy createQuery(
        sql
      ) should have message "std_err must be in range (0.00003, 0.367), but: std_err=0.0000029"
  }

  it should "throwing exception on calling hll_count for metric decimal field and std_err more then 0.367" in withTsdbMock {
    (_, _) =>
      val sql =
        "SELECT hll_count(testLongField, 0.3671) as ch " +
          "FROM test_table " + timeBounds(and = false) + " GROUP BY day(time)"

      the[Exception] thrownBy createQuery(
        sql
      ) should have message "std_err must be in range (0.00003, 0.367), but: std_err=0.3671"
  }

  it should "throwing exception on calling hll_count for metric decimal field" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql =
      "SELECT hll_count(testField, 0.01) as ch " +
        "FROM test_table " + timeBounds(and = false) + " GROUP BY day(time)"

    the[Exception] thrownBy createQuery(
      sql
    ) should have message "hll_count is not defined for given datatype: DOUBLE"
  }

  it should "calculate average for metric fields when evaluating each data row including null field values" in withTsdbMock {
    (tsdb, tsdbDaoMock) =>
      val sql =
        """SELECT 
          |avg(testField) avgDouble, 
          |avg(testLongField) avgLong, 
          |avg(testBigDecimalField) avgBigDecimal,
          |avg(testByteField) avgByte """.stripMargin +
          "FROM test_table " + timeBounds(and = false) + " GROUP BY day(time)"

      val query = createQuery(sql)

      val pointTime = from.toInstant.toEpochMilli + 10

      (tsdbDaoMock.query _)
        .expects(
          InternalQuery(
            TestSchema.testTable,
            Set(
              metric(TestTableFields.TEST_FIELD),
              metric(TestTableFields.TEST_LONG_FIELD),
              metric(TestTableFields.TEST_BIGDECIMAL_FIELD),
              metric(TestTableFields.TEST_BYTE_FIELD),
              time
            ),
            and(ge(time, const(Time(from))), lt(time, const(Time(to))))
          ),
          *,
          *
        )
        .onCall((_, b, _) =>
          Iterator(
            b.set(time, Time(pointTime))
              .set(metric(TestTableFields.TEST_FIELD), null)
              .set(metric(TestTableFields.TEST_LONG_FIELD), null)
              .set(metric(TestTableFields.TEST_BIGDECIMAL_FIELD), null)
              .set(metric(TestTableFields.TEST_BYTE_FIELD), null)
              .buildAndReset(),
            b.set(time, Time(pointTime))
              .set(metric(TestTableFields.TEST_FIELD), 0d)
              .set(metric(TestTableFields.TEST_LONG_FIELD), 1L)
              .set(metric(TestTableFields.TEST_BIGDECIMAL_FIELD), BigDecimal(10))
              .set(metric(TestTableFields.TEST_BYTE_FIELD), 10.toByte)
              .buildAndReset(),
            b.set(time, Time(pointTime))
              .set(metric(TestTableFields.TEST_FIELD), 10d)
              .set(metric(TestTableFields.TEST_LONG_FIELD), 11L)
              .set(metric(TestTableFields.TEST_BIGDECIMAL_FIELD), BigDecimal(101))
              .set(metric(TestTableFields.TEST_BYTE_FIELD), 101.toByte)
              .buildAndReset(),
            b.set(time, Time(pointTime))
              .set(metric(TestTableFields.TEST_FIELD), null)
              .set(metric(TestTableFields.TEST_LONG_FIELD), 2L)
              .set(metric(TestTableFields.TEST_BIGDECIMAL_FIELD), BigDecimal(20))
              .set(metric(TestTableFields.TEST_BYTE_FIELD), 20.toByte)
              .buildAndReset(),
            b.set(time, Time(pointTime))
              .set(metric(TestTableFields.TEST_FIELD), 6d)
              .set(metric(TestTableFields.TEST_LONG_FIELD), 5L)
              .set(metric(TestTableFields.TEST_BIGDECIMAL_FIELD), null)
              .set(metric(TestTableFields.TEST_BYTE_FIELD), null)
              .buildAndReset(),
            b.set(time, Time(pointTime))
              .set(metric(TestTableFields.TEST_FIELD), 5d)
              .set(metric(TestTableFields.TEST_LONG_FIELD), null)
              .set(metric(TestTableFields.TEST_BIGDECIMAL_FIELD), BigDecimal(7))
              .set(metric(TestTableFields.TEST_BYTE_FIELD), 7.toByte)
              .buildAndReset()
          )
        )

      val rows = tsdb.query(query)

      val r1 = rows.next()

      r1.get[BigDecimal]("avgDouble") shouldBe 5.25d
      r1.get[BigDecimal]("avgLong") shouldBe 4.75d
      r1.get[BigDecimal]("avgBigDecimal") shouldBe 34.5d
      r1.get[BigDecimal]("avgByte") shouldBe 34.5d

      rows.hasNext shouldBe false
  }

  it should "calculate average for dimension fields when evaluating each data row" in withTsdbMock {
    (tsdb, tsdbDaoMock) =>
      val sql =
        "SELECT avg(B) avgB, avg(Y) avgY  " +
          "FROM test_table_4 " + timeBounds(and = false) + " GROUP BY day(time)"

      val query = createQuery(sql)

      val pointTime = from.toInstant.toEpochMilli + 10

      (tsdbDaoMock.query _)
        .expects(
          InternalQuery(
            TestSchema.testTable4,
            Set(dimension(TestDims.DIM_B), dimension(TestDims.DIM_Y), time),
            and(ge(time, const(Time(from))), lt(time, const(Time(to))))
          ),
          *,
          *
        )
        .onCall((_, b, _) =>
          Iterator(
            b.set(time, Time(pointTime))
              .set(dimension(TestDims.DIM_B), 1: Short)
              .set(dimension(TestDims.DIM_Y), 1L)
              .buildAndReset(),
            b.set(time, Time(pointTime))
              .set(dimension(TestDims.DIM_B), 2: Short)
              .set(dimension(TestDims.DIM_Y), 1L)
              .buildAndReset(),
            b.set(time, Time(pointTime))
              .set(dimension(TestDims.DIM_B), 1: Short)
              .set(dimension(TestDims.DIM_Y), 2L)
              .buildAndReset(),
            b.set(time, Time(pointTime))
              .set(dimension(TestDims.DIM_B), 1: Short)
              .set(dimension(TestDims.DIM_Y), 1L)
              .buildAndReset()
          )
        )

      val rows = tsdb.query(query)

      val r1 = rows.next()
      r1.get[BigDecimal]("avgB") shouldBe 1.25
      r1.get[BigDecimal]("avgY") shouldBe 1.25

      rows.hasNext shouldBe false
  }

  it should "calculate average for fields when evaluating each data row and each field has null value" in withTsdbMock {
    (tsdb, tsdbDaoMock) =>
      val sql =
        "SELECT avg(testField) avgDouble, avg(testLongField) avgLong, avg(testBigDecimalField) avgBigDecimal " +
          "FROM test_table " + timeBounds(and = false) + " GROUP BY day(time)"

      val query = createQuery(sql)

      val pointTime = from.toInstant.toEpochMilli + 10

      (tsdbDaoMock.query _)
        .expects(
          InternalQuery(
            TestSchema.testTable,
            Set(
              metric(TestTableFields.TEST_FIELD),
              metric(TestTableFields.TEST_LONG_FIELD),
              metric(TestTableFields.TEST_BIGDECIMAL_FIELD),
              time
            ),
            and(ge(time, const(Time(from))), lt(time, const(Time(to))))
          ),
          *,
          *
        )
        .onCall((_, b, _) =>
          Iterator(
            b.set(time, Time(pointTime))
              .set(metric(TestTableFields.TEST_FIELD), null)
              .set(metric(TestTableFields.TEST_LONG_FIELD), null)
              .set(metric(TestTableFields.TEST_BIGDECIMAL_FIELD), null)
              .buildAndReset(),
            b.set(time, Time(pointTime))
              .set(metric(TestTableFields.TEST_FIELD), null)
              .set(metric(TestTableFields.TEST_LONG_FIELD), null)
              .set(metric(TestTableFields.TEST_BIGDECIMAL_FIELD), null)
              .buildAndReset()
          )
        )

      val rows = tsdb.query(query)

      val r1 = rows.next()

      r1.get[BigDecimal]("avgDouble") shouldBe null
      r1.get[BigDecimal]("avgLong") shouldBe null
      r1.get[BigDecimal]("avgBigDecimal") shouldBe null

      rows.hasNext shouldBe false
  }

  it should "execute query like this (be able to cast long to double)" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT testField + testLongField as plus2 " +
      "FROM test_table " + timeBounds(and = false)
    val query = createQuery(sql)

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression[_]](time, metric(TestTableFields.TEST_FIELD), metric(TestTableFields.TEST_LONG_FIELD)),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(metric(TestTableFields.TEST_FIELD), 1d)
            .set(metric(TestTableFields.TEST_LONG_FIELD), 3L)
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query)

    val r1 = rows.next()
    r1.get[Double]("plus2") shouldBe 4d

    rows.hasNext shouldBe false
  }

  it should "handle arithmetic in having" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT time, lag(time) as lag_time, A " +
      "FROM test_table " + timeBounds(and = false) +
      "HAVING (time - lag_time) >= INTERVAL '10' SECOND"
    val query = createQuery(sql)

    val pointTime = from.toInstant.toEpochMilli + 10
    val pointTime2 = pointTime + 10 * 1000

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression[_]](dimension(TestDims.DIM_A), time),
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

    val rows = tsdb.query(query)

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

    val pointTime = from.toInstant.toEpochMilli + 10
    val pointTime2 = pointTime + 10 * 1000
    val pointTime3 = pointTime + 10 * 2000

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression[_]](metric(TestTableFields.TEST_STRING_FIELD), time),
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

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head
    row.get[String]("operator") shouldBe "Blatov"
    row.get[String]("lag_operator") shouldBe "Mayorova"
  }

  it should "handle arithmetic with window functions" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sql = "SELECT testField, lag(testField), testField + lag(testField) as plus2 " +
      "FROM test_table " + timeBounds(and = false) + " HAVING lag(testField) IS NOT NULL "
    val query = createQuery(sql)

    val pointTime = from.toInstant.toEpochMilli + 10
    val pointTime2 = pointTime + 10 * 1000

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression[_]](metric(TestTableFields.TEST_FIELD), time),
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

    val rows = tsdb.query(query).toList

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

    val doubleLinkExpr = link[Double](TestLinks.TEST_LINK5, LinkField[Double]("testField5D"))

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression[_]](time, dimension(TestDims.DIM_B)),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(dimension(TestDims.DIM_B), 12).buildAndReset(),
          b.set(dimension(TestDims.DIM_B), 15).buildAndReset()
        )
      )

    (link5.setLinkedValues _)
      .expects(*, *, *)
      .onCall((idx, rs, _) => rs.foreach(r => r.set(idx, doubleLinkExpr, 15.23)))

    val rows = tsdb.query(query).toList

    val r1 = rows.head
    r1.get[Double]("plus5") shouldBe 20.23

    rows should have size 2
  }
}

package org.yupana.core

import java.util.Properties

import org.joda.time.format.DateTimeFormat
import org.joda.time.{ DateTime, DateTimeZone, LocalDateTime }
import org.scalatest._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.api.schema.{ Dimension, MetricValue }
import org.yupana.api.types._
import org.yupana.api.utils.SortedSetIterator
import org.yupana.core.cache.CacheFactory
import org.yupana.core.dao.{ DictionaryDao, DictionaryProviderImpl, TSDao, TsdbQueryMetricsDao }
import org.yupana.core.model._
import org.yupana.core.sql.SqlQueryProcessor
import org.yupana.core.sql.parser.{ Select, SqlParser }
import org.yupana.core.utils.SparseTable
import org.yupana.core.utils.metric.NoMetricCollector

trait TSTestDao extends TSDao[Iterator, Long]

class TsdbTest
    extends FlatSpec
    with Matchers
    with TsdbMocks
    with OptionValues
    with TableDrivenPropertyChecks
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  override protected def beforeAll(): Unit = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("app.properties"))
    CacheFactory.init(properties, "test")
  }

  override protected def beforeEach(): Unit = {
    CacheFactory.flushCaches()
  }

  import org.yupana.api.query.syntax.All._

  "TSDB" should "put datapoint to database" in {

    val tsdbDaoMock = mock[TSTestDao]
    val metricsDaoMock = mock[TsdbQueryMetricsDao]
    val dictionaryDaoMock = mock[DictionaryDao]
    val dictionaryProvider = new DictionaryProviderImpl(dictionaryDaoMock)
    val tsdb = new TSDB(tsdbDaoMock, metricsDaoMock, dictionaryProvider, identity, SimpleTsdbConfig(putEnabled = true))

    val time = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC).getMillis
    val dims = Map[Dimension, Any](TestDims.DIM_A -> "test1", TestDims.DIM_B -> "test2")
    val dp1 = DataPoint(TestSchema.testTable, time, dims, Seq(MetricValue(TestTableFields.TEST_FIELD, 1.0)))
    val dp2 = DataPoint(TestSchema.testTable, time + 1, dims, Seq(MetricValue(TestTableFields.TEST_FIELD, 1.0)))
    val dp3 =
      DataPoint(TestSchema.testTable2, time + 1, dims, Seq(MetricValue(TestTable2Fields.TEST_FIELD, BigDecimal(1))))

    (tsdbDaoMock.put _).expects(Seq(dp1, dp2, dp3))

    tsdb.put(Seq(dp1, dp2, dp3))
  }

  it should "not allow put if disabled" in {
    val tsdbDaoMock = mock[TSTestDao]
    val metricsDaoMock = mock[TsdbQueryMetricsDao]
    val dictionaryDaoMock = mock[DictionaryDao]
    val dictionaryProvider = new DictionaryProviderImpl(dictionaryDaoMock)
    val tsdb = new TSDB(tsdbDaoMock, metricsDaoMock, dictionaryProvider, identity, SimpleTsdbConfig())

    val dp = DataPoint(
      TestSchema.testTable,
      123456789L,
      Map(TestDims.DIM_A -> "test1", TestDims.DIM_B -> "test2"),
      Seq(MetricValue(TestTableFields.TEST_FIELD, 1.0))
    )

    an[IllegalAccessException] should be thrownBy tsdb.put(Seq(dp))
  }

  it should "execute query with filter by tags" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        time as "time_time",
        metric(TestTableFields.TEST_FIELD) as "testField",
        dimension(TestDims.DIM_A) as "A",
        dimension(TestDims.DIM_B) as "B"
      ),
      BinaryOperationExpr(BinaryOperation.equ[String], dimension(TestDims.DIM_A), const("test1"))
    )

    val pointTime = qtime.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set[Expression](
            time,
            metric(TestTableFields.TEST_FIELD),
            dimension(TestDims.DIM_A),
            dimension(TestDims.DIM_B)
          ),
          and(
            equ(dimension(TestDims.DIM_A), const("test1")),
            ge(time, const(Time(from))),
            lt(time, const(Time(to)))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime))
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), "test2")
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head

    row.fieldValueByName[Time]("time_time") shouldBe Time(pointTime)
    row.fieldValueByName[Double]("testField") shouldBe 1d
    row.fieldValueByName[String]("A") shouldBe "test1"
    row.fieldValueByName[String]("B") shouldBe "test2"
  }

  it should "execute query with filter by tag ids" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        time as "time_time",
        metric(TestTableFields.TEST_FIELD) as "testField",
        dimension(TestDims.DIM_A) as "A",
        dimension(TestDims.DIM_B) as "B"
      ),
      DimIdInExpr(TestDims.DIM_A, SortedSetIterator((123, 456L)))
    )

    val pointTime = qtime.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.DIM_A), dimension(TestDims.DIM_B)),
          and(
            DimIdInExpr(TestDims.DIM_A, SortedSetIterator((123, 456L))),
            ge(time, const(Time(from))),
            lt(time, const(Time(to)))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime))
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .set(dimension(TestDims.DIM_A), "test123")
            .set(dimension(TestDims.DIM_B), "test2")
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head

    row.fieldValueByName[Time]("time_time") shouldBe Time(pointTime)
    row.fieldValueByName[Double]("testField") shouldBe 1d
    row.fieldValueByName[String]("A") shouldBe "test123"
    row.fieldValueByName[String]("B") shouldBe "test2"
  }

  it should "execute query with filter by exact time values" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val pointTime = qtime.plusHours(2)

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        time as "time_time",
        metric(TestTableFields.TEST_FIELD) as "testField",
        dimension(TestDims.DIM_A) as "A"
      ),
      equ(time, const(Time(pointTime)))
    )

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.DIM_A)),
          and(
            equ(time, const(Time(pointTime))),
            ge(time, const(Time(from))),
            lt(time, const(Time(to)))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Some(Time(pointTime)))
            .set(metric(TestTableFields.TEST_FIELD), Some(3d))
            .set(dimension(TestDims.DIM_A), Some("test12"))
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head

    row.fieldValueByName[Time]("time_time") shouldBe Time(pointTime)
    row.fieldValueByName[Double]("testField") shouldBe 3d
    row.fieldValueByName[String]("A") shouldBe "test12"
  }

  it should "support filter by tuples" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val pointTime1 = qtime.plusMinutes(10)
    val pointTime2 = qtime.plusHours(2)

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        time as "time_time",
        metric(TestTableFields.TEST_FIELD) as "testField",
        dimension(TestDims.DIM_A) as "A"
      ),
      AndExpr(
        Seq(
          InExpr(tuple(time, dimension(TestDims.DIM_A)), Set((Time(pointTime2), "test42")))
        )
      )
    )

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.DIM_A)),
          and(
            in(tuple(time, dimension(TestDims.DIM_A)), Set((Time(pointTime2), "test42"))),
            ge(time, const(Time(from))),
            lt(time, const(Time(to)))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1))
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .set(dimension(TestDims.DIM_A), "test42")
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(metric(TestTableFields.TEST_FIELD), 3d)
            .set(dimension(TestDims.DIM_A), "test42")
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head

    row.fieldValueByName[Time]("time_time") shouldBe Time(pointTime2)
    row.fieldValueByName[Double]("testField") shouldBe 3d
    row.fieldValueByName[String]("A") shouldBe "test42"
  }

  it should "support exclude filter by tuples" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val pointTime1 = qtime.plusMinutes(10)
    val pointTime2 = qtime.plusHours(2)

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        time.toField,
        metric(TestTableFields.TEST_FIELD) as "testField",
        dimension(TestDims.DIM_A) as "A"
      ),
      AndExpr(
        Seq(
          equ(dimension(TestDims.DIM_B), const(52.toShort)),
          notIn(tuple(time, dimension(TestDims.DIM_A.aux)), Set((Time(pointTime2), "test42")))
        )
      )
    )

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.DIM_A)),
          and(
            notIn(tuple(time, dimension(TestDims.DIM_A)), Set((Time(pointTime2), "test42"))),
            equ(dimension(TestDims.DIM_B), const(52.toShort)),
            ge(time, const(Time(from))),
            lt(time, const(Time(to)))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test42")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test24")
            .set(metric(TestTableFields.TEST_FIELD), 2d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test42")
            .set(metric(TestTableFields.TEST_FIELD), 3d)
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query).toList.sortBy(_.fields.toList.map(_.toString).mkString(","))
    rows should have size 2

    val row1 = rows(0)
    row1.fieldValueByName[Time]("time") shouldBe Time(pointTime2)
    row1.fieldValueByName[Double]("testField") shouldBe 2d
    row1.fieldValueByName[String]("A") shouldBe "test24"

    val row2 = rows(1)
    row2.fieldValueByName[Time]("time") shouldBe Time(pointTime1)
    row2.fieldValueByName[Double]("testField") shouldBe 1d
    row2.fieldValueByName[String]("A") shouldBe "test42"
  }

  it should "support filter not equal for tags" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        time as "time_time",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        dimension(TestDims.DIM_A) as "A",
        dimension(TestDims.DIM_B) as "B"
      ),
      neq(dimension(TestDims.DIM_A), const("test11"))
    )

    val pointTime = qtime.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, dimension(TestDims.DIM_B), dimension(TestDims.DIM_A), metric(TestTableFields.TEST_FIELD)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            neq(dimension(TestDims.DIM_A), const("test11"))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime))
            .set(dimension(TestDims.DIM_A), "test12")
            .set(dimension(TestDims.DIM_B), "test2")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head

    row.fieldValueByName[Time]("time_time") shouldBe Time(pointTime)
    row.fieldValueByName[Double]("sum_testField") shouldBe 1d
    row.fieldValueByName[String]("A") shouldBe "test12"
    row.fieldValueByName[String]("B") shouldBe "test2"
  }

  it should "execute query" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        time as "time_time",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        dimension(TestDims.DIM_A) as "A",
        dimension(TestDims.DIM_B) as "B"
      )
    )

    val pointTime = qtime.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.DIM_A), dimension(TestDims.DIM_B)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to)))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), "test2")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset()
        )
      )

    val row = tsdb.query(query).head

    row.fieldValueByName[Time]("time_time") shouldBe Time(pointTime)
    row.fieldValueByName[Double]("sum_testField") shouldBe 1d
    row.fieldValueByName[String]("A") shouldBe "test1"
    row.fieldValueByName[String]("B") shouldBe "test2"
  }

  it should "execute query with downsampling" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        function(UnaryOperation.truncDay, time) as "time",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        dimension(TestDims.DIM_A) as "A",
        dimension(TestDims.DIM_B) as "B"
      ),
      None,
      Seq(function(UnaryOperation.truncDay, time), dimension(TestDims.DIM_A), dimension(TestDims.DIM_B))
    )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.DIM_A), dimension(TestDims.DIM_B)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to)))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), "test2")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), "test2")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset()
        )
      )

    val row = tsdb.query(query).head

    row.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    row.fieldValueByName[Double]("sum_testField") shouldBe 2d
    row.fieldValueByName[String]("A") shouldBe "test1"
    row.fieldValueByName[String]("B") shouldBe "test2"
  }

  it should "execute query with aggregation by tag" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        function(UnaryOperation.truncDay, time) as "time",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        dimension(TestDims.DIM_A) as "A"
      ),
      None,
      Seq(dimension(TestDims.DIM_A))
    )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.DIM_A)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to)))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test12")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test12")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset()
        )
      )

    val results = tsdb.query(query).toList.sortBy(_.fields.toList.map(_.toString).mkString(","))
    results should have size (2)

    val group1 = results(0)
    group1.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    group1.fieldValueByName[Double]("sum_testField") shouldBe 4d
    group1.fieldValueByName[String]("A") shouldBe "test1"

    val group2 = results(1)
    group2.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    group2.fieldValueByName[Double]("sum_testField") shouldBe 2d
    group2.fieldValueByName[String]("A") shouldBe "test12"
  }

  it should "execute query with aggregation by expression" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        metric(TestTableFields.TEST_FIELD) as "testField",
        function(UnaryOperation.truncDay, time) as "time",
        aggregate(Aggregation.count[String], dimension(TestDims.DIM_A)) as "A"
      ),
      None,
      Seq(metric(TestTableFields.TEST_FIELD))
    )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.DIM_A)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to)))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test12")
            .set(metric(TestTableFields.TEST_FIELD), 2d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test12")
            .set(metric(TestTableFields.TEST_FIELD), 2d)
            .buildAndReset()
        )
      )

    val results = tsdb.query(query).toList.sortBy(_.fields.toList.map(_.toString).mkString(","))

    results should have size 2

    val group1 = results(0)
    group1.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    group1.fieldValueByName[Double]("testField") shouldBe 1d
    group1.fieldValueByName[Int]("A") shouldBe 4

    val group2 = results(1)
    group2.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    group2.fieldValueByName[Double]("testField") shouldBe 2d
    group2.fieldValueByName[Int]("A") shouldBe 2
  }

  it should "execute query without aggregation (grouping) by key" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val qtime = new LocalDateTime(2017, 12, 18, 11, 26).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      filter = Some(
        AndExpr(
          Seq(
            ge(time, const(Time(from))),
            lt(time, const(Time(to)))
          )
        )
      ),
      groupBy = Seq(function(UnaryOperation.truncDay, time)),
      fields = Seq(
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField"
      ),
      limit = None,
      table = Some(TestSchema.testTable)
    )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to)))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1)).set(metric(TestTableFields.TEST_FIELD), 1d).buildAndReset(),
          b.set(time, Time(pointTime2)).set(metric(TestTableFields.TEST_FIELD), 1d).buildAndReset(),
          b.set(time, Time(pointTime1)).set(metric(TestTableFields.TEST_FIELD), 1d).buildAndReset(),
          b.set(time, Time(pointTime2)).set(metric(TestTableFields.TEST_FIELD), 1d).buildAndReset()
        )
      )

    val results = tsdb.query(query)

    val res = results.iterator.next()
    res.fieldValueByName[Double]("sum_testField") shouldBe 4d

    results.iterator.hasNext shouldBe false
  }

  it should "execute query with filter values by external link field" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK)

    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        function(UnaryOperation.truncDay, time) as "time",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        dimension(TestDims.DIM_A) as "A",
        dimension(TestDims.DIM_B) as "B",
        link(TestLinks.TEST_LINK, "testField") as "TestCatalog_testField"
      ),
      Some(equ(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue"))),
      Seq(
        function(UnaryOperation.truncDay, time),
        dimension(TestDims.DIM_A),
        dimension(TestDims.DIM_B),
        link(TestLinks.TEST_LINK, "testField")
      )
    )

    (testCatalogServiceMock.condition _)
      .expects(
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          equ(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue"))
        )
      )
      .returning(
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          in(dimension(TestDims.DIM_A), Set("test1", "test12"))
        )
      )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (testCatalogServiceMock.setLinkedValues _)
      .expects(*, *, Set(link(TestLinks.TEST_LINK, "testField")))
      .onCall((qc, datas, _) => {
        setCatalogValueByTag(
          qc,
          datas,
          TestLinks.TEST_LINK,
          SparseTable("test1" -> Map("testField" -> "testFieldValue"), "test12" -> Map("testField" -> "testFieldValue"))
        )
      })

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, dimension(TestDims.DIM_A), dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(dimension(TestDims.DIM_A), Set("test1", "test12"))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), "test2")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), "test2")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test12")
            .set(dimension(TestDims.DIM_B), "test2")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test12")
            .set(dimension(TestDims.DIM_B), "test2")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset()
        )
      )

    val results = tsdb.query(query).toList.sortBy(_.fields.toList.map(_.toString).mkString(","))

    val r1 = results(0)
    r1.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r1.fieldValueByName[Double]("sum_testField") shouldBe 2d
    r1.fieldValueByName[String]("A") shouldBe "test1"
    r1.fieldValueByName[String]("B") shouldBe "test2"
    r1.fieldValueByName[String]("TestCatalog_testField") shouldBe "testFieldValue"

    val r2 = results(1)
    r2.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r2.fieldValueByName[Double]("sum_testField") shouldBe 2d
    r2.fieldValueByName[String]("A") shouldBe "test12"
    r2.fieldValueByName[String]("B") shouldBe "test2"
    r2.fieldValueByName[String]("TestCatalog_testField") shouldBe "testFieldValue"
  }

  it should "execute query with filter values by external link field return empty result when linked values not found" in withTsdbMock {
    (tsdb, tsdbDaoMock) =>
      val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK)

      val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
      val from = qtime.getMillis
      val to = qtime.plusDays(1).getMillis

      val query = Query(
        TestSchema.testTable,
        const(Time(qtime)),
        const(Time(qtime.plusDays(1))),
        Seq(
          truncDay(time) as "time",
          aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
          dimension(TestDims.DIM_A) as "A",
          dimension(TestDims.DIM_B) as "B"
        ),
        Some(equ(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue"))),
        Seq(truncDay(time))
      )

      (testCatalogServiceMock.condition _)
        .expects(
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            equ(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue"))
          )
        )
        .returning(
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(dimension(TestDims.DIM_A), Set.empty)
          )
        )

      (tsdbDaoMock.query _)
        .expects(
          InternalQuery(
            TestSchema.testTable,
            Set(time, dimension(TestDims.DIM_A), dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD)),
            and(
              ge(time, const(Time(from))),
              lt(time, const(Time(to))),
              in(dimension(TestDims.DIM_A), Set())
            )
          ),
          *,
          NoMetricCollector
        )
        .returning(Iterator.empty)

      val result = tsdb.query(query)

      result shouldBe empty
  }

  it should "execute query with filter values by external link field return empty result when linked tag ids not found" in withTsdbMock {
    (tsdb, tsdbDaoMock) =>
      val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK)

      val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
      val from = qtime.getMillis
      val to = qtime.plusDays(1).getMillis

      val query = Query(
        TestSchema.testTable,
        const(Time(qtime)),
        const(Time(qtime.plusDays(1))),
        Seq(
          function(UnaryOperation.truncDay, time) as "time",
          aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
          dimension(TestDims.DIM_A) as "A",
          dimension(TestDims.DIM_B) as "B"
        ),
        Some(
          BinaryOperationExpr(
            BinaryOperation.equ[String],
            link(TestLinks.TEST_LINK, "testField"),
            const("testFieldValue")
          )
        ),
        Seq(function(UnaryOperation.truncDay, time))
      )

      (testCatalogServiceMock.condition _)
        .expects(
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            equ(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue"))
          )
        )
        .returning(
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            DimIdInExpr(TestDims.DIM_A, SortedSetIterator.empty[(Int, Long)])
          )
        )

      (tsdbDaoMock.query _)
        .expects(
          InternalQuery(
            TestSchema.testTable,
            Set(time, dimension(TestDims.DIM_A), dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD)),
            and(
              ge(time, const(Time(from))),
              lt(time, const(Time(to))),
              DimIdInExpr(TestDims.DIM_A, SortedSetIterator.empty[(Int, Long)])
            )
          ),
          *,
          NoMetricCollector
        )
        .returning(Iterator.empty)

      val result = tsdb.query(query)

      result shouldBe empty
  }

  it should "execute query with exclude filter by external link field" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK)

    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(from)),
      const(Time(to)),
      Seq(
        function(UnaryOperation.truncDay, time) as "time",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        dimension(TestDims.DIM_A) as "A",
        dimension(TestDims.DIM_B) as "B",
        link(TestLinks.TEST_LINK, "testField") as "TestCatalog_testField"
      ),
      Some(
        BinaryOperationExpr(
          BinaryOperation.neq[String],
          link(TestLinks.TEST_LINK, "testField"),
          const("testFieldValue")
        )
      ),
      Seq(
        function(UnaryOperation.truncDay, time),
        dimension(TestDims.DIM_A),
        dimension(TestDims.DIM_B),
        link(TestLinks.TEST_LINK, "testField")
      )
    )

    (testCatalogServiceMock.condition _)
      .expects(
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          neq(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue"))
        )
      )
      .returning(
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          NotInExpr(dimension(TestDims.DIM_A), Set("test11", "test12"))
        )
      )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (testCatalogServiceMock.setLinkedValues _)
      .expects(*, *, Set(link(TestLinks.TEST_LINK, "testField")))
      .onCall((qc, datas, _) => {
        setCatalogValueByTag(
          qc,
          datas,
          TestLinks.TEST_LINK,
          SparseTable("test13" -> Map("testField" -> "test value 3"))
        )
      })

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, dimension(TestDims.DIM_A), dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            notIn(dimension(TestDims.DIM_A), Set("test11", "test12"))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test13")
            .set(dimension(TestDims.DIM_B), "test21")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test13")
            .set(dimension(TestDims.DIM_B), "test21")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head

    row.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    row.fieldValueByName[Double]("sum_testField") shouldBe 2d
    row.fieldValueByName[String]("A") shouldBe "test13"
    row.fieldValueByName[String]("B") shouldBe "test21"
    row.fieldValueByName[String]("TestCatalog_testField") shouldBe "test value 3"
  }

  it should "execute query with exclude filter by external link field when link service return tag ids" in withTsdbMock {
    (tsdb, tsdbDaoMock) =>
      val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK)

      val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
      val from = qtime.getMillis
      val to = qtime.plusDays(1).getMillis

      val query = Query(
        TestSchema.testTable,
        const(Time(from)),
        const(Time(to)),
        Seq(
          function(UnaryOperation.truncDay, time) as "time",
          aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
          dimension(TestDims.DIM_A) as "A",
          dimension(TestDims.DIM_B) as "B",
          link(TestLinks.TEST_LINK, "testField") as "TestCatalog_testField"
        ),
        Some(neq(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue"))),
        Seq(
          function(UnaryOperation.truncDay, time),
          dimension(TestDims.DIM_A),
          dimension(TestDims.DIM_B),
          link(TestLinks.TEST_LINK, "testField")
        )
      )

      (testCatalogServiceMock.condition _)
        .expects(
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            neq(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue"))
          )
        )
        .returning(
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            DimIdNotInExpr(TestDims.DIM_A, SortedSetIterator((1, 1L), (2, 2L)))
          )
        )

      (testCatalogServiceMock.setLinkedValues _)
        .expects(*, *, Set(link(TestLinks.TEST_LINK, "testField")))
        .onCall((qc, datas, _) => {
          setCatalogValueByTag(
            qc,
            datas,
            TestLinks.TEST_LINK,
            SparseTable("test13" -> Map("testField" -> "test value 3"))
          )
        })

      val pointTime1 = qtime.getMillis + 10
      val pointTime2 = pointTime1 + 1

      (tsdbDaoMock.query _)
        .expects(
          InternalQuery(
            TestSchema.testTable,
            Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.DIM_A), dimension(TestDims.DIM_B)),
            and(
              ge(time, const(Time(from))),
              lt(time, const(Time(to))),
              DimIdNotInExpr(TestDims.DIM_A, SortedSetIterator((1, 1L), (2, 2L)))
            )
          ),
          *,
          NoMetricCollector
        )
        .onCall((_, b, _) =>
          Iterator(
            b.set(time, Time(pointTime1))
              .set(dimension(TestDims.DIM_A), "test13")
              .set(dimension(TestDims.DIM_B), "test21")
              .set(metric(TestTableFields.TEST_FIELD), 1d)
              .buildAndReset(),
            b.set(time, Time(pointTime2))
              .set(dimension(TestDims.DIM_A), "test13")
              .set(dimension(TestDims.DIM_B), "test21")
              .set(metric(TestTableFields.TEST_FIELD), 1d)
              .buildAndReset()
          )
        )

      val rows = tsdb.query(query).toList
      rows should have size 1
      val row = rows.head

      row.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
      row.fieldValueByName[Double]("sum_testField") shouldBe 2d
      row.fieldValueByName[String]("A") shouldBe "test13"
      row.fieldValueByName[String]("B") shouldBe "test21"
      row.fieldValueByName[String]("TestCatalog_testField") shouldBe "test value 3"
  }

  it should "exclude tag ids from external link filter then they are in FilterNeq" in withTsdbMock {
    (tsdb, tsdbDaoMock) =>
      val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK)
      val testCatalog2ServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK2)

      val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
      val from = qtime.getMillis
      val to = qtime.plusDays(1).getMillis

      val query = Query(
        TestSchema.testTable,
        const(Time(from)),
        const(Time(to)),
        Seq(
          function(UnaryOperation.truncDay, time) as "time",
          sum(metric(TestTableFields.TEST_FIELD)) as "sum_testField",
          dimension(TestDims.DIM_A).toField,
          dimension(TestDims.DIM_B).toField,
          link(TestLinks.TEST_LINK, "testField") as "TestCatalog_testField"
        ),
        Some(
          AndExpr(
            Seq(
              neq(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue")),
              equ(link(TestLinks.TEST_LINK2, "testField2"), const("testFieldValue2"))
            )
          )
        ),
        Seq(
          function(UnaryOperation.truncDay, time),
          dimension(TestDims.DIM_A),
          dimension(TestDims.DIM_B),
          link(TestLinks.TEST_LINK, "testField")
        )
      )

      (testCatalogServiceMock.condition _)
        .expects(
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            neq(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue")),
            equ(link(TestLinks.TEST_LINK2, "testField2"), const("testFieldValue2"))
          )
        )
        .returning(
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            notIn(dimension(TestDims.DIM_A), Set("test11", "test12")),
            equ(link(TestLinks.TEST_LINK2, "testField2"), const("testFieldValue2"))
          )
        )

      (testCatalog2ServiceMock.condition _)
        .expects(
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            neq(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue")),
            equ(link(TestLinks.TEST_LINK2, "testField2"), const("testFieldValue2"))
          )
        )
        .returning(
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            neq(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue")),
            in(dimension(TestDims.DIM_A), Set("test12", "test13"))
          )
        )

      (testCatalogServiceMock.setLinkedValues _)
        .expects(*, *, Set(link(TestLinks.TEST_LINK, "testField")))
        .onCall((qc, datas, _) => {
          setCatalogValueByTag(
            qc,
            datas,
            TestLinks.TEST_LINK,
            SparseTable("test13" -> Map("testField" -> "test value 3"))
          )
        })

      val pointTime1 = qtime.getMillis + 10
      val pointTime2 = pointTime1 + 1

      (tsdbDaoMock.query _)
        .expects(
          InternalQuery(
            TestSchema.testTable,
            Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.DIM_A), dimension(TestDims.DIM_B)),
            and(
              ge(time, const(Time(from))),
              lt(time, const(Time(to))),
              in(dimension(TestDims.DIM_A), Set("test12", "test13")),
              notIn(dimension(TestDims.DIM_A), Set("test11", "test12"))
            )
          ),
          *,
          NoMetricCollector
        )
        .onCall((_, b, _) =>
          Iterator(
            b.set(time, Time(pointTime1))
              .set(dimension(TestDims.DIM_A), "test13")
              .set(dimension(TestDims.DIM_B), "test21")
              .set(metric(TestTableFields.TEST_FIELD), 1d)
              .buildAndReset(),
            b.set(time, Time(pointTime2))
              .set(dimension(TestDims.DIM_A), "test13")
              .set(dimension(TestDims.DIM_B), "test21")
              .set(metric(TestTableFields.TEST_FIELD), 1d)
              .buildAndReset()
          )
        )

      val rows = tsdb.query(query).toList
      rows should have size 1
      val row = rows.head

      row.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
      row.fieldValueByName[Double]("sum_testField") shouldBe 2d
      row.fieldValueByName[String]("A") shouldBe "test13"
      row.fieldValueByName[String]("B") shouldBe "test21"
      row.fieldValueByName[String]("TestCatalog_testField") shouldBe "test value 3"
  }

  it should "handle not equal filters with both tags and external link fields" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK3)

    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        time as "time",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        dimension(TestDims.DIM_A) as "A",
        dimension(TestDims.DIM_B) as "B"
      ),
      Some(
        AndExpr(
          Seq(
            neq(dimension(TestDims.DIM_A), const("test11")),
            neq(link(TestLinks.TEST_LINK3, "testField3-1"), const("aaa")),
            neq(link(TestLinks.TEST_LINK3, "testField3-1"), const("bbb")),
            neq(link(TestLinks.TEST_LINK3, "testField3-2"), const("ccc"))
          )
        )
      ),
      Seq.empty
    )

    (testCatalogServiceMock.condition _)
      .expects(
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          neq(dimension(TestDims.DIM_A), const("test11")),
          neq(link(TestLinks.TEST_LINK3, "testField3-1"), const("aaa")),
          neq(link(TestLinks.TEST_LINK3, "testField3-1"), const("bbb")),
          neq(link(TestLinks.TEST_LINK3, "testField3-2"), const("ccc"))
        )
      )
      .returning(
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          neq(dimension(TestDims.DIM_A), const("test11")),
          notIn(dimension(TestDims.DIM_A), Set("test11", "test12")),
          notIn(dimension(TestDims.DIM_A), Set("test13")),
          notIn(dimension(TestDims.DIM_A), Set("test11", "test14"))
        )
      )

    val pointTime = qtime.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, dimension(TestDims.DIM_A), dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            notIn(dimension(TestDims.DIM_A), Set("test11", "test12")),
            notIn(dimension(TestDims.DIM_A), Set("test13")),
            notIn(dimension(TestDims.DIM_A), Set("test11", "test14")),
            neq(dimension(TestDims.DIM_A), const("test11"))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime))
            .set(dimension(TestDims.DIM_A), "test15")
            .set(dimension(TestDims.DIM_B), "test22")
            .set(metric(TestTableFields.TEST_FIELD), 5d)
            .buildAndReset()
        )
      )

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head

    row.fieldValueByName[Time]("time") shouldBe Time(pointTime)
    row.fieldValueByName[Double]("sum_testField") shouldBe 5d
    row.fieldValueByName[String]("A") shouldBe "test15"
    row.fieldValueByName[String]("B") shouldBe "test22"
  }

  it should "intersect tag ids with one tag for query with filter values by catalogs fields" in withTsdbMock {
    (tsdb, tsdbDaoMock) =>
      val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK)
      val testCatalog2ServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK2)

      val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
      val from = qtime.getMillis
      val to = qtime.plusDays(1).getMillis

      val query = Query(
        TestSchema.testTable,
        const(Time(qtime)),
        const(Time(qtime.plusDays(1))),
        Seq(
          function(UnaryOperation.truncDay, time) as "time",
          aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
          dimension(TestDims.DIM_A) as "A",
          dimension(TestDims.DIM_B) as "B"
        ),
        Some(
          AndExpr(
            Seq(
              equ(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue")),
              equ(link(TestLinks.TEST_LINK2, "testField2"), const("testFieldValue2"))
            )
          )
        ),
        Seq(function(UnaryOperation.truncDay, time), dimension(TestDims.DIM_A), dimension(TestDims.DIM_B))
      )

      (testCatalogServiceMock.condition _)
        .expects(
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            equ(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue")),
            equ(link(TestLinks.TEST_LINK2, "testField2"), const("testFieldValue2"))
          )
        )
        .returning(
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(dimension(TestDims.DIM_A), Set("test11", "test12")),
            equ(link(TestLinks.TEST_LINK2, "testField2"), const("testFieldValue2"))
          )
        )
      (testCatalog2ServiceMock.condition _)
        .expects(
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            equ(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue")),
            equ(link(TestLinks.TEST_LINK2, "testField2"), const("testFieldValue2"))
          )
        )
        .returning(
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            equ(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue")),
            in(dimension(TestDims.DIM_A), Set("test12"))
          )
        )

      val pointTime1 = qtime.getMillis + 10
      val pointTime2 = pointTime1 + 1

      (tsdbDaoMock.query _)
        .expects(
          InternalQuery(
            TestSchema.testTable,
            Set(time, dimension(TestDims.DIM_A), dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD)),
            and(
              ge(time, const(Time(from))),
              lt(time, const(Time(to))),
              in(dimension(TestDims.DIM_A), Set("test11", "test12")),
              in(dimension(TestDims.DIM_A), Set("test12"))
            )
          ),
          *,
          NoMetricCollector
        )
        .onCall((_, b, _) =>
          Iterator(
            b.set(time, Time(pointTime1))
              .set(dimension(TestDims.DIM_A), "test12")
              .set(dimension(TestDims.DIM_B), "test2")
              .set(metric(TestTableFields.TEST_FIELD), 1d)
              .buildAndReset(),
            b.set(time, Time(pointTime2))
              .set(dimension(TestDims.DIM_A), "test12")
              .set(dimension(TestDims.DIM_B), "test2")
              .set(metric(TestTableFields.TEST_FIELD), 1d)
              .buildAndReset()
          )
        )

      val result = tsdb.query(query).toList
      val r1 = result.head
      r1.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
      r1.fieldValueByName[Double]("sum_testField") shouldBe 2d
      r1.fieldValueByName[String]("A") shouldBe "test12"
      r1.fieldValueByName[String]("B") shouldBe "test2"
      result should have size 1
  }

  it should "intersect catalogs by different tags" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK)
    val testCatalog4ServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK4)

    val qtime = new LocalDateTime(2018, 7, 20, 11, 49).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        function(UnaryOperation.truncDay, time) as "time",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        dimension(TestDims.DIM_A) as "A",
        dimension(TestDims.DIM_B) as "B"
      ),
      Some(
        AndExpr(
          Seq(
            equ(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue")),
            equ(link(TestLinks.TEST_LINK4, "testField4"), const("testFieldValue2"))
          )
        )
      ),
      Seq(dimension(TestDims.DIM_A), dimension(TestDims.DIM_B), function(UnaryOperation.truncDay, time))
    )

    (testCatalogServiceMock.condition _)
      .expects(
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          equ(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue")),
          equ(link(TestLinks.TEST_LINK4, "testField4"), const("testFieldValue2"))
        )
      )
      .returning(
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          in(dimension(TestDims.DIM_A), Set("test11", "test12")),
          equ(link(TestLinks.TEST_LINK4, "testField4"), const("testFieldValue2"))
        )
      )
    (testCatalog4ServiceMock.condition _)
      .expects(
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          equ(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue")),
          equ(link(TestLinks.TEST_LINK4, "testField4"), const("testFieldValue2"))
        )
      )
      .returning(
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          equ(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue")),
          in(dimension(TestDims.DIM_B), Set(23.toShort, 24.toShort))
        )
      )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, dimension(TestDims.DIM_A), dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(dimension(TestDims.DIM_A), Set("test11", "test12")),
            in(dimension(TestDims.DIM_B), Set(23.toShort, 24.toShort))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test12")
            .set(dimension(TestDims.DIM_B), 23.toShort)
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test12")
            .set(dimension(TestDims.DIM_B), 23.toShort)
            .set(metric(TestTableFields.TEST_FIELD), 5d)
            .buildAndReset()
        )
      )

    val result = tsdb.query(query).toList
    val r1 = result.head
    r1.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r1.fieldValueByName[Double]("sum_testField") shouldBe 6d
    r1.fieldValueByName[String]("A") shouldBe "test12"
    r1.fieldValueByName[Short]("B") shouldBe 23.toShort
    result should have size 1
  }

  it should "support IN for catalogs" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK)

    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        time as "time",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        dimension(TestDims.DIM_A) as "A",
        dimension(TestDims.DIM_B) as "B"
      ),
      Some(
        in(link(TestLinks.TEST_LINK, "testField"), Set("testFieldValue1", "testFieldValue2"))
      ),
      Seq.empty
    )

    (testCatalogServiceMock.condition _)
      .expects(
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          in(link(TestLinks.TEST_LINK, "testField"), Set("testFieldValue1", "testFieldValue2"))
        )
      )
      .returning(
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          in(dimension(TestDims.DIM_A), Set("Test a 1", "Test a 2", "Test a 3"))
        )
      )

    val pointTime = qtime.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, dimension(TestDims.DIM_A), dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(dimension(TestDims.DIM_A), Set("Test a 1", "Test a 2", "Test a 3"))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime))
            .set(dimension(TestDims.DIM_A), "Test a 1")
            .set(dimension(TestDims.DIM_B), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 2d)
            .buildAndReset(),
          b.set(time, Time(pointTime))
            .set(dimension(TestDims.DIM_A), "Test a 3")
            .set(dimension(TestDims.DIM_B), "test2")
            .set(metric(TestTableFields.TEST_FIELD), 3d)
            .buildAndReset()
        )
      )

    val rs = tsdb.query(query).toList.sortBy(_.fields.toList.map(_.toString).mkString(","))

    rs should have size (2)

    val r1 = rs(0)

    r1.fieldValueByName[Time]("time") shouldBe Time(pointTime)
    r1.fieldValueByName[Double]("sum_testField") shouldBe 2d
    r1.fieldValueByName[String]("A") shouldBe "Test a 1"
    r1.fieldValueByName[String]("B") shouldBe "test1"

    val r2 = rs(1)

    r2.fieldValueByName[Time]("time") shouldBe Time(pointTime)
    r2.fieldValueByName[Double]("sum_testField") shouldBe 3d
    r2.fieldValueByName[String]("A") shouldBe "Test a 3"
    r2.fieldValueByName[String]("B") shouldBe "test2"
  }

  it should "intersect values for IN filter for tags and catalogs" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK)

    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        time as "time",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        dimension(TestDims.DIM_A) as "A",
        dimension(TestDims.DIM_B) as "B"
      ),
      Some(
        AndExpr(
          Seq(
            InExpr(dimension(TestDims.DIM_B), Set(1.toShort, 2.toShort)),
            InExpr(link(TestLinks.TEST_LINK, "testField"), Set("testFieldValue1", "testFieldValue2"))
          )
        )
      ),
      Seq.empty
    )

    (testCatalogServiceMock.condition _)
      .expects(
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          in(dimension(TestDims.DIM_B), Set(1.toShort, 2.toShort)),
          in(link(TestLinks.TEST_LINK, "testField"), Set("testFieldValue1", "testFieldValue2"))
        )
      )
      .returning(
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          in(dimension(TestDims.DIM_B), Set(1.toShort, 2.toShort)),
          in(dimension(TestDims.DIM_A), Set("A 1", "A 2", "A 3"))
        )
      )

    val pointTime = qtime.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, dimension(TestDims.DIM_A), dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(dimension(TestDims.DIM_A), Set("A 1", "A 2", "A 3")),
            in(dimension(TestDims.DIM_B), Set(1.toShort, 2.toShort))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime))
            .set(dimension(TestDims.DIM_A), "A 1")
            .set(dimension(TestDims.DIM_B), 1.toShort)
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime))
            .set(dimension(TestDims.DIM_A), "A 2")
            .set(dimension(TestDims.DIM_B), 1.toShort)
            .set(metric(TestTableFields.TEST_FIELD), 3d)
            .buildAndReset(),
          b.set(time, Time(pointTime))
            .set(dimension(TestDims.DIM_A), "A 2")
            .set(dimension(TestDims.DIM_B), 2.toShort)
            .set(metric(TestTableFields.TEST_FIELD), 4d)
            .buildAndReset(),
          b.set(time, Time(pointTime))
            .set(dimension(TestDims.DIM_A), "A 3")
            .set(dimension(TestDims.DIM_B), 2.toShort)
            .set(metric(TestTableFields.TEST_FIELD), 6d)
            .buildAndReset()
        )
      )

    val rs = tsdb.query(query).toList.sortBy(_.fields.toList.map(_.toString).mkString(","))

    rs should have size (4)

    val r1 = rs(0)

    r1.fieldValueByName[Time]("time") shouldBe Time(pointTime)
    r1.fieldValueByName[Double]("sum_testField") shouldBe 1d
    r1.fieldValueByName[String]("A") shouldBe "A 1"
    r1.fieldValueByName[Short]("B") shouldBe 1.toShort

    val r2 = rs(1)

    r2.fieldValueByName[Time]("time") shouldBe Time(pointTime)
    r2.fieldValueByName[Double]("sum_testField") shouldBe 3d
    r2.fieldValueByName[String]("A") shouldBe "A 2"
    r2.fieldValueByName[Short]("B") shouldBe 1.toShort

    val r3 = rs(2)

    r3.fieldValueByName[Time]("time") shouldBe Time(pointTime)
    r3.fieldValueByName[Double]("sum_testField") shouldBe 4d
    r3.fieldValueByName[String]("A") shouldBe "A 2"
    r3.fieldValueByName[Short]("B") shouldBe 2.toShort

    val r4 = rs(3)

    r4.fieldValueByName[Time]("time") shouldBe Time(pointTime)
    r4.fieldValueByName[Double]("sum_testField") shouldBe 6d
    r4.fieldValueByName[String]("A") shouldBe "A 3"
    r4.fieldValueByName[Short]("B") shouldBe 2.toShort

  }

  it should "execute query with group values by external link field" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mock[ExternalLinkService[TestLinks.TestLink]]
    tsdb.registerExternalLink(TestLinks.TEST_LINK, testCatalogServiceMock)

    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        function(UnaryOperation.truncDay, time) as "time",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        dimension(TestDims.DIM_A) as "A",
        link(TestLinks.TEST_LINK, "testField") as "TestCatalog_testField"
      ),
      None,
      Seq(function(UnaryOperation.truncDay, time), link(TestLinks.TEST_LINK, "testField"))
    )

    (testCatalogServiceMock.setLinkedValues _)
      .expects(*, *, Set(link(TestLinks.TEST_LINK, "testField")))
      .onCall((qc, datas, _) => {
        setCatalogValueByTag(
          qc,
          datas,
          TestLinks.TEST_LINK,
          SparseTable(
            Map(
              "test1" -> Map("testField" -> "testFieldValue1"),
              "test12" -> Map("testField" -> "testFieldValue1"),
              "test13" -> Map("testField" -> "testFieldValue2")
            )
          )
        )
      })

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.DIM_A)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to)))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test12")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test12")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test13")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test13")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset()
        )
      )

    val results = tsdb.query(query).toList.sortBy(_.fields.toList.map(_.toString).mkString(","))

    val r1 = results(0)
    r1.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r1.fieldValueByName[Double]("sum_testField") shouldBe 4d
    r1.fieldValueByName[String]("TestCatalog_testField") shouldBe "testFieldValue1"

    val r2 = results(1)
    r2.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r2.fieldValueByName[Double]("sum_testField") shouldBe 2d
    r2.fieldValueByName[String]("TestCatalog_testField") shouldBe "testFieldValue2"
  }

  it should "execute query with aggregate functions on string field" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query1 = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        function(UnaryOperation.truncDay, time) as "time",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        aggregate(Aggregation.min[String], TestTableFields.TEST_STRING_FIELD) as "min_testStringField"
      ),
      None,
      Seq(function(UnaryOperation.truncDay, time), dimension(TestDims.DIM_A))
    )

    val pointTime1 = qtime.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(
            time,
            metric(TestTableFields.TEST_FIELD),
            metric(TestTableFields.TEST_STRING_FIELD),
            dimension(TestDims.DIM_A)
          ),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .set(metric(TestTableFields.TEST_STRING_FIELD), "001_01_1")
            .buildAndReset(),
          b.set(time, Time(pointTime1 + 1))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .set(metric(TestTableFields.TEST_STRING_FIELD), "001_01_2")
            .buildAndReset(),
          b.set(time, Time(pointTime1 + 2))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .set(metric(TestTableFields.TEST_STRING_FIELD), "001_01_200")
            .buildAndReset(),
          b.set(time, Time(pointTime1 + 3))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .set(metric(TestTableFields.TEST_STRING_FIELD), "001_02_1")
            .buildAndReset()
        )
      )
      .repeated(3)

    val startDay = Time(qtime.withMillisOfDay(0).getMillis)

    val r1 = tsdb.query(query1).head
    r1.fieldValueByName[Time]("time") shouldBe startDay
    r1.fieldValueByName[Double]("sum_testField") shouldBe 4d
    r1.fieldValueByName[String]("min_testStringField") shouldBe "001_01_1"

    val query2 = query1.copy(
      fields = Seq(
        function(UnaryOperation.truncDay, time) as "time",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        aggregate(Aggregation.max[String], TestTableFields.TEST_STRING_FIELD) as "max_testStringField"
      )
    )

    val r2 = tsdb.query(query2).head
    r2.fieldValueByName[Time]("time") shouldBe startDay
    r2.fieldValueByName[Double]("sum_testField") shouldBe 4d
    r2.fieldValueByName[String]("max_testStringField") shouldBe "001_02_1"

    val query3 = query1.copy(
      fields = Seq(
        function(UnaryOperation.truncDay, time) as "time",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        aggregate(Aggregation.count[String], TestTableFields.TEST_STRING_FIELD) as "count_testStringField"
      )
    )

    val r3 = tsdb.query(query3).head
    r3.fieldValueByName[Time]("time") shouldBe startDay
    r3.fieldValueByName[Double]("sum_testField") shouldBe 4d
    r3.fieldValueByName[Long]("count_testStringField") shouldBe 4L
  }

  it should "handle the same values for different grouping fields" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mock[ExternalLinkService[TestLinks.TestLink]]
    tsdb.registerExternalLink(TestLinks.TEST_LINK3, testCatalogServiceMock)

    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        function(UnaryOperation.truncDay, time) as "time",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        link(TestLinks.TEST_LINK3, "testField3-1") as "TestCatalog3_testField3-1",
        link(TestLinks.TEST_LINK3, "testField3-2") as "TestCatalog3_testField3-2",
        link(TestLinks.TEST_LINK3, "testField3-3") as "TestCatalog3_testField3-3"
      ),
      None,
      Seq(
        function(UnaryOperation.truncDay, time),
        link(TestLinks.TEST_LINK3, "testField3-1"),
        link(TestLinks.TEST_LINK3, "testField3-2"),
        link(TestLinks.TEST_LINK3, "testField3-3")
      )
    )

    (testCatalogServiceMock.setLinkedValues _)
      .expects(
        *,
        *,
        Set(
          link(TestLinks.TEST_LINK3, "testField3-1"),
          link(TestLinks.TEST_LINK3, "testField3-2"),
          link(TestLinks.TEST_LINK3, "testField3-3")
        )
      )
      .onCall((qc, datas, _) => {
        setCatalogValueByTag(
          qc,
          datas,
          TestLinks.TEST_LINK3,
          SparseTable(
            Map(
              "testA1" -> Map("testField3-1" -> "Value1", "testField3-2" -> "Value1", "testField3-3" -> "Value2"),
              "testA2" -> Map("testField3-1" -> "Value1", "testField3-2" -> "Value2", "testField3-3" -> "Value2")
            )
          )
        )
      })

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.DIM_A)),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "testA1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "testA1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "testA2")
            .set(metric(TestTableFields.TEST_FIELD), 2d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "testA2")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset()
        )
      )

    val rs = tsdb.query(query).toList.sortBy(_.fields.toList.map(_.toString).mkString(","))

    rs should have size 2

    val r1 = rs(0)
    r1.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r1.fieldValueByName[Double]("sum_testField") shouldBe 2d
    r1.fieldValueByName[String]("TestCatalog3_testField3-1") shouldBe "Value1"
    r1.fieldValueByName[String]("TestCatalog3_testField3-2") shouldBe "Value1"
    r1.fieldValueByName[String]("TestCatalog3_testField3-3") shouldBe "Value2"

    val r2 = rs(1)
    r2.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r2.fieldValueByName[Double]("sum_testField") shouldBe 3d
    r2.fieldValueByName[String]("TestCatalog3_testField3-1") shouldBe "Value1"
    r2.fieldValueByName[String]("TestCatalog3_testField3-2") shouldBe "Value2"
    r2.fieldValueByName[String]("TestCatalog3_testField3-3") shouldBe "Value2"
  }

  it should "calculate min and max time" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        function(UnaryOperation.truncDay, time) as "time",
        aggregate(Aggregation.min[Time], time) as "min_time",
        aggregate(Aggregation.max[Time], time) as "max_time",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        dimension(TestDims.DIM_A) as "A",
        dimension(TestDims.DIM_B) as "B"
      ),
      None,
      Seq(function(UnaryOperation.truncDay, time), dimension(TestDims.DIM_A), dimension(TestDims.DIM_B))
    )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 5
    val pointTime3 = pointTime1 + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.DIM_A), dimension(TestDims.DIM_B)),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), "test2")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), "test2")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime3))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), "test2")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset()
        )
      )

    val r = tsdb.query(query).head
    r.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r.fieldValueByName[Time]("min_time") shouldBe Time(pointTime1)
    r.fieldValueByName[Time]("max_time") shouldBe Time(pointTime3)
    r.fieldValueByName[Double]("sum_testField") shouldBe 3d
    r.fieldValueByName[String]("A") shouldBe "test1"
    r.fieldValueByName[String]("B") shouldBe "test2"
  }

  it should "preserve const fields" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        const(BigDecimal(1)) as "dummy",
        function(UnaryOperation.truncDay, time) as "time",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        dimension(TestDims.DIM_A) as "A",
        dimension(TestDims.DIM_B) as "B"
      ),
      None,
      Seq(function(UnaryOperation.truncDay, time), dimension(TestDims.DIM_A), dimension(TestDims.DIM_B))
    )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.DIM_A), dimension(TestDims.DIM_B)),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), "test2")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), "test2")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset()
        )
      )

    val row = tsdb.query(query).head

    row.fieldValueByName[BigDecimal]("dummy") shouldEqual BigDecimal(1)
    row.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    row.fieldValueByName[Double]("sum_testField") shouldBe 2d
    row.fieldValueByName[String]("A") shouldBe "test1"
    row.fieldValueByName[String]("B") shouldBe "test2"
  }

  it should "be possible to make aggregations by tags" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        function(UnaryOperation.truncDay, time) as "time",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        aggregate(Aggregation.count[String], dimension(TestDims.DIM_A)) as "count_A",
        dimension(TestDims.DIM_B) as "B"
      ),
      None,
      Seq(function(UnaryOperation.truncDay, time), dimension(TestDims.DIM_B))
    )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.DIM_A), dimension(TestDims.DIM_B)),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), "test2")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(dimension(TestDims.DIM_B), "test2")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset()
        )
      )

    val row = tsdb.query(query).head

    row.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    row.fieldValueByName[Double]("sum_testField") shouldBe 2d
    row.fieldValueByName[Long]("count_A") shouldBe 2L
    row.fieldValueByName[String]("B") shouldBe "test2"
  }

  it should "be possible to make aggregations on catalogs" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mockCatalogService(tsdb, TestLinks.TEST_LINK)

    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        function(UnaryOperation.truncDay, time) as "time",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        dimension(TestDims.DIM_A) as "A",
        aggregate(Aggregation.count[String], link(TestLinks.TEST_LINK, "testField")) as "count_TestCatalog_testField"
      ),
      Some(equ(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue"))),
      Seq(function(UnaryOperation.truncDay, time), dimension(TestDims.DIM_A))
    )

    (testCatalogServiceMock.condition _)
      .expects(
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          equ(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue"))
        )
      )
      .returning(
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          in(dimension(TestDims.DIM_A), Set("test1", "test12"))
        )
      )

    (testCatalogServiceMock.setLinkedValues _)
      .expects(*, *, Set(link(TestLinks.TEST_LINK, "testField")))
      .onCall((qc, datas, _) => {
        setCatalogValueByTag(
          qc,
          datas,
          TestLinks.TEST_LINK,
          SparseTable(
            Map("test1" -> Map("testField" -> "testFieldValue"), "test12" -> Map("testField" -> "testFieldValue"))
          )
        )
      })

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.DIM_A)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(dimension(TestDims.DIM_A), Set("test1", "test12"))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test12")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test12")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset()
        )
      )

    val results = tsdb.query(query).toList.sortBy(_.fields.toList.map(_.toString).mkString(","))
    results should have size 2

    val r1 = results(0)
    r1.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r1.fieldValueByName[Double]("sum_testField") shouldBe 2d
    r1.fieldValueByName[String]("A") shouldBe "test1"
    r1.fieldValueByName[Long]("count_TestCatalog_testField") shouldBe 2L

    val r2 = results(1)
    r2.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r2.fieldValueByName[Double]("sum_testField") shouldBe 2d
    r2.fieldValueByName[String]("A") shouldBe "test12"
    r2.fieldValueByName[Long]("count_TestCatalog_testField") shouldBe 2L
  }

  it should "calculate distinct count" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        function(UnaryOperation.truncDay, time) as "time",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        aggregate(Aggregation.distinctCount[String], dimension(TestDims.DIM_A)) as "distinct_count_A",
        aggregate(Aggregation.count[String], dimension(TestDims.DIM_A)) as "count_A",
        dimension(TestDims.DIM_B) as "B"
      ),
      None,
      Seq(function(UnaryOperation.truncDay, time), dimension(TestDims.DIM_B))
    )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.DIM_A), dimension(TestDims.DIM_B)),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "testA1")
            .set(dimension(TestDims.DIM_B), "testB2")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "testA1")
            .set(dimension(TestDims.DIM_B), "testB2")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "testA2")
            .set(dimension(TestDims.DIM_B), "testB1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "testA2")
            .set(dimension(TestDims.DIM_B), "testB1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "testA1")
            .set(dimension(TestDims.DIM_B), "testB1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "testA1")
            .set(dimension(TestDims.DIM_B), "testB1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset()
        )
      )

    val results = tsdb.query(query).toList.sortBy(_.fields.toList.map(_.toString).mkString(","))

    results should have size (2)

    val r1 = results(0)
    r1.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r1.fieldValueByName[Double]("sum_testField") shouldBe 2d
    r1.fieldValueByName[Long]("count_A") shouldBe 2L
    r1.fieldValueByName[Int]("distinct_count_A") shouldBe 1
    r1.fieldValueByName[String]("B") shouldBe "testB2"

    val r2 = results(1)
    r2.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r2.fieldValueByName[Double]("sum_testField") shouldBe 4d
    r2.fieldValueByName[Long]("count_A") shouldBe 4L
    r2.fieldValueByName[Int]("distinct_count_A") shouldBe 2
    r2.fieldValueByName[String]("B") shouldBe "testB1"
  }

  it should "calculate lag" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      Some(TestSchema.testTable),
      Seq(
        time as "time_time",
        windowFunction(WindowOperation.lag[Time], time) as "lag_time_time",
        metric(TestTableFields.TEST_FIELD) as "testField",
        dimension(TestDims.DIM_A) as "A",
        dimension(TestDims.DIM_B) as "B"
      ),
      Some(
        AndExpr(
          Seq(
            BinaryOperationExpr(BinaryOperation.ge[Time], time, const(Time(qtime))),
            BinaryOperationExpr(BinaryOperation.lt[Time], time, const(Time(qtime.plusDays(1))))
          )
        )
      ),
      Seq(dimension(TestDims.DIM_B)),
      None
    )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.DIM_A), dimension(TestDims.DIM_B)),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "testA1")
            .set(dimension(TestDims.DIM_B), "testB2")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "testA1")
            .set(dimension(TestDims.DIM_B), "testB2")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "testA2")
            .set(dimension(TestDims.DIM_B), "testB1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "testA2")
            .set(dimension(TestDims.DIM_B), "testB1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "testA1")
            .set(dimension(TestDims.DIM_B), "testB1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2 + 1000))
            .set(dimension(TestDims.DIM_A), "testA1")
            .set(dimension(TestDims.DIM_B), "testB1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset()
        )
      )

    val t = Table(
      ("time_time", "lag_time_time", "testField", "A", "B"),
      (qtime.toLocalDateTime, None, 1d, "testA1", "testB2"),
      (qtime.toLocalDateTime, Some(qtime.toLocalDateTime), 1d, "testA1", "testB2"),
      (qtime.toLocalDateTime, None, 1d, "testA2", "testB1"),
      (qtime.toLocalDateTime, Some(qtime.toLocalDateTime), 1d, "testA2", "testB1"),
      (qtime.toLocalDateTime, Some(qtime.toLocalDateTime), 1d, "testA1", "testB1"),
      (qtime.toLocalDateTime.plusSeconds(1), Some(qtime.toLocalDateTime), 1d, "testA1", "testB1")
    )
    val results = tsdb.query(query).iterator

    forAll(t) { (time, lagTime, testField, tagA, tagB) =>
      val r = results.next()

      r.fieldValueByName[Time]("time_time").toLocalDateTime.withMillisOfSecond(0) shouldBe time
      r.fieldValueByName[Time]("lag_time_time").toLocalDateTime.withMillisOfSecond(0) shouldBe lagTime
      r.fieldValueByName[Double]("testField") shouldBe testField
      r.fieldValueByName[String]("A") shouldBe tagA
      r.fieldValueByName[String]("B") shouldBe tagB
    }
  }

  it should "calculate conditional expressions" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        function(UnaryOperation.truncDay, time) as "time",
        sum(
          condition(
            and(
              ge(double2bigDecimal(metric(TestTableFields.TEST_FIELD)), const(BigDecimal(10))),
              le(metric(TestTableFields.TEST_FIELD), const(20d))
            ),
            const[BigDecimal](1),
            const[BigDecimal](0)
          )
        ) as "between_10_20",
        dimension(TestDims.DIM_A) as "A"
      ),
      None,
      Seq(function(UnaryOperation.truncDay, time), dimension(TestDims.DIM_A))
    )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.DIM_A)),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 10d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 15d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset()
        )
      )

    val results = tsdb.query(query).iterator

    val group1 = results.next()
    group1.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    group1.fieldValueByName[BigDecimal]("between_10_20") shouldBe BigDecimal(2)
    group1.fieldValueByName[String]("A") shouldBe "test1"
  }

  it should "calculate conditional expressions with empty external link values" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mock[ExternalLinkService[TestLinks.TestLink]]
    tsdb.registerExternalLink(TestLinks.TEST_LINK, testCatalogServiceMock)

    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        function(UnaryOperation.truncDay, time) as "time",
        aggregate(
          Aggregation.sum[BigDecimal],
          condition(
            equ(link(TestLinks.TEST_LINK, "testField"), const("sdfsafsdagf")),
            const[BigDecimal](1),
            const[BigDecimal](0)
          )
        ) as "between_10_20",
        dimension(TestDims.DIM_A) as "A"
      ),
      None,
      Seq(function(UnaryOperation.truncDay, time), dimension(TestDims.DIM_A))
    )

    (testCatalogServiceMock.setLinkedValues _)
      .expects(*, *, Set(link(TestLinks.TEST_LINK, "testField")))
      .onCall((qc, datas, _) => {
        setCatalogValueByTag(qc, datas, TestLinks.TEST_LINK, SparseTable.empty)
      })

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, dimension(TestDims.DIM_A)),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1)).set(dimension(TestDims.DIM_A), "test1").buildAndReset(),
          b.set(time, Time(pointTime2)).set(dimension(TestDims.DIM_A), "test1").buildAndReset(),
          b.set(time, Time(pointTime1)).set(dimension(TestDims.DIM_A), "test1").buildAndReset(),
          b.set(time, Time(pointTime2)).set(dimension(TestDims.DIM_A), "test1").buildAndReset(),
          b.set(time, Time(pointTime1)).set(dimension(TestDims.DIM_A), "test1").buildAndReset(),
          b.set(time, Time(pointTime2)).set(dimension(TestDims.DIM_A), "test1").buildAndReset()
        )
      )

    val results = tsdb.query(query).iterator

    val group1 = results.next()
    group1.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    group1.fieldValueByName[BigDecimal]("between_10_20") shouldBe BigDecimal(0)
    group1.fieldValueByName[String]("A") shouldBe "test1"
  }

  it should "perform post filtering" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        function(UnaryOperation.truncDay, time) as "time",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        dimension(TestDims.DIM_A) as "A"
      ),
      None,
      Seq(function(UnaryOperation.truncDay, time), dimension(TestDims.DIM_A)),
      None,
      Some(ge(sum(metric(TestTableFields.TEST_FIELD)), const[Double](3d)))
    )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.DIM_A)),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test12")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test12")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset()
        )
      )

    val results = tsdb.query(query).toList
    results should have size 1

    val r = results.head
    r.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r.fieldValueByName[Double]("sum_testField") shouldBe 4d
    r.fieldValueByName[String]("A") shouldBe "test1"
  }

  it should "handle if external link doesn't return value" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val testCatalogServiceMock = mock[ExternalLinkService[TestLinks.TestLink]]
    tsdb.registerExternalLink(TestLinks.TEST_LINK, testCatalogServiceMock)

    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        metric(TestTableFields.TEST_FIELD) as "testField",
        dimension(TestDims.DIM_A) as "A",
        link(TestLinks.TEST_LINK, "testField") as "TestCatalog_testField"
      )
    )

    (testCatalogServiceMock.setLinkedValues _)
      .expects(*, *, Set(link(TestLinks.TEST_LINK, "testField")))
      .onCall((qc, datas, _) => {
        setCatalogValueByTag(
          qc,
          datas,
          TestLinks.TEST_LINK,
          SparseTable(Map("test1" -> Map("testField" -> "testFieldValue")))
        )
      })

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.DIM_A)),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 1d)
            .buildAndReset(),
          b.set(time, Time(pointTime1))
            .set(dimension(TestDims.DIM_A), "test1")
            .set(metric(TestTableFields.TEST_FIELD), 2d)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(dimension(TestDims.DIM_A), "test2")
            .set(metric(TestTableFields.TEST_FIELD), 3d)
            .buildAndReset()
        )
      )

    val results = tsdb.query(query).toList.sortBy(_.fields.toList.map(_.toString).mkString(","))

    results should have size (3)

    val r1 = results(0)
    r1.fieldValueByName[Double]("testField") shouldBe 1d
    r1.fieldValueByName[String]("A") shouldBe "test1"
    r1.fieldValueByName[String]("TestCatalog_testField") shouldBe "testFieldValue"

    val r2 = results(1)
    r2.fieldValueByName[Double]("testField") shouldBe 2d
    r2.fieldValueByName[String]("A") shouldBe "test1"
    r2.fieldValueByName[String]("TestCatalog_testField") shouldBe "testFieldValue"

    val r3 = results(2)
    r3.fieldValueByName[Double]("testField") shouldBe 3d
    r3.fieldValueByName[String]("A") shouldBe "test2"
    r3.fieldValueByName[String]("TestCatalog_testField") shouldBe empty
  }

  it should "handle queries like this" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val sqlQueryProcessor = new SqlQueryProcessor(TestSchema.schema)
    val format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val from: DateTime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val to: DateTime = from.plusDays(1)

    val testCatalogServiceMock = mock[ExternalLinkService[TestLinks.TestLink]]
    tsdb.registerExternalLink(TestLinks.TEST_LINK2, testCatalogServiceMock)

    val sql = s"SELECT sum(CASE WHEN A = '2' THEN 1 ELSE 0) AS salesTicketsCount, day(time) AS d FROM test_table " +
      s"WHERE time >= TIMESTAMP '${from.toString(format)}' AND time < TIMESTAMP '${to.toString(format)}' GROUP BY d"

    val query = SqlParser.parse(sql).right.flatMap {
      case s: Select => sqlQueryProcessor.createQuery(s)
      case x         => Left(s"SELECT statement expected, but got $x")
    } match {
      case Right(q) => q
      case Left(e)  => fail(e)
    }

    val pointTime1 = from.getMillis + 10
    val pointTime2 = from.getMillis + 100

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, dimension(TestDims.DIM_A)),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime2)).set(dimension(TestDims.DIM_A), "1").buildAndReset(),
          b.set(time, Time(pointTime1)).set(dimension(TestDims.DIM_A), "1").buildAndReset(),
          b.set(time, Time(pointTime1)).set(dimension(TestDims.DIM_A), "2").buildAndReset()
        )
      )

    val results = tsdb.query(query).toList
    results should have size 1

    val r1 = results.head
    r1.fieldValueByName[Double]("salesTicketsCount") shouldBe 1
  }

  it should "support queries without tables" in withTsdbMock { (tsdb, _) =>
    val res = tsdb
      .query(
        Query(
          None,
          Seq(minus(const(10), const(3)) as "seven"),
          None
        )
      )
      .toList

    res should have size 1
    res.head.fieldValueByName[BigDecimal]("seven") shouldEqual BigDecimal(7)
  }

  it should "be able to filter without table" in withTsdbMock { (tsdb, _) =>
    val res = tsdb
      .query(
        Query(
          None,
          Seq(minus(const(10), const(3)) as "seven"),
          Some(le(minus(const(10), const(3)), const(5)))
        )
      )

    res shouldBe empty
  }

  it should "handle None aggregate results" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusDays(1))),
      Seq(
        function(UnaryOperation.truncDay, time) as "time",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        aggregate(Aggregation.count[Double], TestTableFields.TEST_FIELD) as "count_testField",
        aggregate(Aggregation.distinctCount[Double], TestTableFields.TEST_FIELD) as "distinct_count_testField"
      ),
      None,
      Seq(function(UnaryOperation.truncDay, time))
    )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to)))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(time, Time(pointTime1))
            .set(metric(TestTableFields.TEST_FIELD), None)
            .buildAndReset(),
          b.set(time, Time(pointTime2))
            .set(metric(TestTableFields.TEST_FIELD), None)
            .buildAndReset()
        )
      )

    val row = tsdb.query(query).head

    row.fieldValueByName[Time]("time") shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    row.fieldValueByName[Double]("sum_testField") shouldBe 0
    row.fieldValueByName[Double]("count_testField") shouldBe 0
    row.fieldValueByName[Double]("distinct_count_testField") shouldBe 0
  }
}

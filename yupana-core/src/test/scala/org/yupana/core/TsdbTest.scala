package org.yupana.core

import java.util.Properties

import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone, LocalDateTime}
import org.scalatest._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.api.schema.MetricValue
import org.yupana.api.types._
import org.yupana.api.utils.SortedSetIterator
import org.yupana.core.cache.CacheFactory
import org.yupana.core.dao.{DictionaryDao, DictionaryProviderImpl, TSDao, TsdbQueryMetricsDao}
import org.yupana.core.model._
import org.yupana.core.sql.SqlQueryProcessor
import org.yupana.core.sql.parser.{Select, SqlParser}
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
    val tsdb = new TSDB(tsdbDaoMock, metricsDaoMock, dictionaryProvider, identity)

    val time = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC).getMillis
    val tags = Map(TestDims.TAG_A -> "test1", TestDims.TAG_B -> "test2")
    val dp1 = DataPoint(TestSchema.testTable, time, tags, Seq(MetricValue(TestTableFields.TEST_FIELD, 1.0)))
    val dp2 = DataPoint(TestSchema.testTable, time + 1, tags, Seq(MetricValue(TestTableFields.TEST_FIELD, 1.0)))
    val dp3 =
      DataPoint(TestSchema.testTable2, time + 1, tags, Seq(MetricValue(TestTable2Fields.TEST_FIELD, BigDecimal(1))))

    (dictionaryDaoMock.getIdsByValues _).expects(TestDims.TAG_A, Set("test1")).returning(Map("test1" -> 1L))
    (dictionaryDaoMock.getIdsByValues _).expects(TestDims.TAG_B, Set("test2")).returning(Map("test2" -> 2L))
    (tsdbDaoMock.put _).expects(Seq(dp1, dp2, dp3))

    tsdb.put(Seq(dp1, dp2, dp3))
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
        dimension(TestDims.TAG_A) as "TAG_A",
        dimension(TestDims.TAG_B) as "TAG_B"
      ),
      SimpleCondition(BinaryOperationExpr(BinaryOperation.equ[String], dimension(TestDims.TAG_A), const("test1")))
    )

    val pointTime = qtime.getMillis + 10

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
            equ(dimension(TestDims.TAG_A), const("test1")),
            ge(time, const(Time(from))),
            lt(time, const(Time(to)))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime)))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(dimension(TestDims.TAG_B), Some("test2"))
              .buildAndReset()
          )
      )

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head

    row.fieldValueByName[Time]("time_time").value shouldBe Time(pointTime)
    row.fieldValueByName[Double]("testField").value shouldBe 1d
    row.fieldValueByName[String]("TAG_A").value shouldBe "test1"
    row.fieldValueByName[String]("TAG_B").value shouldBe "test2"
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
        dimension(TestDims.TAG_A) as "TAG_A",
        dimension(TestDims.TAG_B) as "TAG_B"
      ),
      DimIdIn(dimension(TestDims.TAG_A), SortedSetIterator(123))
    )

    val pointTime = qtime.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.TAG_A), dimension(TestDims.TAG_B)),
          and(
            DimIdIn(dimension(TestDims.TAG_A), SortedSetIterator(123)),
            ge(time, const(Time(from))),
            lt(time, const(Time(to)))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime)))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .set(dimension(TestDims.TAG_A), Some("test123"))
              .set(dimension(TestDims.TAG_B), Some("test2"))
              .buildAndReset()
          )
      )

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head

    row.fieldValueByName[Time]("time_time").value shouldBe Time(pointTime)
    row.fieldValueByName[Double]("testField").value shouldBe 1d
    row.fieldValueByName[String]("TAG_A").value shouldBe "test123"
    row.fieldValueByName[String]("TAG_B").value shouldBe "test2"
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
        dimension(TestDims.TAG_A) as "TAG_A"
      ),
      equ(time, const(Time(pointTime)))
    )

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.TAG_A)),
          and(
            equ(time, const(Time(pointTime))),
            ge(time, const(Time(from))),
            lt(time, const(Time(to)))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime)))
              .set(metric(TestTableFields.TEST_FIELD), Some(3d))
              .set(dimension(TestDims.TAG_A), Some("test12"))
              .buildAndReset()
          )
      )

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head

    row.fieldValueByName[Time]("time_time").value shouldBe Time(pointTime)
    row.fieldValueByName[Double]("testField").value shouldBe 3d
    row.fieldValueByName[String]("TAG_A").value shouldBe "test12"
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
        dimension(TestDims.TAG_A) as "TAG_A"
      ),
      And(
        Seq(
          In(tuple(time, dimension(TestDims.TAG_A)), Set((Time(pointTime2), "test42")))
        )
      )
    )

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.TAG_A)),
          and(
            in(tuple(time, dimension(TestDims.TAG_A)), Set((Time(pointTime2), "test42"))),
            ge(time, const(Time(from))),
            lt(time, const(Time(to)))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime1)))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .set(dimension(TestDims.TAG_A), Some("test42"))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(metric(TestTableFields.TEST_FIELD), Some(3d))
              .set(dimension(TestDims.TAG_A), Some("test42"))
              .buildAndReset()
          )
      )

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head

    row.fieldValueByName[Time]("time_time").value shouldBe Time(pointTime2)
    row.fieldValueByName[Double]("testField").value shouldBe 3d
    row.fieldValueByName[String]("TAG_A").value shouldBe "test42"
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
        dimension(TestDims.TAG_A) as "TAG_A"
      ),
      And(
        Seq(
          equ(dimension(TestDims.TAG_B), const("B-52")),
          notIn(tuple(time, dimension(TestDims.TAG_A)), Set((Time(pointTime2), "test42")))
        )
      )
    )

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.TAG_A)),
          and(
            notIn(tuple(time, dimension(TestDims.TAG_A)), Set((Time(pointTime2), "test42"))),
            equ(dimension(TestDims.TAG_B), const("B-52")),
            ge(time, const(Time(from))),
            lt(time, const(Time(to)))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test42"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test24"))
              .set(metric(TestTableFields.TEST_FIELD), Some(2d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test42"))
              .set(metric(TestTableFields.TEST_FIELD), Some(3d))
              .buildAndReset()
          )
      )

    val rows = tsdb.query(query).toList
    rows should have size 2

    val row = rows.head
    row.fieldValueByName[Time]("time").value shouldBe Time(pointTime1)
    row.fieldValueByName[Double]("testField").value shouldBe 1d
    row.fieldValueByName[String]("TAG_A").value shouldBe "test42"

    val row2 = rows(1)
    row2.fieldValueByName[Time]("time").value shouldBe Time(pointTime2)
    row2.fieldValueByName[Double]("testField").value shouldBe 2d
    row2.fieldValueByName[String]("TAG_A").value shouldBe "test24"
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
        dimension(TestDims.TAG_A) as "TAG_A",
        dimension(TestDims.TAG_B) as "TAG_B"
      ),
      neq(dimension(TestDims.TAG_A), const("test11"))
    )

    val pointTime = qtime.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, dimension(TestDims.TAG_B), dimension(TestDims.TAG_A), metric(TestTableFields.TEST_FIELD)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            neq(dimension(TestDims.TAG_A), const("test11"))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime)))
              .set(dimension(TestDims.TAG_A), Some("test12"))
              .set(dimension(TestDims.TAG_B), Some("test2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset()
          )
      )

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head

    row.fieldValueByName[Time]("time_time").value shouldBe Time(pointTime)
    row.fieldValueByName[Double]("sum_testField").value shouldBe 1d
    row.fieldValueByName[String]("TAG_A").value shouldBe "test12"
    row.fieldValueByName[String]("TAG_B").value shouldBe "test2"
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
        dimension(TestDims.TAG_A) as "TAG_A",
        dimension(TestDims.TAG_B) as "TAG_B"
      )
    )

    val pointTime = qtime.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.TAG_A), dimension(TestDims.TAG_B)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to)))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(dimension(TestDims.TAG_B), Some("test2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset()
          )
      )

    val row = tsdb.query(query).head

    row.fieldValueByName[Time]("time_time").value shouldBe Time(pointTime)
    row.fieldValueByName[Double]("sum_testField").value shouldBe 1d
    row.fieldValueByName[String]("TAG_A").value shouldBe "test1"
    row.fieldValueByName[String]("TAG_B").value shouldBe "test2"
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
        dimension(TestDims.TAG_A) as "TAG_A",
        dimension(TestDims.TAG_B) as "TAG_B"
      ),
      None,
      Seq(function(UnaryOperation.truncDay, time), dimension(TestDims.TAG_A), dimension(TestDims.TAG_B))
    )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.TAG_A), dimension(TestDims.TAG_B)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to)))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(dimension(TestDims.TAG_B), Some("test2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(dimension(TestDims.TAG_B), Some("test2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset()
          )
      )

    val row = tsdb.query(query).head

    row.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    row.fieldValueByName[Double]("sum_testField").value shouldBe 2d
    row.fieldValueByName[String]("TAG_A").value shouldBe "test1"
    row.fieldValueByName[String]("TAG_B").value shouldBe "test2"
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
        dimension(TestDims.TAG_A) as "TAG_A"
      ),
      None,
      Seq(dimension(TestDims.TAG_A))
    )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.TAG_A)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to)))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test12"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test12"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset()
          )
      )

    val results = tsdb.query(query).iterator

    val group1 = results.next()
    group1.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    group1.fieldValueByName[Double]("sum_testField").value shouldBe 2d
    group1.fieldValueByName[String]("TAG_A").value shouldBe "test12"

    val group2 = results.next()
    group2.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    group2.fieldValueByName[Double]("sum_testField").value shouldBe 4d
    group2.fieldValueByName[String]("TAG_A").value shouldBe "test1"
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
        aggregate(Aggregation.count[String], dimension(TestDims.TAG_A)) as "TAG_A"
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
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.TAG_A)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to)))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test12"))
              .set(metric(TestTableFields.TEST_FIELD), Some(2d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test12"))
              .set(metric(TestTableFields.TEST_FIELD), Some(2d))
              .buildAndReset()
          )
      )

    val results = tsdb.query(query).iterator

    val group1 = results.next()
    group1.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    group1.fieldValueByName[Double]("testField").value shouldBe 1d
    group1.fieldValueByName[Int]("TAG_A").value shouldBe 4

    val group2 = results.next()
    group2.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    group2.fieldValueByName[Double]("testField").value shouldBe 2d
    group2.fieldValueByName[Int]("TAG_A").value shouldBe 2
  }

  it should "execute query without aggregation (grouping) by key" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val qtime = new LocalDateTime(2017, 12, 18, 11, 26).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      filter = And(
        Seq(
          ge(time, const(Time(from))),
          lt(time, const(Time(to)))
        )
      ),
      groupBy = Seq(function(UnaryOperation.truncDay, time)),
      fields = Seq(
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField"
      ),
      limit = None,
      table = TestSchema.testTable
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
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime1))).set(metric(TestTableFields.TEST_FIELD), Some(1d)).buildAndReset(),
            b.set(time, Some(Time(pointTime2))).set(metric(TestTableFields.TEST_FIELD), Some(1d)).buildAndReset(),
            b.set(time, Some(Time(pointTime1))).set(metric(TestTableFields.TEST_FIELD), Some(1d)).buildAndReset(),
            b.set(time, Some(Time(pointTime2))).set(metric(TestTableFields.TEST_FIELD), Some(1d)).buildAndReset()
          )
      )

    val results = tsdb.query(query)

    val res = results.iterator.next()
    res.fieldValueByName[Double]("sum_testField").value shouldBe 4d

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
        dimension(TestDims.TAG_A) as "TAG_A",
        dimension(TestDims.TAG_B) as "TAG_B",
        link(TestLinks.TEST_LINK, "testField") as "TestCatalog_testField"
      ),
      Some(equ(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue"))),
      Seq(
        function(UnaryOperation.truncDay, time),
        dimension(TestDims.TAG_A),
        dimension(TestDims.TAG_B),
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
          in(dimension(TestDims.TAG_A), Set("test1", "test12"))
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
          Set(time, dimension(TestDims.TAG_A), dimension(TestDims.TAG_B), metric(TestTableFields.TEST_FIELD)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(dimension(TestDims.TAG_A), Set("test1", "test12"))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(dimension(TestDims.TAG_B), Some("test2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(dimension(TestDims.TAG_B), Some("test2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test12"))
              .set(dimension(TestDims.TAG_B), Some("test2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test12"))
              .set(dimension(TestDims.TAG_B), Some("test2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset()
          )
      )

    val results = tsdb.query(query).iterator

    val r1 = results.next()
    r1.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r1.fieldValueByName[Double]("sum_testField").value shouldBe 2d
    r1.fieldValueByName[String]("TAG_A").value shouldBe "test1"
    r1.fieldValueByName[String]("TAG_B").value shouldBe "test2"
    r1.fieldValueByName[String]("TestCatalog_testField").value shouldBe "testFieldValue"

    val r2 = results.next()
    r2.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r2.fieldValueByName[Double]("sum_testField").value shouldBe 2d
    r2.fieldValueByName[String]("TAG_A").value shouldBe "test12"
    r2.fieldValueByName[String]("TAG_B").value shouldBe "test2"
    r2.fieldValueByName[String]("TestCatalog_testField").value shouldBe "testFieldValue"
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
          dimension(TestDims.TAG_A) as "TAG_A",
          dimension(TestDims.TAG_B) as "TAG_B"
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
            in(dimension(TestDims.TAG_A), Set.empty)
          )
        )

      (tsdbDaoMock.query _)
        .expects(
          InternalQuery(
            TestSchema.testTable,
            Set(time, dimension(TestDims.TAG_A), dimension(TestDims.TAG_B), metric(TestTableFields.TEST_FIELD)),
            and(
              ge(time, const(Time(from))),
              lt(time, const(Time(to))),
              in(dimension(TestDims.TAG_A), Set())
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
          dimension(TestDims.TAG_A) as "TAG_A",
          dimension(TestDims.TAG_B) as "TAG_B"
        ),
        Some(
          SimpleCondition(
            BinaryOperationExpr(
              BinaryOperation.equ[String],
              link(TestLinks.TEST_LINK, "testField"),
              const("testFieldValue")
            )
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
            DimIdIn(dimension(TestDims.TAG_A), SortedSetIterator.empty)
          )
        )

      (tsdbDaoMock.query _)
        .expects(
          InternalQuery(
            TestSchema.testTable,
            Set(time, dimension(TestDims.TAG_A), dimension(TestDims.TAG_B), metric(TestTableFields.TEST_FIELD)),
            and(
              ge(time, const(Time(from))),
              lt(time, const(Time(to))),
              DimIdIn(dimension(TestDims.TAG_A), SortedSetIterator.empty)
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
        dimension(TestDims.TAG_A) as "TAG_A",
        dimension(TestDims.TAG_B) as "TAG_B",
        link(TestLinks.TEST_LINK, "testField") as "TestCatalog_testField"
      ),
      Some(
        SimpleCondition(
          BinaryOperationExpr(
            BinaryOperation.neq[String],
            link(TestLinks.TEST_LINK, "testField"),
            const("testFieldValue")
          )
        )
      ),
      Seq(
        function(UnaryOperation.truncDay, time),
        dimension(TestDims.TAG_A),
        dimension(TestDims.TAG_B),
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
          NotIn(dimension(TestDims.TAG_A), Set("test11", "test12"))
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
          Set(time, dimension(TestDims.TAG_A), dimension(TestDims.TAG_B), metric(TestTableFields.TEST_FIELD)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            notIn(dimension(TestDims.TAG_A), Set("test11", "test12"))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test13"))
              .set(dimension(TestDims.TAG_B), Some("test21"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test13"))
              .set(dimension(TestDims.TAG_B), Some("test21"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset()
          )
      )

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head

    row.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    row.fieldValueByName[Double]("sum_testField").value shouldBe 2d
    row.fieldValueByName[String]("TAG_A").value shouldBe "test13"
    row.fieldValueByName[String]("TAG_B").value shouldBe "test21"
    row.fieldValueByName[String]("TestCatalog_testField").value shouldBe "test value 3"
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
          dimension(TestDims.TAG_A) as "TAG_A",
          dimension(TestDims.TAG_B) as "TAG_B",
          link(TestLinks.TEST_LINK, "testField") as "TestCatalog_testField"
        ),
        Some(neq(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue"))),
        Seq(
          function(UnaryOperation.truncDay, time),
          dimension(TestDims.TAG_A),
          dimension(TestDims.TAG_B),
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
            DimIdNotIn(dimension(TestDims.TAG_A), SortedSetIterator(1, 2))
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
            Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.TAG_A), dimension(TestDims.TAG_B)),
            and(
              ge(time, const(Time(from))),
              lt(time, const(Time(to))),
              DimIdNotIn(dimension(TestDims.TAG_A), SortedSetIterator(1, 2))
            )
          ),
          *,
          NoMetricCollector
        )
        .onCall(
          (_, b, _) =>
            Iterator(
              b.set(time, Some(Time(pointTime1)))
                .set(dimension(TestDims.TAG_A), Some("test13"))
                .set(dimension(TestDims.TAG_B), Some("test21"))
                .set(metric(TestTableFields.TEST_FIELD), Some(1d))
                .buildAndReset(),
              b.set(time, Some(Time(pointTime2)))
                .set(dimension(TestDims.TAG_A), Some("test13"))
                .set(dimension(TestDims.TAG_B), Some("test21"))
                .set(metric(TestTableFields.TEST_FIELD), Some(1d))
                .buildAndReset()
            )
        )

      val rows = tsdb.query(query).toList
      rows should have size 1
      val row = rows.head

      row.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
      row.fieldValueByName[Double]("sum_testField").value shouldBe 2d
      row.fieldValueByName[String]("TAG_A").value shouldBe "test13"
      row.fieldValueByName[String]("TAG_B").value shouldBe "test21"
      row.fieldValueByName[String]("TestCatalog_testField").value shouldBe "test value 3"
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
          dimension(TestDims.TAG_A).toField,
          dimension(TestDims.TAG_B).toField,
          link(TestLinks.TEST_LINK, "testField") as "TestCatalog_testField"
        ),
        Some(
          And(
            Seq(
              neq(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue")),
              equ(link(TestLinks.TEST_LINK2, "testField2"), const("testFieldValue2"))
            )
          )
        ),
        Seq(
          function(UnaryOperation.truncDay, time),
          dimension(TestDims.TAG_A),
          dimension(TestDims.TAG_B),
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
            notIn(dimension(TestDims.TAG_A), Set("test11", "test12")),
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
            in(dimension(TestDims.TAG_A), Set("test12", "test13"))
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
            Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.TAG_A), dimension(TestDims.TAG_B)),
            and(
              ge(time, const(Time(from))),
              lt(time, const(Time(to))),
              in(dimension(TestDims.TAG_A), Set("test12", "test13")),
              notIn(dimension(TestDims.TAG_A), Set("test11", "test12"))
            )
          ),
          *,
          NoMetricCollector
        )
        .onCall(
          (_, b, _) =>
            Iterator(
              b.set(time, Some(Time(pointTime1)))
                .set(dimension(TestDims.TAG_A), Some("test13"))
                .set(dimension(TestDims.TAG_B), Some("test21"))
                .set(metric(TestTableFields.TEST_FIELD), Some(1d))
                .buildAndReset(),
              b.set(time, Some(Time(pointTime2)))
                .set(dimension(TestDims.TAG_A), Some("test13"))
                .set(dimension(TestDims.TAG_B), Some("test21"))
                .set(metric(TestTableFields.TEST_FIELD), Some(1d))
                .buildAndReset()
            )
        )

      val rows = tsdb.query(query).toList
      rows should have size 1
      val row = rows.head

      row.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
      row.fieldValueByName[Double]("sum_testField").value shouldBe 2d
      row.fieldValueByName[String]("TAG_A").value shouldBe "test13"
      row.fieldValueByName[String]("TAG_B").value shouldBe "test21"
      row.fieldValueByName[String]("TestCatalog_testField").value shouldBe "test value 3"
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
        dimension(TestDims.TAG_A) as "TAG_A",
        dimension(TestDims.TAG_B) as "TAG_B"
      ),
      Some(
        And(
          Seq(
            neq(dimension(TestDims.TAG_A), const("test11")),
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
          neq(dimension(TestDims.TAG_A), const("test11")),
          neq(link(TestLinks.TEST_LINK3, "testField3-1"), const("aaa")),
          neq(link(TestLinks.TEST_LINK3, "testField3-1"), const("bbb")),
          neq(link(TestLinks.TEST_LINK3, "testField3-2"), const("ccc"))
        )
      )
      .returning(
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          neq(dimension(TestDims.TAG_A), const("test11")),
          notIn(dimension(TestDims.TAG_A), Set("test11", "test12")),
          notIn(dimension(TestDims.TAG_A), Set("test13")),
          notIn(dimension(TestDims.TAG_A), Set("test11", "test14"))
        )
      )

    val pointTime = qtime.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, dimension(TestDims.TAG_A), dimension(TestDims.TAG_B), metric(TestTableFields.TEST_FIELD)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            notIn(dimension(TestDims.TAG_A), Set("test11", "test12")),
            notIn(dimension(TestDims.TAG_A), Set("test13")),
            notIn(dimension(TestDims.TAG_A), Set("test11", "test14")),
            neq(dimension(TestDims.TAG_A), const("test11"))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime)))
              .set(dimension(TestDims.TAG_A), Some("test15"))
              .set(dimension(TestDims.TAG_B), Some("test22"))
              .set(metric(TestTableFields.TEST_FIELD), Some(5d))
              .buildAndReset()
          )
      )

    val rows = tsdb.query(query).toList
    rows should have size 1
    val row = rows.head

    row.fieldValueByName[Time]("time").value shouldBe Time(pointTime)
    row.fieldValueByName[Double]("sum_testField").value shouldBe 5d
    row.fieldValueByName[String]("TAG_A").value shouldBe "test15"
    row.fieldValueByName[String]("TAG_B").value shouldBe "test22"
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
          dimension(TestDims.TAG_A) as "TAG_A",
          dimension(TestDims.TAG_B) as "TAG_B"
        ),
        Some(
          And(
            Seq(
              equ(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue")),
              equ(link(TestLinks.TEST_LINK2, "testField2"), const("testFieldValue2"))
            )
          )
        ),
        Seq(function(UnaryOperation.truncDay, time), dimension(TestDims.TAG_A), dimension(TestDims.TAG_B))
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
            in(dimension(TestDims.TAG_A), Set("test11", "test12")),
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
            in(dimension(TestDims.TAG_A), Set("test12"))
          )
        )

      val pointTime1 = qtime.getMillis + 10
      val pointTime2 = pointTime1 + 1

      (tsdbDaoMock.query _)
        .expects(
          InternalQuery(
            TestSchema.testTable,
            Set(time, dimension(TestDims.TAG_A), dimension(TestDims.TAG_B), metric(TestTableFields.TEST_FIELD)),
            and(
              ge(time, const(Time(from))),
              lt(time, const(Time(to))),
              in(dimension(TestDims.TAG_A), Set("test11", "test12")),
              in(dimension(TestDims.TAG_A), Set("test12"))
            )
          ),
          *,
          NoMetricCollector
        )
        .onCall(
          (_, b, _) =>
            Iterator(
              b.set(time, Some(Time(pointTime1)))
                .set(dimension(TestDims.TAG_A), Some("test12"))
                .set(dimension(TestDims.TAG_B), Some("test2"))
                .set(metric(TestTableFields.TEST_FIELD), Some(1d))
                .buildAndReset(),
              b.set(time, Some(Time(pointTime2)))
                .set(dimension(TestDims.TAG_A), Some("test12"))
                .set(dimension(TestDims.TAG_B), Some("test2"))
                .set(metric(TestTableFields.TEST_FIELD), Some(1d))
                .buildAndReset()
            )
        )

      val result = tsdb.query(query).toList
      val r1 = result.head
      r1.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
      r1.fieldValueByName[Double]("sum_testField").value shouldBe 2d
      r1.fieldValueByName[String]("TAG_A").value shouldBe "test12"
      r1.fieldValueByName[String]("TAG_B").value shouldBe "test2"
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
        dimension(TestDims.TAG_A) as "TAG_A",
        dimension(TestDims.TAG_B) as "TAG_B"
      ),
      Some(
        And(
          Seq(
            equ(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue")),
            equ(link(TestLinks.TEST_LINK4, "testField4"), const("testFieldValue2"))
          )
        )
      ),
      Seq(dimension(TestDims.TAG_A), dimension(TestDims.TAG_B), function(UnaryOperation.truncDay, time))
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
          in(dimension(TestDims.TAG_A), Set("test11", "test12")),
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
          in(dimension(TestDims.TAG_B), Set("test23", "test24"))
        )
      )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, dimension(TestDims.TAG_A), dimension(TestDims.TAG_B), metric(TestTableFields.TEST_FIELD)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(dimension(TestDims.TAG_A), Set("test11", "test12")),
            in(dimension(TestDims.TAG_B), Set("test23", "test24"))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test12"))
              .set(dimension(TestDims.TAG_B), Some("test23"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test12"))
              .set(dimension(TestDims.TAG_B), Some("test23"))
              .set(metric(TestTableFields.TEST_FIELD), Some(5d))
              .buildAndReset()
          )
      )

    val result = tsdb.query(query).toList
    val r1 = result.head
    r1.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r1.fieldValueByName[Double]("sum_testField").value shouldBe 6d
    r1.fieldValueByName[String]("TAG_A").value shouldBe "test12"
    r1.fieldValueByName[String]("TAG_B").value shouldBe "test23"
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
        dimension(TestDims.TAG_A) as "TAG_A",
        dimension(TestDims.TAG_B) as "TAG_B"
      ),
      Some(
        In(link(TestLinks.TEST_LINK, "testField"), Set("testFieldValue1", "testFieldValue2"))
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
          In(dimension(TestDims.TAG_A), Set("Test a 1", "Test a 2", "Test a 3"))
        )
      )

    val pointTime = qtime.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, dimension(TestDims.TAG_A), dimension(TestDims.TAG_B), metric(TestTableFields.TEST_FIELD)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(dimension(TestDims.TAG_A), Set("Test a 1", "Test a 2", "Test a 3"))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime)))
              .set(dimension(TestDims.TAG_A), Some("Test a 1"))
              .set(dimension(TestDims.TAG_B), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(2d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime)))
              .set(dimension(TestDims.TAG_A), Some("Test a 3"))
              .set(dimension(TestDims.TAG_B), Some("test2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(3d))
              .buildAndReset()
          )
      )

    val iterator = tsdb.query(query).iterator

    val r1 = iterator.next()

    r1.fieldValueByName[Time]("time").value shouldBe Time(pointTime)
    r1.fieldValueByName[Double]("sum_testField").value shouldBe 2d
    r1.fieldValueByName[String]("TAG_A").value shouldBe "Test a 1"
    r1.fieldValueByName[String]("TAG_B").value shouldBe "test1"

    val r2 = iterator.next()

    r2.fieldValueByName[Time]("time").value shouldBe Time(pointTime)
    r2.fieldValueByName[Double]("sum_testField").value shouldBe 3d
    r2.fieldValueByName[String]("TAG_A").value shouldBe "Test a 3"
    r2.fieldValueByName[String]("TAG_B").value shouldBe "test2"

    iterator.hasNext shouldBe false
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
        dimension(TestDims.TAG_A) as "TAG_A",
        dimension(TestDims.TAG_B) as "TAG_B"
      ),
      Some(
        And(
          Seq(
            In(dimension(TestDims.TAG_B), Set("B 1", "B 2")),
            In(link(TestLinks.TEST_LINK, "testField"), Set("testFieldValue1", "testFieldValue2"))
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
          in(dimension(TestDims.TAG_B), Set("B 1", "B 2")),
          in(link(TestLinks.TEST_LINK, "testField"), Set("testFieldValue1", "testFieldValue2"))
        )
      )
      .returning(
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          in(dimension(TestDims.TAG_B), Set("B 1", "B 2")),
          in(dimension(TestDims.TAG_A), Set("A 1", "A 2", "A 3"))
        )
      )

    val pointTime = qtime.getMillis + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, dimension(TestDims.TAG_A), dimension(TestDims.TAG_B), metric(TestTableFields.TEST_FIELD)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(dimension(TestDims.TAG_A), Set("A 1", "A 2", "A 3")),
            in(dimension(TestDims.TAG_B), Set("B 1", "B 2"))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime)))
              .set(dimension(TestDims.TAG_A), Some("A 1"))
              .set(dimension(TestDims.TAG_B), Some("B 1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime)))
              .set(dimension(TestDims.TAG_A), Some("A 2"))
              .set(dimension(TestDims.TAG_B), Some("B 1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(3d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime)))
              .set(dimension(TestDims.TAG_A), Some("A 2"))
              .set(dimension(TestDims.TAG_B), Some("B 2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(4d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime)))
              .set(dimension(TestDims.TAG_A), Some("A 3"))
              .set(dimension(TestDims.TAG_B), Some("B 2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(6d))
              .buildAndReset()
          )
      )

    val iterator = tsdb.query(query).iterator

    val r1 = iterator.next()

    r1.fieldValueByName[Time]("time").value shouldBe Time(pointTime)
    r1.fieldValueByName[Double]("sum_testField").value shouldBe 1d
    r1.fieldValueByName[String]("TAG_A").value shouldBe "A 1"
    r1.fieldValueByName[String]("TAG_B").value shouldBe "B 1"

    val r2 = iterator.next()

    r2.fieldValueByName[Time]("time").value shouldBe Time(pointTime)
    r2.fieldValueByName[Double]("sum_testField").value shouldBe 3d
    r2.fieldValueByName[String]("TAG_A").value shouldBe "A 2"
    r2.fieldValueByName[String]("TAG_B").value shouldBe "B 1"

    val r3 = iterator.next()

    r3.fieldValueByName[Time]("time").value shouldBe Time(pointTime)
    r3.fieldValueByName[Double]("sum_testField").value shouldBe 4d
    r3.fieldValueByName[String]("TAG_A").value shouldBe "A 2"
    r3.fieldValueByName[String]("TAG_B").value shouldBe "B 2"

    val r4 = iterator.next()

    r4.fieldValueByName[Time]("time").value shouldBe Time(pointTime)
    r4.fieldValueByName[Double]("sum_testField").value shouldBe 6d
    r4.fieldValueByName[String]("TAG_A").value shouldBe "A 3"
    r4.fieldValueByName[String]("TAG_B").value shouldBe "B 2"

    iterator.hasNext shouldBe false
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
        dimension(TestDims.TAG_A) as "TAG_A",
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
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.TAG_A)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to)))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test12"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test12"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test13"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test13"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset()
          )
      )

    val results = tsdb.query(query).iterator

    val r1 = results.next()
    r1.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r1.fieldValueByName[Double]("sum_testField").value shouldBe 4d
    r1.fieldValueByName[String]("TestCatalog_testField").value shouldBe "testFieldValue1"

    val r2 = results.next()
    r2.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r2.fieldValueByName[Double]("sum_testField").value shouldBe 2d
    r2.fieldValueByName[String]("TestCatalog_testField").value shouldBe "testFieldValue2"
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
      Seq(function(UnaryOperation.truncDay, time), dimension(TestDims.TAG_A))
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
            dimension(TestDims.TAG_A)
          ),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .set(metric(TestTableFields.TEST_STRING_FIELD), Some("001_01_1"))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime1 + 1)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .set(metric(TestTableFields.TEST_STRING_FIELD), Some("001_01_2"))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime1 + 2)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .set(metric(TestTableFields.TEST_STRING_FIELD), Some("001_01_200"))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime1 + 3)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .set(metric(TestTableFields.TEST_STRING_FIELD), Some("001_02_1"))
              .buildAndReset()
          )
      )
      .repeated(3)

    val startDay = Time(qtime.withMillisOfDay(0).getMillis)

    val r1 = tsdb.query(query1).head
    r1.fieldValueByName[Time]("time").value shouldBe startDay
    r1.fieldValueByName[Double]("sum_testField").value shouldBe 4d
    r1.fieldValueByName[String]("min_testStringField").value shouldBe "001_01_1"

    val query2 = query1.copy(
      fields = Seq(
        function(UnaryOperation.truncDay, time) as "time",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        aggregate(Aggregation.max[String], TestTableFields.TEST_STRING_FIELD) as "max_testStringField"
      )
    )

    val r2 = tsdb.query(query2).head
    r2.fieldValueByName[Time]("time").value shouldBe startDay
    r2.fieldValueByName[Double]("sum_testField").value shouldBe 4d
    r2.fieldValueByName[String]("max_testStringField").value shouldBe "001_02_1"

    val query3 = query1.copy(
      fields = Seq(
        function(UnaryOperation.truncDay, time) as "time",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        aggregate(Aggregation.count[String], TestTableFields.TEST_STRING_FIELD) as "count_testStringField"
      )
    )

    val r3 = tsdb.query(query3).head
    r3.fieldValueByName[Time]("time").value shouldBe startDay
    r3.fieldValueByName[Double]("sum_testField").value shouldBe 4d
    r3.fieldValueByName[Long]("count_testStringField").value shouldBe 4L
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
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.TAG_A)),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("testA1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("testA1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("testA2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(2d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("testA2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset()
          )
      )

    val results = tsdb.query(query).toList
    results should have size 2

    val it = results.iterator

    val r1 = it.next()
    r1.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r1.fieldValueByName[Double]("sum_testField").value shouldBe 2d
    r1.fieldValueByName[String]("TestCatalog3_testField3-1").value shouldBe "Value1"
    r1.fieldValueByName[String]("TestCatalog3_testField3-2").value shouldBe "Value1"
    r1.fieldValueByName[String]("TestCatalog3_testField3-3").value shouldBe "Value2"

    val r2 = it.next()
    r2.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r2.fieldValueByName[Double]("sum_testField").value shouldBe 3d
    r2.fieldValueByName[String]("TestCatalog3_testField3-1").value shouldBe "Value1"
    r2.fieldValueByName[String]("TestCatalog3_testField3-2").value shouldBe "Value2"
    r2.fieldValueByName[String]("TestCatalog3_testField3-3").value shouldBe "Value2"
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
        dimension(TestDims.TAG_A) as "TAG_A",
        dimension(TestDims.TAG_B) as "TAG_B"
      ),
      None,
      Seq(function(UnaryOperation.truncDay, time), dimension(TestDims.TAG_A), dimension(TestDims.TAG_B))
    )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 5
    val pointTime3 = pointTime1 + 10

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.TAG_A), dimension(TestDims.TAG_B)),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(dimension(TestDims.TAG_B), Some("test2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(dimension(TestDims.TAG_B), Some("test2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime3)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(dimension(TestDims.TAG_B), Some("test2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset()
          )
      )

    val r = tsdb.query(query).head
    r.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r.fieldValueByName[Time]("min_time").value shouldBe Time(pointTime1)
    r.fieldValueByName[Time]("max_time").value shouldBe Time(pointTime3)
    r.fieldValueByName[Double]("sum_testField").value shouldBe 3d
    r.fieldValueByName[String]("TAG_A").value shouldBe "test1"
    r.fieldValueByName[String]("TAG_B").value shouldBe "test2"
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
        dimension(TestDims.TAG_A) as "TAG_A",
        dimension(TestDims.TAG_B) as "TAG_B"
      ),
      None,
      Seq(function(UnaryOperation.truncDay, time), dimension(TestDims.TAG_A), dimension(TestDims.TAG_B))
    )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.TAG_A), dimension(TestDims.TAG_B)),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(dimension(TestDims.TAG_B), Some("test2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(dimension(TestDims.TAG_B), Some("test2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset()
          )
      )

    val row = tsdb.query(query).head

    row.fieldValueByName[BigDecimal]("dummy").value shouldEqual BigDecimal(1)
    row.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    row.fieldValueByName[Double]("sum_testField").value shouldBe 2d
    row.fieldValueByName[String]("TAG_A").value shouldBe "test1"
    row.fieldValueByName[String]("TAG_B").value shouldBe "test2"
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
        aggregate(Aggregation.count[String], dimension(TestDims.TAG_A)) as "count_TAG_A",
        dimension(TestDims.TAG_B) as "TAG_B"
      ),
      None,
      Seq(function(UnaryOperation.truncDay, time), dimension(TestDims.TAG_B))
    )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.TAG_A), dimension(TestDims.TAG_B)),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(dimension(TestDims.TAG_B), Some("test2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(dimension(TestDims.TAG_B), Some("test2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset()
          )
      )

    val row = tsdb.query(query).head

    row.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    row.fieldValueByName[Double]("sum_testField").value shouldBe 2d
    row.fieldValueByName[Long]("count_TAG_A").value shouldBe 2L
    row.fieldValueByName[String]("TAG_B").value shouldBe "test2"
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
        dimension(TestDims.TAG_A) as "TAG_A",
        aggregate(Aggregation.count[String], link(TestLinks.TEST_LINK, "testField")) as "count_TestCatalog_testField"
      ),
      Some(equ(link(TestLinks.TEST_LINK, "testField"), const("testFieldValue"))),
      Seq(function(UnaryOperation.truncDay, time), dimension(TestDims.TAG_A))
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
          in(dimension(TestDims.TAG_A), Set("test1", "test12"))
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
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.TAG_A)),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(dimension(TestDims.TAG_A), Set("test1", "test12"))
          )
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test12"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test12"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset()
          )
      )

    val results = tsdb.query(query).iterator

    val r1 = results.next()
    r1.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r1.fieldValueByName[Double]("sum_testField").value shouldBe 2d
    r1.fieldValueByName[String]("TAG_A").value shouldBe "test12"
    r1.fieldValueByName[Long]("count_TestCatalog_testField").value shouldBe 2L

    val r2 = results.next()
    r2.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r2.fieldValueByName[Double]("sum_testField").value shouldBe 2d
    r2.fieldValueByName[String]("TAG_A").value shouldBe "test1"
    r2.fieldValueByName[Long]("count_TestCatalog_testField").value shouldBe 2L
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
        aggregate(Aggregation.distinctCount[String], dimension(TestDims.TAG_A)) as "distinct_count_TAG_A",
        aggregate(Aggregation.count[String], dimension(TestDims.TAG_A)) as "count_TAG_A",
        dimension(TestDims.TAG_B) as "TAG_B"
      ),
      None,
      Seq(function(UnaryOperation.truncDay, time), dimension(TestDims.TAG_B))
    )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.TAG_A), dimension(TestDims.TAG_B)),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("testA1"))
              .set(dimension(TestDims.TAG_B), Some("testB2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("testA1"))
              .set(dimension(TestDims.TAG_B), Some("testB2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("testA2"))
              .set(dimension(TestDims.TAG_B), Some("testB1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("testA2"))
              .set(dimension(TestDims.TAG_B), Some("testB1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("testA1"))
              .set(dimension(TestDims.TAG_B), Some("testB1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("testA1"))
              .set(dimension(TestDims.TAG_B), Some("testB1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset()
          )
      )

    val results = tsdb.query(query).iterator

    val r1 = results.next()
    r1.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r1.fieldValueByName[Double]("sum_testField").value shouldBe 2d
    r1.fieldValueByName[Long]("count_TAG_A").value shouldBe 2L
    r1.fieldValueByName[Int]("distinct_count_TAG_A").value shouldBe 1
    r1.fieldValueByName[String]("TAG_B").value shouldBe "testB2"

    val r2 = results.next()
    r2.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r2.fieldValueByName[Double]("sum_testField").value shouldBe 4d
    r2.fieldValueByName[Long]("count_TAG_A").value shouldBe 4L
    r2.fieldValueByName[Int]("distinct_count_TAG_A").value shouldBe 2
    r2.fieldValueByName[String]("TAG_B").value shouldBe "testB1"
  }

  it should "calculate lag" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val from = qtime.getMillis
    val to = qtime.plusDays(1).getMillis

    val query = Query(
      TestSchema.testTable,
      Seq(
        time as "time_time",
        windowFunction(WindowOperation.lag[Time], time) as "lag_time_time",
        metric(TestTableFields.TEST_FIELD) as "testField",
        dimension(TestDims.TAG_A) as "TAG_A",
        dimension(TestDims.TAG_B) as "TAG_B"
      ),
      And(
        Seq(
          SimpleCondition(BinaryOperationExpr(BinaryOperation.ge[Time], time, const(Time(qtime)))),
          SimpleCondition(BinaryOperationExpr(BinaryOperation.lt[Time], time, const(Time(qtime.plusDays(1)))))
        )
      ),
      Seq(dimension(TestDims.TAG_B)),
      None
    )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.TAG_A), dimension(TestDims.TAG_B)),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("testA1"))
              .set(dimension(TestDims.TAG_B), Some("testB2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("testA1"))
              .set(dimension(TestDims.TAG_B), Some("testB2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("testA2"))
              .set(dimension(TestDims.TAG_B), Some("testB1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("testA2"))
              .set(dimension(TestDims.TAG_B), Some("testB1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("testA1"))
              .set(dimension(TestDims.TAG_B), Some("testB1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2 + 1000)))
              .set(dimension(TestDims.TAG_A), Some("testA1"))
              .set(dimension(TestDims.TAG_B), Some("testB1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset()
          )
      )

    val t = Table(
      ("time_time", "lag_time_time", "testField", "TAG_A", "TAG_B"),
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

      r.fieldValueByName[Time]("time_time").value.toLocalDateTime.withMillisOfSecond(0) shouldBe time
      r.fieldValueByName[Time]("lag_time_time").map(_.toLocalDateTime.withMillisOfSecond(0)) shouldBe lagTime
      r.fieldValueByName[Double]("testField").value shouldBe testField
      r.fieldValueByName("TAG_A").value shouldBe tagA
      r.fieldValueByName[String]("TAG_B").value shouldBe tagB
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
        dimension(TestDims.TAG_A) as "TAG_A"
      ),
      None,
      Seq(function(UnaryOperation.truncDay, time), dimension(TestDims.TAG_A))
    )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.TAG_A)),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(10d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(15d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset()
          )
      )

    val results = tsdb.query(query).iterator

    val group1 = results.next()
    group1.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    group1.fieldValueByName[BigDecimal]("between_10_20").value shouldBe BigDecimal(2)
    group1.fieldValueByName[String]("TAG_A").value shouldBe "test1"
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
        dimension(TestDims.TAG_A) as "TAG_A"
      ),
      None,
      Seq(function(UnaryOperation.truncDay, time), dimension(TestDims.TAG_A))
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
          Set(time, dimension(TestDims.TAG_A)),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime1))).set(dimension(TestDims.TAG_A), Some("test1")).buildAndReset(),
            b.set(time, Some(Time(pointTime2))).set(dimension(TestDims.TAG_A), Some("test1")).buildAndReset(),
            b.set(time, Some(Time(pointTime1))).set(dimension(TestDims.TAG_A), Some("test1")).buildAndReset(),
            b.set(time, Some(Time(pointTime2))).set(dimension(TestDims.TAG_A), Some("test1")).buildAndReset(),
            b.set(time, Some(Time(pointTime1))).set(dimension(TestDims.TAG_A), Some("test1")).buildAndReset(),
            b.set(time, Some(Time(pointTime2))).set(dimension(TestDims.TAG_A), Some("test1")).buildAndReset()
          )
      )

    val results = tsdb.query(query).iterator

    val group1 = results.next()
    group1.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    group1.fieldValueByName[BigDecimal]("between_10_20").value shouldBe BigDecimal(0)
    group1.fieldValueByName[String]("TAG_A").value shouldBe "test1"
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
        dimension(TestDims.TAG_A) as "TAG_A"
      ),
      None,
      Seq(function(UnaryOperation.truncDay, time), dimension(TestDims.TAG_A)),
      None,
      Some(ge(sum(metric(TestTableFields.TEST_FIELD)), const[Double](3d)))
    )

    val pointTime1 = qtime.getMillis + 10
    val pointTime2 = pointTime1 + 1

    (tsdbDaoMock.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.TAG_A)),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test12"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test12"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset()
          )
      )

    val results = tsdb.query(query).toList
    results should have size 1

    val r = results.head
    r.fieldValueByName[Time]("time").value shouldBe Time(qtime.withMillisOfDay(0).getMillis)
    r.fieldValueByName[Double]("sum_testField").value shouldBe 4d
    r.fieldValueByName[String]("TAG_A").value shouldBe "test1"
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
        dimension(TestDims.TAG_A) as "TAG_A",
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
          Set(time, metric(TestTableFields.TEST_FIELD), dimension(TestDims.TAG_A)),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(1d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime1)))
              .set(dimension(TestDims.TAG_A), Some("test1"))
              .set(metric(TestTableFields.TEST_FIELD), Some(2d))
              .buildAndReset(),
            b.set(time, Some(Time(pointTime2)))
              .set(dimension(TestDims.TAG_A), Some("test2"))
              .set(metric(TestTableFields.TEST_FIELD), Some(3d))
              .buildAndReset()
          )
      )

    val results = tsdb.query(query).iterator

    val r1 = results.next()
    r1.fieldValueByName[Double]("testField").value shouldBe 1d
    r1.fieldValueByName[String]("TAG_A").value shouldBe "test1"
    r1.fieldValueByName[String]("TestCatalog_testField").value shouldBe "testFieldValue"

    val r2 = results.next()
    r2.fieldValueByName[Double]("testField").value shouldBe 2d
    r2.fieldValueByName[String]("TAG_A").value shouldBe "test1"
    r2.fieldValueByName[String]("TestCatalog_testField").value shouldBe "testFieldValue"

    val r3 = results.next()
    r3.fieldValueByName[Double]("testField").value shouldBe 3d
    r3.fieldValueByName[String]("TAG_A").value shouldBe "test2"
    r3.fieldValueByName[String]("TestCatalog_testField") shouldBe empty
  }

  it should "handle queries like this" in withTsdbMock { (tsdb, tsdbDaoMock) =>
    val parser = new SqlParser
    val sqlQueryProcessor = new SqlQueryProcessor(TestSchema.schema)
    val format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val from: DateTime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val to: DateTime = from.plusDays(1)

    val testCatalogServiceMock = mock[ExternalLinkService[TestLinks.TestLink]]
    tsdb.registerExternalLink(TestLinks.TEST_LINK2, testCatalogServiceMock)

    val sql = s"SELECT sum(CASE WHEN TAG_A = '2' THEN 1 ELSE 0) AS salesTicketsCount, day(time) AS d FROM test_table " +
      s"WHERE time >= TIMESTAMP '${from.toString(format)}' AND time < TIMESTAMP '${to.toString(format)}' GROUP BY d"

    val query = parser.parse(sql).right.flatMap {
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
          Set(time, dimension(TestDims.TAG_A)),
          and(ge(time, const(Time(from))), lt(time, const(Time(to))))
        ),
        *,
        NoMetricCollector
      )
      .onCall(
        (_, b, _) =>
          Iterator(
            b.set(time, Some(Time(pointTime2))).set(dimension(TestDims.TAG_A), Some("1")).buildAndReset(),
            b.set(time, Some(Time(pointTime1))).set(dimension(TestDims.TAG_A), Some("1")).buildAndReset(),
            b.set(time, Some(Time(pointTime1))).set(dimension(TestDims.TAG_A), Some("2")).buildAndReset()
          )
      )

    val results = tsdb.query(query).toList
    results should have size 1

    val r1 = results.head
    r1.fieldValueByName[Double]("salesTicketsCount").value shouldBe 1
  }
}

package org.yupana.hbase

import java.util.Properties

import org.apache.hadoop.hbase.client.{ Result, Scan }
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter
import org.apache.hadoop.hbase.util.Bytes
import org.scalamock.function.{ FunctionAdapter1, MockFunction1 }
import org.scalamock.scalatest.MockFactory
import org.scalatest._
import org.yupana.api.Time
import org.yupana.api.query.{ DimIdInExpr, DimIdNotInExpr, Expression }
import org.yupana.api.schema.{ Dimension, Table }
import org.yupana.api.types.Writable
import org.yupana.api.utils.SortedSetIterator
import org.yupana.core.cache.CacheFactory
import org.yupana.core.dao.{ DictionaryDao, DictionaryProvider, DictionaryProviderImpl }
import org.yupana.core.model._
import org.yupana.core.utils.metric.{ MetricQueryCollector, NoMetricCollector }
import org.yupana.core.{ MapReducible, TestDims, TestSchema, TestTableFields }

import scala.collection.JavaConverters._

class TSDaoHBaseTest
    extends FlatSpec
    with Matchers
    with MockFactory
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with OptionValues {

  import TestSchema._
  import org.yupana.api.query.syntax.All._

  type QueryRunner = MockFunction1[Seq[Scan], Iterator[Result]]

  override protected def beforeAll(): Unit = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("app.properties"))
    CacheFactory.init(properties, "test")
  }

  override protected def beforeEach(): Unit = {
    CacheFactory.flushCaches()
  }

  def baseTime(time: Long) = {
    time - (time % testTable.rowTimeSpan)
  }

  def scan(from: Long, to: Long) = {
    where { (scans: Seq[Scan]) =>
      val scan = scans.head
      baseTime(from) == Bytes.toLong(scan.getStartRow) &&
      baseTime(to) == (Bytes.toLong(scan.getStopRow) - 1)
    }
  }

  def scan(from: Long, to: Long, range: Seq[Long]): FunctionAdapter1[Seq[Scan], Boolean] = {
    scan(from, to, Set(range))
  }

  def scan(from: Long, to: Long, ranges: Set[Seq[Long]]): FunctionAdapter1[Seq[Scan], Boolean] = {
    where { (scans: Seq[Scan]) =>
      val scan = scans.head
      val filter = scan.getFilter.asInstanceOf[MultiRowRangeFilter]
      val rowRanges = filter.getRowRanges.asScala

      val rangesChecks = for {
        time <- (baseTime(from) to baseTime(to) by testTable.rowTimeSpan)
        range <- ranges
      } yield {
        rowRanges.exists { rowRange =>
          range.zipWithIndex.forall {
            case (id, idx) =>
              val e = if (idx == range.size - 1) id + 1 else id
              id >= Bytes.toLong(rowRange.getStartRow, 8 + idx * 8) &&
              e <= Bytes.toLong(rowRange.getStopRow, 8 + idx * 8)
          } &&
          time == Bytes.toLong(rowRange.getStartRow) &&
          time == Bytes.toLong(rowRange.getStopRow)
        }
      }

      rangesChecks.forall(b => b) &&
      baseTime(from) == Bytes.toLong(scan.getStartRow) &&
      baseTime(to) == Bytes.toLong(scan.getStopRow)
    }
  }

  "TSDaoHBase" should "execute time bounded queries" in withMock { (dao, dictionary, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression](time, dimension(TestDims.TAG_A), metric(TestTableFields.TEST_FIELD))
    val valueDataBuilder = new InternalRowBuilder(exprs.zipWithIndex.toMap)
    val pointTime = 2000

    queryRunner
      .expects(scan(from, to))
      .returning(
        Iterator(
          HBaseTestUtils
            .row(TSDRowKey(pointTime - (pointTime % testTable.rowTimeSpan), Array(Some(1), Some(2))))
            .cell(pointTime % testTable.rowTimeSpan, 1, 1d)
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "test1")
            .hbaseRow
        )
      )

    val res = dao
      .query(
        InternalQuery(testTable, exprs.toSet, and(ge(time, const(Time(from))), lt(time, const(Time(to))))),
        valueDataBuilder,
        NoMetricCollector
      )
      .toList

    res.size shouldEqual 1

    val r = res.head
    r.get[Time](0).value shouldEqual Time(pointTime)
    r.get[Time](1).value shouldEqual "test1"
    r.get[Time](2).value shouldEqual 1d
  }

  it should "skip values with fields not defined in schema" in withMock { (dao, dictionary, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs =
      Seq[Expression](time, dimension(TestDims.TAG_A), dimension(TestDims.TAG_B), metric(TestTableFields.TEST_FIELD))
    val valueDataBuilder = new InternalRowBuilder(exprs.zipWithIndex.toMap)
    val pointTime = 2000

    queryRunner
      .expects(scan(from, to, Seq(1L)))
      .returning(
        Iterator(
          HBaseTestUtils
            .row(TSDRowKey(pointTime - (pointTime % testTable.rowTimeSpan), Array(Some(1), Some(2))))
            .cell((pointTime + 1) % testTable.rowTimeSpan, 1, 1d)
            .cell(pointTime % testTable.rowTimeSpan, 111, 3d)
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "test1")
            .cell((pointTime + 1) % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "test1")
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test22")
            .cell((pointTime + 1) % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test22")
            .hbaseRow
        )
      )

    (dictionary.getIdsByValues _).expects(TestDims.TAG_A, Set("test1")).returning(Map("test1" -> 1L))

    val res = dao
      .query(
        InternalQuery(
          testTable,
          exprs.toSet,
          and(ge(time, const(Time(from))), lt(time, const(Time(to))), equ(dimension(TestDims.TAG_A), const("test1")))
        ),
        valueDataBuilder,
        NoMetricCollector
      )
      .toList

    res.size shouldEqual 2

    val r1 = res(0)
    r1.get(0).value shouldEqual Time(pointTime)
    r1.get(1).value shouldEqual "test1"
    r1.get(2).value shouldEqual "test22"
    r1.get(3) shouldEqual None

    val r2 = res(1)
    r2.get(0).value shouldEqual Time(pointTime + 1)
    r2.get(1).value shouldEqual "test1"
    r2.get(2).value shouldEqual "test22"
    r2.get(3).value shouldEqual 1d
  }

  it should "set tag filter for equ" in withMock { (dao, dictionary, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs =
      Seq[Expression](time, dimension(TestDims.TAG_A), dimension(TestDims.TAG_B), metric(TestTableFields.TEST_FIELD))
    val valueDataBuilder = new InternalRowBuilder(exprs.zipWithIndex.toMap)
    val pointTime = 2000

    queryRunner
      .expects(scan(from, to, Seq(1L)))
      .returning(
        Iterator(
          HBaseTestUtils
            .row(TSDRowKey(pointTime - (pointTime % testTable.rowTimeSpan), Array(Some(1), Some(2))))
            .cell(pointTime % testTable.rowTimeSpan, 1, 1d)
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "test1")
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test22")
            .hbaseRow
        )
      )

    (dictionary.getIdsByValues _).expects(TestDims.TAG_A, Set("test1")).returning(Map("test1" -> 1L))

    val res = dao
      .query(
        InternalQuery(
          testTable,
          exprs.toSet,
          and(ge(time, const(Time(from))), lt(time, const(Time(to))), equ(dimension(TestDims.TAG_A), const("test1")))
        ),
        valueDataBuilder,
        NoMetricCollector
      )
      .toList

    res.size shouldEqual 1

    val r = res.head
    r.get(0).value shouldEqual Time(pointTime)
    r.get(1).value shouldEqual "test1"
    r.get(2).value shouldEqual "test22"
    r.get(3).value shouldEqual 1d
  }

  it should "support not create queries if dimension value is not found" in withMock { (dao, dictionary, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs =
      Seq[Expression](time, dimension(TestDims.TAG_A), dimension(TestDims.TAG_B), metric(TestTableFields.TEST_FIELD))
    val valueDataBuilder = new InternalRowBuilder(exprs.zipWithIndex.toMap)

    queryRunner.expects(Seq.empty).returning(Iterator.empty)

    (dictionary.getIdsByValues _).expects(TestDims.TAG_A, Set("test1")).returning(Map.empty)

    val res = dao
      .query(
        InternalQuery(
          testTable,
          exprs.toSet,
          and(ge(time, const(Time(from))), lt(time, const(Time(to))), equ(dimension(TestDims.TAG_A), const("test1")))
        ),
        valueDataBuilder,
        NoMetricCollector
      )
      .toList

    res shouldBe empty
  }

  it should "support IN operation for tags" in withMock { (dao, dictionary, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression](time, dimension(TestDims.TAG_B), metric(TestTableFields.TEST_FIELD))
    val valueDataBuilder = new InternalRowBuilder(exprs.zipWithIndex.toMap)

    val pointTime1 = 2000
    val pointTime2 = 2200

    queryRunner
      .expects(scan(from, to, Set(Seq(1L), Seq(2L))))
      .returning(
        Iterator(
          HBaseTestUtils
            .row(TSDRowKey(pointTime1 - (pointTime1 % testTable.rowTimeSpan), Array(Some(1), Some(2))))
            .cell((pointTime1) % testTable.rowTimeSpan, 1, 7d)
            .cell((pointTime2) % testTable.rowTimeSpan, 1, 5d)
            .cell(pointTime1 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "test1")
            .cell(pointTime2 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "test1")
            .cell(pointTime1 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test25")
            .cell(pointTime2 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test25")
            .hbaseRow
        )
      )

    (dictionary.getIdsByValues _)
      .expects(TestDims.TAG_A, Set("test1", "test2"))
      .returning(Map("test1" -> 1L, "test2" -> 2L))

    val res = dao
      .query(
        InternalQuery(
          testTable,
          exprs.toSet,
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(dimension(TestDims.TAG_A), Set("test1", "test2"))
          )
        ),
        valueDataBuilder,
        NoMetricCollector
      )
      .toList

    res.size shouldEqual 2

    res(0).get(0).value shouldEqual Time(pointTime1)
    res(0).get(1).value shouldEqual "test25"
    res(0).get(2).value shouldEqual 7d

    res(1).get(0).value shouldEqual Time(pointTime2)
    res(1).get(1).value shouldEqual "test25"
    res(1).get(2).value shouldEqual 5d
  }

  it should "do nothing if IN values are empty" in withMock { (dao, dictionary, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression](time, dimension(TestDims.TAG_B), metric(TestTableFields.TEST_FIELD))
    val valueDataBuilder = new InternalRowBuilder(exprs.zipWithIndex.toMap)

    queryRunner.expects(Seq()).returning(Iterator.empty)

    val res = dao
      .query(
        InternalQuery(
          testTable,
          exprs.toSet,
          and(ge(time, const(Time(from))), lt(time, const(Time(to))), in(dimension(TestDims.TAG_A), Set()))
        ),
        valueDataBuilder,
        NoMetricCollector
      )
      .toList

    res shouldBe empty
  }

  it should "intersect different conditions for same tag" in withMock { (dao, dictionary, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression](time, dimension(TestDims.TAG_B), metric(TestTableFields.TEST_FIELD))
    val valueDataBuilder = new InternalRowBuilder(exprs.zipWithIndex.toMap)

    val pointTime1 = 2000

    queryRunner
      .expects(
        scan(from, to, Seq(2L, 21L))
      )
      .returning(
        Iterator(
          HBaseTestUtils
            .row(TSDRowKey(pointTime1 - (pointTime1 % testTable.rowTimeSpan), Array(Some(1), Some(2))))
            .cell(pointTime1 % testTable.rowTimeSpan, 1, 3d)
            .cell(pointTime1 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "test1")
            .cell(pointTime1 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test21")
            .hbaseRow
        )
      )

    (dictionary.getIdsByValues _).expects(TestDims.TAG_A, Set("test2")).returning(Map("test2" -> 2L))
    (dictionary.getIdsByValues _).expects(TestDims.TAG_B, Set("test21")).returning(Map("test21" -> 21L))

    dao.query(
      InternalQuery(
        testTable,
        exprs.toSet,
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          in(dimension(TestDims.TAG_A), Set("test1", "test2")),
          equ(dimension(TestDims.TAG_B), const("test21")),
          in(dimension(TestDims.TAG_A), Set("test2", "test3"))
        )
      ),
      valueDataBuilder,
      NoMetricCollector
    )
  }

  it should "cross join different IN conditions for different tags" in withMock { (dao, dictionary, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs =
      Seq[Expression](time, dimension(TestDims.TAG_A), dimension(TestDims.TAG_B), metric(TestTableFields.TEST_FIELD))
    val valueDataBuilder = new InternalRowBuilder(exprs.zipWithIndex.toMap)

    val pointTime = 2000

    queryRunner
      .expects(
        scan(
          from,
          to,
          Set(
            Seq(1L, 1L),
            Seq(1L, 2L),
            Seq(2L, 1L),
            Seq(2L, 2L),
            Seq(3L, 1L),
            Seq(3L, 2L)
          )
        )
      )
      .returning(
        Iterator(
          HBaseTestUtils
            .row(TSDRowKey(pointTime - (pointTime % testTable.rowTimeSpan), Array(Some(1), Some(1))))
            .cell(pointTime % testTable.rowTimeSpan, 1, 1d)
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "A 1")
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "B 1")
            .hbaseRow,
          HBaseTestUtils
            .row(TSDRowKey(pointTime - (pointTime % testTable.rowTimeSpan), Array(Some(2), Some(1))))
            .cell(pointTime % testTable.rowTimeSpan, 1, 3d)
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "A 2")
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "B 1")
            .hbaseRow,
          HBaseTestUtils
            .row(TSDRowKey(pointTime - (pointTime % testTable.rowTimeSpan), Array(Some(2), Some(2))))
            .cell(pointTime % testTable.rowTimeSpan, 1, 4d)
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "A 2")
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "B 2")
            .hbaseRow,
          HBaseTestUtils
            .row(TSDRowKey(pointTime - (pointTime % testTable.rowTimeSpan), Array(Some(3), Some(2))))
            .cell(pointTime % testTable.rowTimeSpan, 1, 6d)
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "A 3")
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "B 3")
            .hbaseRow
        )
      )

    (dictionary.getIdsByValues _)
      .expects(TestDims.TAG_A, Set("A 1", "A 2", "A 3"))
      .returning(Map("A 1" -> 1L, "A 2" -> 2L, "A 3" -> 3L))
    (dictionary.getIdsByValues _).expects(TestDims.TAG_B, Set("B 1", "B 2")).returning(Map("B 1" -> 1L, "B 2" -> 2L))

    val res = dao
      .query(
        InternalQuery(
          testTable,
          exprs.toSet,
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(dimension(TestDims.TAG_A), Set("A 1", "A 2", "A 3")),
            in(dimension(TestDims.TAG_B), Set("B 1", "B 2"))
          )
        ),
        valueDataBuilder,
        NoMetricCollector
      )
      .toList

    res should have size 4
  }

  it should "cross join in and eq for different tags" in withMock { (dao, dictionary, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs =
      Seq[Expression](time, dimension(TestDims.TAG_A), dimension(TestDims.TAG_B), metric(TestTableFields.TEST_FIELD))
    val valueDataBuilder = new InternalRowBuilder(exprs.zipWithIndex.toMap)

    val pointTime = 2000

    queryRunner
      .expects(
        scan(
          from,
          to,
          Set(
            Seq(1L, 1L),
            Seq(2L, 1L),
            Seq(3L, 1L)
          )
        )
      )
      .returning(
        Iterator(
          HBaseTestUtils
            .row(TSDRowKey(pointTime - (pointTime % testTable.rowTimeSpan), Array(Some(1), Some(1))))
            .cell(pointTime % testTable.rowTimeSpan, 1, 1d)
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "A 1")
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "B 1")
            .hbaseRow,
          HBaseTestUtils
            .row(TSDRowKey(pointTime - (pointTime % testTable.rowTimeSpan), Array(Some(2), Some(1))))
            .cell(pointTime % testTable.rowTimeSpan, 1, 1d)
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "A 2")
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "B 1")
            .hbaseRow
        )
      )

    (dictionary.getIdsByValues _)
      .expects(TestDims.TAG_A, Set("A 1", "A 2", "A 3"))
      .returning(Map("A 1" -> 1L, "A 2" -> 2L, "A 3" -> 3L))
    (dictionary.getIdsByValues _).expects(TestDims.TAG_B, Set("B 1")).returning(Map("B 1" -> 1L))

    val res = dao
      .query(
        InternalQuery(
          testTable,
          exprs.toSet,
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(dimension(TestDims.TAG_A), Set("A 1", "A 2", "A 3")),
            equ(dimension(TestDims.TAG_B), const("B 1"))
          )
        ),
        valueDataBuilder,
        NoMetricCollector
      )
      .toList

    res should have size 2
  }

//////  it should "use post filter if there are too many combinations" in withMock { (dao, dictionary, queryRunner) =>
//////    val from = 1000
//////    val to = 5000
//////    val exprs = Seq[Expression](time, dimension(TestTable.TAG_A), dimension(TestTable.TAG_B), metric(TestTable.TEST_FIELD))
//////    val valueDataBuilder = new InternalRowBuilder(exprs.zipWithIndex.toMap)
//////
//////    val pointTime1 = 2000
//////    val pointTime2 = 2500
//////
//////    val manyAs = (1 to 200).map(_.toString)
//////    val manyBs = (1 to 3000).map(_.toString)
//////
//////
//////    queryRunner.expects(where { tsdQueries: Seq[TSDQuery] =>
//////      tsdQueries.size  == 200 &&
//////      tsdQueries.sortBy(_.dimensionFilter(0)).zipWithIndex.forall { case (tsdQuery, idx) =>
//////        (tsdQuery.dimensionFilter sameElements Array(Some(idx + 1), None)) &&
//////        tsdQuery.from == from &&
//////        tsdQuery.to == to }
//////    }).returning(
//////      Iterator(
//////        TSDOutputRow(
//////          TSDRowKey(pointTime1 - (pointTime1 % TestTable.rowTimeSpan), Array(Some(2), Some(2))),
//////          Array(
//////            pointTime1 % TestTable.rowTimeSpan ->  tagged(1, 1d),
//////            pointTime2 % TestTable.rowTimeSpan ->  tagged(1, 5d)
//////          )
//////        ),
//////        TSDOutputRow(
//////          TSDRowKey(pointTime1 - (pointTime1 % TestTable.rowTimeSpan), Array(Some(5), Some(6000))),
//////          Array(
//////            pointTime1 % TestTable.rowTimeSpan ->  tagged(1, 2d),
//////            pointTime2 % TestTable.rowTimeSpan ->  tagged(1, 3d)
//////          )
//////        )
//////      )
//////    )
//////
//////    (dictionary.getIdsByValues _)
//////      .expects(where { (tag, values) => tag == TestTable.TAG_A && values == manyAs.toSet })
//////      .returning(manyBs.map(x => x -> x.toLong).toMap)
//////    (dictionary.getIdsByValues _)
//////      .expects(where { (tag, values) => tag == TestTable.TAG_B && values == manyBs.toSet })
//////      .returning(manyBs.map(x => x -> x.toLong).toMap)
//////
//////    (dictionary.getValuesByIds _).expects(TestTable.TAG_A, Set(2l)).returning(Map(2l -> "2"))
//////    (dictionary.getValuesByIds _).expects(TestTable.TAG_B, Set(2l)).returning(Map(2l -> "2"))
//////
//////    val res = dao.query(
//////      InternalQuery(
//////        TestTable,
//////        exprs.toSet,
//////        and(
//////          ge(time, const(Time(from))),
//////          lt(time, const(Time(to))),
//////          in(dimension(TestTable.TAG_A), manyAs.toSet),
//////          in(dimension(TestTable.TAG_B), manyBs.toSet)
//////        )
//////      ),
//////      valueDataBuilder,
//////      NoMetricCollector
//////    ).toList
//////
//////    res should have size 2
//////
//////    res(0).get(0).value shouldEqual Time(pointTime1)
//////    res(0).get(1).value shouldEqual "2"
//////    res(0).get(2).value shouldEqual "2"
//////    res(0).get(3).value shouldEqual 1d
//////
//////    res(1).get(0).value shouldEqual Time(pointTime2)
//////    res(1).get(1).value shouldEqual "2"
//////    res(1).get(2).value shouldEqual "2"
//////    res(1).get(3).value shouldEqual 5d
//////  }
//////
  it should "exclude NOT IN from IN" in withMock { (dao, dictionary, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression](time, dimension(TestDims.TAG_B), metric(TestTableFields.TEST_FIELD))
    val valueDataBuilder = new InternalRowBuilder(exprs.zipWithIndex.toMap)

    val pointTime1 = 2000

    queryRunner
      .expects(
        scan(from, to)
      )
      .returning(
        Iterator(
          HBaseTestUtils
            .row(TSDRowKey(pointTime1 - (pointTime1 % testTable.rowTimeSpan), Array(Some(2), Some(1))))
            .cell(pointTime1 % testTable.rowTimeSpan, 1, 3d)
            .cell(pointTime1 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "doesn't matter")
            .cell(pointTime1 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test3")
            .hbaseRow
        )
      )

    (dictionary.getIdsByValues _)
      .expects(TestDims.TAG_B, Set("test1", "test2"))
      .returning(Map("test1" -> 1, "test2" -> 2))
    (dictionary.getIdsByValues _).expects(TestDims.TAG_B, Set("test3")).returning(Map("test3" -> 3))

    dao.query(
      InternalQuery(
        testTable,
        exprs.toSet,
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          in(dimension(TestDims.TAG_B), Set("test1", "test2")),
          notIn(dimension(TestDims.TAG_B), Set("test2", "test3"))
        )
      ),
      valueDataBuilder,
      NoMetricCollector
    )
  }

  it should "filter by exclude conditions" in withMock { (dao, dictionary, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression](time, dimension(TestDims.TAG_B), metric(TestTableFields.TEST_FIELD))
    val valueDataBuilder = new InternalRowBuilder(exprs.zipWithIndex.toMap)

    val pointTime = 2000

    queryRunner
      .expects(
        scan(from, to)
      )
      .returning(
        Iterator(
          HBaseTestUtils
            .row(TSDRowKey(pointTime - (pointTime % testTable.rowTimeSpan), Array(Some(1), Some(2))))
            .cell(pointTime % testTable.rowTimeSpan, 1, 1d)
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "test11")
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test22")
            .hbaseRow,
          HBaseTestUtils
            .row(TSDRowKey(pointTime - (pointTime % testTable.rowTimeSpan), Array(Some(2), Some(2))))
            .cell(pointTime % testTable.rowTimeSpan, 1, 2d)
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "test12")
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test22")
            .hbaseRow,
          HBaseTestUtils
            .row(TSDRowKey(pointTime - (pointTime % testTable.rowTimeSpan), Array(Some(3), Some(2))))
            .cell(pointTime % testTable.rowTimeSpan, 1, 3d)
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "test13")
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test22")
            .hbaseRow,
          HBaseTestUtils
            .row(TSDRowKey(pointTime - (pointTime % testTable.rowTimeSpan), Array(Some(4), Some(3))))
            .cell(pointTime % testTable.rowTimeSpan, 1, 4d)
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "test14")
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test23")
            .hbaseRow,
          HBaseTestUtils
            .row(TSDRowKey(pointTime - (pointTime % testTable.rowTimeSpan), Array(Some(5), Some(2))))
            .cell(pointTime % testTable.rowTimeSpan, 1, 5d)
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "test15")
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test22")
            .hbaseRow
        )
      )

    (dictionary.getIdsByValues _)
      .expects(TestDims.TAG_A, Set("test11", "test12", "test14", "test15"))
      .returning(Map("test11" -> 1, "test12" -> 2, "test14" -> 4, "test15" -> 5))

    val results = dao
      .query(
        InternalQuery(
          testTable,
          exprs.toSet,
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            notIn(dimension(TestDims.TAG_A), Set("test11", "test12")),
            notIn(dimension(TestDims.TAG_A), Set("test12", "test15")),
            neq(dimension(TestDims.TAG_A), const("test14"))
          )
        ),
        valueDataBuilder,
        NoMetricCollector
      )
      .toList

    results should have size 1
  }

  it should "do nothing if exclude produce empty set" in withMock { (dao, dictionary, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression](time, dimension(TestDims.TAG_B), metric(TestTableFields.TEST_FIELD))
    val valueDataBuilder = new InternalRowBuilder(exprs.zipWithIndex.toMap)

    queryRunner.expects(Seq.empty).returning(Iterator.empty)

    (dictionary.getIdsByValues _)
      .expects(TestDims.TAG_A, Set("tagValue1"))
      .returning(Map("tagValue1" -> 1L))

    (dictionary.getIdsByValues _)
      .expects(TestDims.TAG_A, Set("tagValue2"))
      .returning(Map("tagValue2" -> 2L))

    val results = dao
      .query(
        InternalQuery(
          testTable,
          exprs.toSet,
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            notIn(dimension(TestDims.TAG_A), Set("tagValue1", "tagValue2")),
            equ(dimension(TestDims.TAG_A), const("tagValue1"))
          )
        ),
        valueDataBuilder,
        NoMetricCollector
      )
      .toList

    results shouldBe empty
  }

  it should "handle tag ID IN" in withMock { (dao, dictionary, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression](time, dimension(TestDims.TAG_B), metric(TestTableFields.TEST_FIELD))
    val valueDataBuilder = new InternalRowBuilder(exprs.zipWithIndex.toMap)

    val pointTime1 = 2000
    val pointTime2 = 2200

    queryRunner
      .expects(
        scan(
          from,
          to,
          Set(
            Seq(1L),
            Seq(2L)
          )
        )
      )
      .returning(
        Iterator(
          HBaseTestUtils
            .row(TSDRowKey(pointTime1 - (pointTime1 % testTable.rowTimeSpan), Array(Some(2), Some(5))))
            .cell(pointTime1 % testTable.rowTimeSpan, 1, 7d)
            .cell(pointTime2 % testTable.rowTimeSpan, 1, 5d)
            .cell(pointTime1 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "test12")
            .cell(pointTime2 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "test13")
            .cell(pointTime1 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test25")
            .cell(pointTime2 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test25")
            .hbaseRow
        )
      )

    val res = dao
      .query(
        InternalQuery(
          testTable,
          exprs.toSet,
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            DimIdInExpr(dimension(TestDims.TAG_A), SortedSetIterator(1, 2))
          )
        ),
        valueDataBuilder,
        NoMetricCollector
      )
      .toList

    res.size shouldEqual 2

    res(0).get(0).value shouldEqual Time(pointTime1)
    res(0).get(1).value shouldEqual "test25"
    res(0).get(2).value shouldEqual 7d

    res(1).get(0).value shouldEqual Time(pointTime2)
    res(1).get(1).value shouldEqual "test25"
    res(1).get(2).value shouldEqual 5d
  }

  it should "handle tag ID NOT IN condition" in withMock { (dao, dictionary, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression](time, dimension(TestDims.TAG_B), metric(TestTableFields.TEST_FIELD))
    val valueDataBuilder = new InternalRowBuilder(exprs.zipWithIndex.toMap)

    val pointTime = 2000

    queryRunner
      .expects(
        scan(from, to, Seq(1L))
      )
      .returning(
        Iterator(
          HBaseTestUtils
            .row(TSDRowKey(pointTime - (pointTime % testTable.rowTimeSpan), Array(Some(1), Some(2))))
            .cell(pointTime % testTable.rowTimeSpan, 1, 1d)
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "test11")
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test22")
            .hbaseRow
        )
      )

    (dictionary.getIdsByValues _)
      .expects(TestDims.TAG_A, Set("test11", "test12"))
      .returning(Map("test11" -> 1, "test12" -> 2))

    (dictionary.getIdsByValues _)
      .expects(TestDims.TAG_A, Set("test14"))
      .returning(Map("test14" -> 4))

    val results = dao
      .query(
        InternalQuery(
          testTable,
          exprs.toSet,
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(dimension(TestDims.TAG_A), Set("test11", "test12")),
            DimIdNotInExpr(dimension(TestDims.TAG_A), SortedSetIterator(2, 5)),
            neq(dimension(TestDims.TAG_A), const("test14"))
          )
        ),
        valueDataBuilder,
        NoMetricCollector
      )
      .toList

    results should have size 1
  }

  it should "support exact time values" in withMock { (dao, dictionaryDao, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression](time, dimension(TestDims.TAG_A), metric(TestTableFields.TEST_FIELD))
    val valueDataBuilder = new InternalRowBuilder(exprs.zipWithIndex.toMap)

    val pointTime = 2000

    (dictionaryDao.getIdsByValues _).expects(TestDims.TAG_A, Set("tag_a")).returning(Map("tag_a" -> 1))

    queryRunner
      .expects(
        scan(from, to, Seq(1L))
      )
      .returning(
        Iterator(
          HBaseTestUtils
            .row(TSDRowKey(pointTime - (pointTime % testTable.rowTimeSpan), Array(Some(1), Some(5))))
            .cell(pointTime % testTable.rowTimeSpan, 1, 7d)
            .cell((pointTime + 1) % testTable.rowTimeSpan, 1, 5d)
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "tag_a")
            .cell((pointTime + 1) % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "tag_a")
            .cell(pointTime % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test_b")
            .cell((pointTime + 1) % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test_b")
            .hbaseRow
        )
      )

    val res = dao
      .query(
        InternalQuery(
          testTable,
          exprs.toSet,
          and(
            equ(time, const(Time(pointTime))),
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            equ(dimension(TestDims.TAG_A), const("tag_a"))
          )
        ),
        valueDataBuilder,
        NoMetricCollector
      )
      .toList

    res should have size 1
    res.head.get(0).value shouldEqual Time(pointTime)
    res.head.get(1).value shouldEqual "tag_a"
    res.head.get(2).value shouldEqual 7d
  }

  it should "support EQ filter for tuples" in withMock { (dao, dictionaryDao, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression](time, dimension(TestDims.TAG_A), metric(TestTableFields.TEST_FIELD))
    val valueDataBuilder = new InternalRowBuilder(exprs.zipWithIndex.toMap)

    val pointTime1 = 2000
    val pointTime2 = 2500

    (dictionaryDao.getIdsByValues _).expects(TestDims.TAG_A, Set("test42")).returning(Map("test42" -> 42L))

    queryRunner
      .expects(
        scan(from, to, Seq(42L))
      )
      .returning(
        Iterator(
          HBaseTestUtils
            .row(TSDRowKey(pointTime1 - (pointTime1 % testTable.rowTimeSpan), Array(Some(42), Some(5))))
            .cell(pointTime1 % testTable.rowTimeSpan, 1, 7d)
            .cell(pointTime2 % testTable.rowTimeSpan, 1, 5d)
            .cell(pointTime1 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "test42")
            .cell(pointTime2 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "test42")
            .cell(pointTime1 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test25")
            .cell(pointTime2 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test25")
            .hbaseRow
        )
      )

    val res = dao
      .query(
        InternalQuery(
          testTable,
          exprs.toSet,
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            equ(tuple(time, dimension(TestDims.TAG_A)), const((Time(pointTime2), "test42")))
          )
        ),
        valueDataBuilder,
        NoMetricCollector
      )
      .toList

    res should have size 1
    res.head.get(0).value shouldEqual Time(pointTime2)
    res.head.get(1).value shouldEqual "test42"
    res.head.get(2).value shouldEqual 5d

  }

  it should "perform pre-filtering by IN for tuples" in withMock { (dao, dictionaryDao, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression](time, dimension(TestDims.TAG_A), metric(TestTableFields.TEST_FIELD))
    val valueDataBuilder = new InternalRowBuilder(exprs.zipWithIndex.toMap)

    val pointTime1 = 1010
    val pointTime2 = 1020
    val pointTime3 = 1030

    (dictionaryDao.getIdsByValues _)
      .expects(TestDims.TAG_A, Set("test42", "test51"))
      .returning(Map("test42" -> 42L, "test51" -> 51L))

    queryRunner
      .expects(
        scan(
          from,
          to,
          Set(
            Seq(42L),
            Seq(51L)
          )
        )
      )
      .returning(
        Iterator(
          HBaseTestUtils
            .row(TSDRowKey(pointTime1 - (pointTime1 % testTable.rowTimeSpan), Array(Some(42), Some(5))))
            .cell(pointTime1 % testTable.rowTimeSpan, 1, 7d)
            .cell(pointTime2 % testTable.rowTimeSpan, 1, 5d)
            .cell(pointTime3 % testTable.rowTimeSpan, 1, 8d)
            .cell(pointTime1 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "test42")
            .cell(pointTime2 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "test42")
            .cell(pointTime3 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "test42")
            .cell(pointTime1 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test25")
            .cell(pointTime2 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test25")
            .cell(pointTime3 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test25")
            .hbaseRow,
          HBaseTestUtils
            .row(TSDRowKey(pointTime1 - (pointTime1 % testTable.rowTimeSpan), Array(Some(42), Some(5))))
            .cell(pointTime1 % testTable.rowTimeSpan, 1, 15d)
            .cell(pointTime2 % testTable.rowTimeSpan, 1, 33d)
            .cell(pointTime3 % testTable.rowTimeSpan, 1, 43d)
            .cell(pointTime1 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "test51")
            .cell(pointTime2 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "test51")
            .cell(pointTime3 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET, "test51")
            .cell(pointTime1 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test26")
            .cell(pointTime2 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test26")
            .cell(pointTime3 % testTable.rowTimeSpan, Table.DIM_TAG_OFFSET + 1, "test26")
            .hbaseRow
        )
      )

    val res = dao
      .query(
        InternalQuery(
          testTable,
          exprs.toSet,
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(tuple(time, dimension(TestDims.TAG_A)), Set((Time(pointTime2), "test42"), (Time(pointTime1), "test51")))
          )
        ),
        valueDataBuilder,
        NoMetricCollector
      )
      .toList

    res should have size 4
    res.head.get(0).value shouldEqual Time(pointTime1)
    res.head.get(1).value shouldEqual "test42"
    res.head.get(2).value shouldEqual 7d

    res(1).get(0).value shouldEqual Time(pointTime2)
    res(1).get(1).value shouldEqual "test42"
    res(1).get(2).value shouldEqual 5d

    res(2).get(0).value shouldEqual Time(pointTime1)
    res(2).get(1).value shouldEqual "test51"
    res(2).get(2).value shouldEqual 15d

    res(3).get(0).value shouldEqual Time(pointTime2)
    res(3).get(1).value shouldEqual "test51"
    res(3).get(2).value shouldEqual 33d
  }

  class TestDao(override val dictionaryProvider: DictionaryProvider, queryRunner: QueryRunner)
      extends TSDaoHBaseBase[Iterator] {
    override def mapReduceEngine(metricQueryCollector: MetricQueryCollector): MapReducible[Iterator] =
      MapReducible.iteratorMR

    override def executeScans(
        queryContext: InternalQueryContext,
        from: IdType,
        to: IdType,
        rangeScanDims: Iterator[Map[Dimension, Seq[IdType]]]
    ): Iterator[Result] = {
      val scans = rangeScanDims.map { dimIds =>
        val filter = HBaseUtils.multiRowRangeFilter(queryContext.table, from, to, dimIds)
        HBaseUtils.createScan(queryContext, filter, Seq.empty, from, to)
      }
      queryRunner(scans.toSeq)
    }
  }

  def withMock(body: (TestDao, DictionaryDao, QueryRunner) => Unit): Unit = {
    val exec = mockFunction[Seq[Scan], Iterator[Result]]
    val dictionaryDaoMock = mock[DictionaryDao]
    val dicionaryProvider = new DictionaryProviderImpl(dictionaryDaoMock)
    val dao = new TestDao(dicionaryProvider, exec)
    body(dao, dictionaryDaoMock, exec)
  }

  def tagged[T](tag: Byte, value: T)(implicit writable: Writable[T]): Array[Byte] = {
    tag +: writable.write(value)
  }

  def dimBytes(values: String*) = {
    values.zipWithIndex.foldLeft(Array.ofDim[Byte](0)) {
      case (aac, (dim, idx)) =>
        aac ++ tagged((Table.DIM_TAG_OFFSET + idx).toByte, dim)
    }
  }
}

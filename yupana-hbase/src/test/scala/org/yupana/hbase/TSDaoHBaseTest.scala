package org.yupana.hbase

import org.apache.hadoop.hbase.client.{ Scan, Result => HResult }
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter
import org.apache.hadoop.hbase.util.Bytes
import org.scalamock.function.{ FunctionAdapter1, MockFunction1 }
import org.scalamock.scalatest.MockFactory
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.api.schema.{ Dimension, Schema, Table }
import org.yupana.api.types.ByteReaderWriter
import org.yupana.api.utils.SortedSetIterator
import org.yupana.cache.CacheFactory
import org.yupana.core._
import org.yupana.core.dao.{ DictionaryDao, DictionaryProvider, DictionaryProviderImpl }
import org.yupana.core.model._
import org.yupana.core.utils.metric.{ MetricQueryCollector, NoMetricCollector }
import org.yupana.serialization.{ MemoryBuffer, MemoryBufferEvalReaderWriter }
import org.yupana.settings.Settings
import org.yupana.utils.RussianTokenizer

import java.util.Properties
import scala.jdk.CollectionConverters._

class TSDaoHBaseTest
    extends AnyFlatSpec
    with Matchers
    with MockFactory
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with OptionValues {

  import HBaseTestUtils._
  import TestSchema._
  import org.yupana.api.query.syntax.All._

  type QueryRunner = MockFunction1[Seq[Scan], Iterator[HResult]]

  implicit private val calculator: ConstantCalculator = new ConstantCalculator(RussianTokenizer)

  override protected def beforeAll(): Unit = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("app.properties"))
    CacheFactory.init(Settings(properties))
  }

  override protected def beforeEach(): Unit = {
    CacheFactory.flushCaches()
  }

  private def baseTime(time: Long) = {
    time - (time % testTable.rowTimeSpan)
  }

  private def scan(from: Long, to: Long): FunctionAdapter1[Seq[Scan], Boolean] = {
    where { (scans: Seq[Scan]) =>
      val scan = scans.head
      baseTime(from) == Bytes.toLong(scan.getStartRow) &&
      baseTime(to) == (Bytes.toLong(scan.getStopRow) - 1)
    }
  }

  def scan(table: Table, from: Long, to: Long, range: Seq[Any]): FunctionAdapter1[Seq[Scan], Boolean] = {
    scanMultiRanges(table, from, to, Set(range))
  }

  def scanMultiRanges(
      table: Table,
      from: Long,
      to: Long,
      ranges: Set[Seq[Any]]
  ): FunctionAdapter1[Seq[Scan], Boolean] = {

    implicit val rw: ByteReaderWriter[MemoryBuffer] = MemoryBufferEvalReaderWriter

    where { (scans: Seq[Scan]) =>
      val scan = scans.head
      val filter = scan.getFilter.asInstanceOf[MultiRowRangeFilter]
      val rowRanges = filter.getRowRanges.asScala

      val rangesChecks = for {
        time <- baseTime(from) to baseTime(to) by table.rowTimeSpan
        range <- ranges
      } yield {
        rowRanges.exists { rowRange =>
          var offset = 8
          val valuesAndLimits = range.zip(table.dimensionSeq).map {
            case (id, dim) =>
              val start =
                dim.rStorable.read(MemoryBuffer.ofBytes(rowRange.getStartRow).asSlice(offset, dim.rStorable.size))
              val stop =
                dim.rStorable.read(MemoryBuffer.ofBytes(rowRange.getStopRow).asSlice(offset, dim.rStorable.size))

              val tid = id.asInstanceOf[dim.R]
              val allZeros = (offset to (offset + dim.rStorable.size))
                .forall(i => rowRange.getStopRow()(i) == 0)

              offset += dim.rStorable.size
              (tid, start, stop, dim.rOrdering, allZeros)
          }

          val goodStart = valuesAndLimits.forall {
            case (i, b, _, ord, _) => ord.gte(i, b)
          }

          val valuable = valuesAndLimits.reverse.dropWhile(_._5).toList

          val goodStop = valuable match {
            case (i, _, e, ord, _) :: tail =>
              ord.lt(i, e) && tail.forall {
                case (i, _, e, ord, _) => ord.lte(i, e)
              }
            case _ => true
          }

          goodStart && goodStop &&
          time == Bytes.toLong(rowRange.getStartRow) &&
          time == Bytes.toLong(rowRange.getStopRow)
        }
      }

      rangesChecks.forall(_ == true) &&
      baseTime(from) == Bytes.toLong(scan.getStartRow) &&
      baseTime(to) == Bytes.toLong(scan.getStopRow)
    }
  }

  "TSDaoHBase" should "handle empty result" in withMock { (dao, _, queryRunner) =>

    val from = 1000
    val to = 5000
    val exprs = Seq[Expression[_]](time, dimension(TestDims.DIM_A), metric(TestTableFields.TEST_FIELD))

    queryRunner
      .expects(scan(from, to))
      .returning(
        Iterator.empty
      )

    val res = dao
      .query(
        InternalQuery(testTable, exprs.toSet, and(ge(time, const(Time(from))), lt(time, const(Time(to))))),
        null,
        new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable)),
        NoMetricCollector
      )
      .toList

    res.size shouldEqual 0
  }

  it should "execute time bounded queries" in withMock { (dao, _, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression[_]](time, dimension(TestDims.DIM_A), metric(TestTableFields.TEST_FIELD))
    val pointTime = 2000

    queryRunner
      .expects(scan(from, to))
      .returning(
        Iterator(
          HBaseTestUtils
            .row(pointTime - (pointTime % testTable.rowTimeSpan), dimAHash("test1"), 2.toShort)
            .cell("d1", pointTime % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 1d)
            .field(Table.DIM_TAG_OFFSET, "test1")
            .hbaseRow
        )
      )

    val res = dao
      .query(
        InternalQuery(testTable, exprs.toSet, and(ge(time, const(Time(from))), lt(time, const(Time(to))))),
        null,
        new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable)),
        NoMetricCollector
      )
      .toList

    res.size shouldEqual 1

    val batch = res.head
    batch.size shouldEqual 1
    batch.get[Time](0, 0) shouldEqual Time(pointTime)
    batch.get[String](0, 1) shouldEqual "test1"
    batch.get[Double](0, 2) shouldEqual 1d
  }

  it should "support different time conditions" in withMock { (dao, _, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression[_]](time, dimension(TestDims.DIM_A))
    val pointTime = 2000

    queryRunner
      .expects(scan(from + 1, to + 1))
      .returning(
        Iterator(
          HBaseTestUtils
            .row(pointTime - (pointTime % testTable.rowTimeSpan), dimAHash("test2"), 2.toShort)
            .cell("d1", pointTime % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 1d)
            .field(Table.DIM_TAG_OFFSET, "test2")
            .hbaseRow
        )
      )

    val res = dao
      .query(
        InternalQuery(testTable, exprs.toSet, and(gt(time, const(Time(from))), le(time, const(Time(to))))),
        null,
        new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable)),
        NoMetricCollector
      )
      .toList

    res.size shouldEqual 1

    val batch = res.head
    batch.size shouldEqual 1
    batch.get[Time](0, 0) shouldEqual Time(pointTime)
    batch.get[String](0, 1) shouldEqual "test2"
  }

  it should "use most strict time conditions" in withMock { (dao, _, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression[_]](time, metric(TestTableFields.TEST_FIELD))
    val pointTime = 2000

    queryRunner
      .expects(scan(from + 100 + 1, to))
      .returning(
        Iterator(
          HBaseTestUtils
            .row(pointTime - (pointTime % testTable.rowTimeSpan), dimAHash("test1"), 2.toShort)
            .cell("d1", pointTime % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 3d)
            .field(Table.DIM_TAG_OFFSET, "test1")
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
            lt(const(Time(from + 100)), time),
            gt(const(Time(to)), time),
            ge(const(Time(to)), time)
          )
        ),
        null,
        new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable)),
        NoMetricCollector
      )
      .toList

    res.size shouldEqual 1

    val batch = res.head
    batch.size shouldEqual 1

    batch.get[Time](0, 0) shouldEqual Time(pointTime)
    batch.get[Double](0, 1) shouldEqual 3d
  }

  it should "skip values with fields not defined in schema" in withMock { (dao, _, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs =
      Seq[Expression[_]](time, dimension(TestDims.DIM_A), dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD))
    val pointTime = 2000

    queryRunner
      .expects(scan(testTable, from, to, Seq(dimAHash("test1"))))
      .returning(
        Iterator(
          HBaseTestUtils
            .row(pointTime - (pointTime % testTable.rowTimeSpan), dimAHash("test1"), 2.toShort)
            .cell("d1", pointTime % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 3d)
            .field(Table.DIM_TAG_OFFSET, "test1")
            .cell("d1", (pointTime + 1) % testTable.rowTimeSpan)
            .field(111, 1d)
            .field(Table.DIM_TAG_OFFSET, "test1")
            .hbaseRow
        )
      )

    val res = dao
      .query(
        InternalQuery(
          testTable,
          exprs.toSet,
          and(ge(time, const(Time(from))), lt(time, const(Time(to))), equ(dimension(TestDims.DIM_A), const("test1")))
        ),
        null,
        new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable)),
        NoMetricCollector
      )
      .toList

    val batch = res.head
    batch.size shouldEqual 2

    batch.get[Time](0, 0) shouldEqual Time(pointTime)
    batch.get[String](0, 1) shouldEqual "test1"
    batch.get[Short](0, 2) shouldEqual 2.toShort
    batch.get[Double](0, 3) shouldEqual 3d

    batch.get[Time](1, 0) shouldEqual Time(pointTime + 1)
    batch.isNull(1, 1) shouldBe true // should be "test1" but storage format does not allow this
    batch.get[Short](1, 2) shouldEqual 2.toShort // should be "test22" but storage format does not allow this
    batch.isNull(1, 3) shouldBe true
  }

  it should "set tag filter for equ" in withMock { (dao, _, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs =
      Seq[Expression[_]](time, dimension(TestDims.DIM_A), dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD))
    val pointTime = 2000

    queryRunner
      .expects(scan(testTable, from, to, Seq(dimAHash("test1"))))
      .returning(
        Iterator(
          HBaseTestUtils
            .row(pointTime - (pointTime % testTable.rowTimeSpan), dimAHash("test1"), 2.toShort)
            .cell("d1", pointTime % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 1d)
            .field(Table.DIM_TAG_OFFSET, "test1")
            .hbaseRow
        )
      )

    val res = dao
      .query(
        InternalQuery(
          testTable,
          exprs.toSet,
          and(ge(time, const(Time(from))), lt(time, const(Time(to))), equ(dimension(TestDims.DIM_A), const("test1")))
        ),
        null,
        new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable)),
        NoMetricCollector
      )
      .toList

    val batch = res.head
    batch.size shouldEqual 1

    batch.get[Time](0, 0) shouldEqual Time(pointTime)
    batch.get[String](0, 1) shouldEqual "test1"
    batch.get[Short](0, 2) shouldEqual 2.toShort
    batch.get[Double](0, 3) shouldEqual 1d
  }

  it should "handle tag repr overflow while filtering" in withMock { (dao, _, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs =
      Seq[Expression[_]](time, dimension(TestDims.DIM_A), dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD))
    val pointTime = 2000

    queryRunner
      .expects(scan(testTable3, from, to, Seq(dimAHash("test1"), -1.toShort)))
      .returning(
        Iterator(
          HBaseTestUtils
            .row(pointTime - (pointTime % testTable3.rowTimeSpan), dimAHash("test1"), 2.toShort, 1L)
            .cell("d1", pointTime % testTable3.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 1d)
            .field(Table.DIM_TAG_OFFSET, "test1")
            .hbaseRow
        )
      )

    val res = dao
      .query(
        InternalQuery(
          testTable3,
          exprs.toSet,
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            equ(dimension(TestDims.DIM_A), const("test1")),
            equ(dimension(TestDims.DIM_B), const(-1.toShort))
          )
        ),
        null,
        new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable3)),
        NoMetricCollector
      )
      .toList

    val batch = res.head
    batch.size shouldEqual 1
    batch.get[Time](0, 0) shouldEqual Time(pointTime)
    batch.get[String](0, 1) shouldEqual "test1"
    batch.get[Short](0, 2) shouldEqual 2.toShort
    batch.get[Double](0, 3) shouldEqual 1d
  }

//  it should "support not create queries if dimension value is not found" in withMock { (dao, dictionary, queryRunner) =>
//    val from = 1000
//    val to = 5000
//    val exprs = Seq[Expression[_]](time, dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD))
//
//    queryRunner.expects(Seq.empty).returning(Iterator.empty)
//
//    (dictionary.getIdsByValues _).expects(TestDims.DIM_A, Set("test1")).returning(Map.empty)
//
//    val res = dao
//      .query(
//        InternalQuery(
//          testTable,
//          exprs.toSet,
//          and(ge(time, const(Time(from))), lt(time, const(Time(to))), equ(dimension(TestDims.DIM_A), const("test1")))
//        ),
//        new InternalRowSchema(valExprIndex(exprs), refExprIndex(exprs), Some(testTable)),
//        NoMetricCollector
//      )
//      .toList
//
//    res shouldBe empty
//  }

  it should "support IN operation for tags" in withMock { (dao, _, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression[_]](time, dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD))

    val pointTime1 = 2000
    val pointTime2 = 2200

    queryRunner
      .expects(scanMultiRanges(testTable, from, to, Set(Seq(dimAHash("test1")), Seq(dimAHash("test2")))))
      .returning(
        Iterator(
          HBaseTestUtils
            .row(pointTime1 - (pointTime1 % testTable.rowTimeSpan), dimAHash("test2"), 5.toShort)
            .cell("d1", pointTime1 % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 7d)
            .field(Table.DIM_TAG_OFFSET, "test1")
            .cell("d1", pointTime2 % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 5d)
            .field(Table.DIM_TAG_OFFSET, "test1")
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
            in(dimension(TestDims.DIM_A), Set("test1", "test2"))
          )
        ),
        null,
        new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable)),
        NoMetricCollector
      )
      .toList

    val batch = res.head
    batch.size shouldEqual 2

    batch.get[Time](0, 0) shouldEqual Time(pointTime1)
    batch.get[Short](0, 1) shouldEqual 5.toShort
    batch.get[Double](0, 2) shouldEqual 7d

    batch.get[Time](1, 0) shouldEqual Time(pointTime2)
    batch.get[Short](1, 1) shouldEqual 5.toShort
    batch.get[Double](1, 2) shouldEqual 5d
  }

  it should "do nothing if IN values are empty" in withMock { (dao, _, _) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression[_]](time, dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD))

    val res = dao
      .query(
        InternalQuery(
          testTable,
          exprs.toSet,
          and(ge(time, const(Time(from))), lt(time, const(Time(to))), in(dimension(TestDims.DIM_A), Set()))
        ),
        null,
        new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable)),
        NoMetricCollector
      )
      .toList

    res shouldBe empty
  }

  it should "intersect different conditions for same tag" in withMock { (dao, _, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression[_]](time, dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD))

    val pointTime1 = 2000

    queryRunner
      .expects(
        scan(testTable, from, to, Seq(dimAHash("test2"), 21.toShort))
      )
      .returning(
        Iterator(
          HBaseTestUtils
            .row(pointTime1 - (pointTime1 % testTable.rowTimeSpan), dimAHash("test2"), 21.toShort)
            .cell("d1", pointTime1 % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 3d)
            .field(Table.DIM_TAG_OFFSET, "test2")
            .hbaseRow
        )
      )

    dao
      .query(
        InternalQuery(
          testTable,
          exprs.toSet,
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(dimension(TestDims.DIM_A), Set("test1", "test2")),
            equ(dimension(TestDims.DIM_B), const(21.toShort)),
            in(dimension(TestDims.DIM_A), Set("test2", "test3"))
          )
        ),
        null,
        new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable)),
        NoMetricCollector
      )
      .toList
  }

  it should "cross join different IN conditions for different tags" in withMock { (dao, _, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs =
      Seq[Expression[_]](time, dimension(TestDims.DIM_A), dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD))

    val pointTime = 2000

    queryRunner
      .expects(
        scanMultiRanges(
          testTable,
          from,
          to,
          Set(
            Seq(dimAHash("A 1"), 1.toShort),
            Seq(dimAHash("A 1"), 2.toShort),
            Seq(dimAHash("A 2"), 1.toShort),
            Seq(dimAHash("A 2"), 2.toShort),
            Seq(dimAHash("A 3"), 1.toShort),
            Seq(dimAHash("A 3"), 2.toShort)
          )
        )
      )
      .returning(
        Iterator(
          HBaseTestUtils
            .row(pointTime - (pointTime % testTable.rowTimeSpan), dimAHash("A 1"), 1.toShort)
            .cell("d1", pointTime % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 1d)
            .field(Table.DIM_TAG_OFFSET, "A 1")
            .hbaseRow,
          HBaseTestUtils
            .row(pointTime - (pointTime % testTable.rowTimeSpan), dimAHash("A 2"), 1.toShort)
            .cell("d1", pointTime % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 3d)
            .field(Table.DIM_TAG_OFFSET, "A 2")
            .hbaseRow,
          HBaseTestUtils
            .row(pointTime - (pointTime % testTable.rowTimeSpan), dimAHash("A 2"), 2.toShort)
            .cell("d1", pointTime % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 4d)
            .field(Table.DIM_TAG_OFFSET, "A 2")
            .hbaseRow,
          HBaseTestUtils
            .row(pointTime - (pointTime % testTable.rowTimeSpan), dimAHash("A 3"), 2.toShort)
            .cell("d1", pointTime % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 6d)
            .field(Table.DIM_TAG_OFFSET, "A 3")
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
            in(dimension(TestDims.DIM_A), Set("A 1", "A 2", "A 3")),
            in(dimension(TestDims.DIM_B), Set(1.toShort, 2.toShort))
          )
        ),
        null,
        new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable)),
        NoMetricCollector
      )
      .toList

    res.head should have size 4
  }

  it should "cross join in and eq for different tags" in withMock { (dao, dictionary, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs =
      Seq[Expression[_]](time, dimension(TestDims.DIM_A), dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD))

    val pointTime = 2000

    queryRunner
      .expects(
        scanMultiRanges(
          testTable3,
          from,
          to,
          Set(
            Seq(dimAHash("A 1"), 1.toShort, 42L),
            Seq(dimAHash("A 2"), 1.toShort, 42L),
            Seq(dimAHash("A 3"), 1.toShort, 42L)
          )
        )
      )
      .returning(
        Iterator(
          HBaseTestUtils
            .row(pointTime - (pointTime % testTable3.rowTimeSpan), dimAHash("A 1"), 1.toShort, 42L)
            .cell("d1", pointTime % testTable3.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 1d)
            .field(Table.DIM_TAG_OFFSET, "A 1")
            .field(Table.DIM_TAG_OFFSET + 1, "X 2")
            .hbaseRow,
          HBaseTestUtils
            .row(pointTime - (pointTime % testTable3.rowTimeSpan), dimAHash("A 2"), 1.toShort, 42L)
            .cell("d1", pointTime % testTable3.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 3d)
            .field(Table.DIM_TAG_OFFSET, "A 2")
            .field(Table.DIM_TAG_OFFSET + 1, "X 2")
            .hbaseRow
        )
      )

    (dictionary.getIdsByValues _)
      .expects(TestDims.DIM_X, Set("X 1", "X 2"))
      .returning(Map("X 2" -> 42L))

    val res = dao
      .query(
        InternalQuery(
          testTable3,
          exprs.toSet,
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(dimension(TestDims.DIM_A), Set("A 1", "A 2", "A 3")),
            equ(dimension(TestDims.DIM_B), const(1.toShort)),
            in(dimension(TestDims.DIM_X), Set("X 1", "X 2"))
          )
        ),
        null,
        new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable3)),
        NoMetricCollector
      )
      .toList

    res.head should have size 2
  }

  it should "use post filter if there are too many combinations" in withMock { (dao, _, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs =
      Seq[Expression[_]](time, dimension(TestDims.DIM_A), dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD))

    val pointTime1 = 2000
    val pointTime2 = 2500

    val manyAs = (1 to 200).map(x => s"A $x")
    val manyBs = (1 to 3000).map(_.toShort)

    queryRunner
      .expects(scanMultiRanges(testTable, from, to, manyAs.map(s => Seq(dimAHash(s))).toSet))
      .returning(
        Iterator(
          HBaseTestUtils
            .row(pointTime1 - (pointTime1 % testTable.rowTimeSpan), dimAHash("A 1"), 2.toShort)
            .cell("d1", pointTime1 % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 1d)
            .field(Table.DIM_TAG_OFFSET, "A 1")
            .cell("d1", pointTime2 % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 5d)
            .field(Table.DIM_TAG_OFFSET, "A 1")
            .hbaseRow,
          HBaseTestUtils
            .row(pointTime1 - (pointTime1 % testTable.rowTimeSpan), dimAHash("A 2"), 6000.toShort)
            .cell("d1", pointTime1 % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 2d)
            .field(Table.DIM_TAG_OFFSET, "A 2")
            .cell("d1", pointTime2 % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 3d)
            .field(Table.DIM_TAG_OFFSET, "A 2")
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
            in(dimension(TestDims.DIM_A), manyAs.toSet),
            in(dimension(TestDims.DIM_B), manyBs.toSet)
          )
        ),
        null,
        new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable)),
        NoMetricCollector
      )
      .toList

    val batch = res.head
    batch.size shouldEqual 2

    batch.get[Time](0, 0) shouldEqual Time(pointTime1)
    batch.get[String](0, 1) shouldEqual "A 1"
    batch.get[Short](0, 2) shouldEqual 2
    batch.get[Double](0, 3) shouldEqual 1d

    batch.get[Time](1, 0) shouldEqual Time(pointTime2)
    batch.get[String](1, 1) shouldEqual "A 1"
    batch.get[Short](1, 2) shouldEqual 2
    batch.get[Double](1, 3) shouldEqual 5d
  }

  it should "exclude NOT IN from IN" in withMock { (dao, _, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression[_]](time, dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD))

    val pointTime1 = 2000

    queryRunner
      .expects(
        scan(from, to)
      )
      .returning(
        Iterator(
          HBaseTestUtils
            .row(pointTime1 - (pointTime1 % testTable.rowTimeSpan), (2, 2L), 1.toShort)
            .cell("d1", pointTime1 % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 1d)
            .field(Table.DIM_TAG_OFFSET, "doesn't matter")
            .hbaseRow
        )
      )

    dao
      .query(
        InternalQuery(
          testTable,
          exprs.toSet,
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(dimension(TestDims.DIM_B), Set(1.toShort, 2.toShort)),
            notIn(dimension(TestDims.DIM_B), Set(2.toShort, 3.toShort))
          )
        ),
        null,
        new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable)),
        NoMetricCollector
      )
      .toList
  }

  it should "filter by exclude conditions" in withMock { (dao, _, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression[_]](time, dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD))

    val pointTime = 2000

    queryRunner
      .expects(
        scan(from, to)
      )
      .returning(
        Iterator(
          HBaseTestUtils
            .row(pointTime - (pointTime % testTable.rowTimeSpan), dimAHash("test11"), 2.toShort)
            .cell("d1", pointTime % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 1d)
            .field(Table.DIM_TAG_OFFSET, "test11")
            .hbaseRow,
          HBaseTestUtils
            .row(pointTime - (pointTime % testTable.rowTimeSpan), dimAHash("test12"), 2.toShort)
            .cell("d1", pointTime % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 2d)
            .field(Table.DIM_TAG_OFFSET, "test12")
            .hbaseRow,
          HBaseTestUtils
            .row(pointTime - (pointTime % testTable.rowTimeSpan), dimAHash("test13"), 2.toShort)
            .cell("d1", pointTime % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 3d)
            .field(Table.DIM_TAG_OFFSET, "test13")
            .hbaseRow,
          HBaseTestUtils
            .row(pointTime - (pointTime % testTable.rowTimeSpan), dimAHash("test14"), 3.toShort)
            .cell("d1", pointTime % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 4d)
            .field(Table.DIM_TAG_OFFSET, "test14")
            .hbaseRow,
          HBaseTestUtils
            .row(pointTime - (pointTime % testTable.rowTimeSpan), dimAHash("test15"), 2.toShort)
            .cell("d1", pointTime % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 5d)
            .field(Table.DIM_TAG_OFFSET, "test15")
            .hbaseRow
        )
      )

    val results = dao
      .query(
        InternalQuery(
          testTable,
          exprs.toSet,
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            notIn(dimension(TestDims.DIM_A), Set("test11", "test12")),
            notIn(dimension(TestDims.DIM_A), Set("test12", "test15")),
            neq(dimension(TestDims.DIM_A), const("test14"))
          )
        ),
        null,
        new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable)),
        NoMetricCollector
      )
      .toList

    results.head should have size 1
  }

  it should "do nothing if exclude produce empty set" in withMock { (dao, _, _) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression[_]](time, dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD))

    val results = dao
      .query(
        InternalQuery(
          testTable,
          exprs.toSet,
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            notIn(dimension(TestDims.DIM_A), Set("tagValue1", "tagValue2")),
            equ(dimension(TestDims.DIM_A), const("tagValue1"))
          )
        ),
        null,
        new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable)),
        NoMetricCollector
      )
      .toList

    results.size shouldEqual 0
  }

  it should "handle tag ID IN" in withMock { (dao, _, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression[_]](time, dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD))

    val pointTime1 = 2000
    val pointTime2 = 2200

    queryRunner
      .expects(
        scanMultiRanges(
          testTable,
          from,
          to,
          Set(
            Seq(dimAHash("test12")),
            Seq(dimAHash("test22"))
          )
        )
      )
      .returning(
        Iterator(
          HBaseTestUtils
            .row(pointTime1 - (pointTime1 % testTable.rowTimeSpan), dimAHash("test12"), 5.toShort)
            .cell("d1", pointTime1 % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 7d)
            .field(Table.DIM_TAG_OFFSET, "test12")
            .cell("d1", pointTime2 % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 5d)
            .field(Table.DIM_TAG_OFFSET, "test12")
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
            DimIdInExpr(TestDims.DIM_A, SortedSetIterator(dimAHash("test12"), dimAHash("test22")))
          )
        ),
        null,
        new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable)),
        NoMetricCollector
      )
      .toList

    val batch = res.head
    batch.size shouldEqual 2

    batch.get[Time](0, 0) shouldEqual Time(pointTime1)
    batch.get[Short](0, 1) shouldEqual 5.toShort
    batch.get[Double](0, 2) shouldEqual 7d

    batch.get[Time](1, 0) shouldEqual Time(pointTime2)
    batch.get[Short](1, 1) shouldEqual 5.toShort
    batch.get[Double](1, 2) shouldEqual 5d
  }

  it should "handle tag ID NOT IN condition" in withMock { (dao, _, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression[_]](time, dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD))

    val pointTime = 2000

    queryRunner
      .expects(
        scan(testTable, from, to, Seq(dimAHash("test12")))
      )
      .returning(
        Iterator(
          HBaseTestUtils
            .row(pointTime - (pointTime % testTable.rowTimeSpan), dimAHash("test12"), 2.toShort)
            .cell("d1", pointTime % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 1d)
            .field(Table.DIM_TAG_OFFSET, "test12")
            .hbaseRow
        )
      )

    val results = dao
      .query(
        InternalQuery(
          testTable,
          exprs.toSet,
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            in(dimension(TestDims.DIM_A), Set("test11", "test12")),
            DimIdNotInExpr(TestDims.DIM_A, SortedSetIterator(dimAHash("test11"), dimAHash("test15"))),
            neq(dimension(TestDims.DIM_A), const("test14"))
          )
        ),
        null,
        new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable)),
        NoMetricCollector
      )
      .toList

    results.head should have size 1
  }

  it should "support dimension id eq" in withMock { (dao, _, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression[_]](time, dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD))

    val pointTime1 = 2000

    queryRunner
      .expects(
        scanMultiRanges(
          testTable,
          from,
          to,
          Set(
            Seq((1234, 12345678L))
          )
        )
      )
      .returning(
        Iterator(
          HBaseTestUtils
            .row(pointTime1 - (pointTime1 % testTable.rowTimeSpan), (1234, 12345678L), 5.toShort)
            .cell("d1", pointTime1 % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 7d)
            .field(Table.DIM_TAG_OFFSET, "test12")
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
            equ(DimensionIdExpr(TestDims.DIM_A), const("000004d20000000000bc614e"))
          )
        ),
        null,
        new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable)),
        NoMetricCollector
      )
      .toList

    val batch = res.head
    batch.size shouldEqual 1

    batch.get[Time](0, 0) shouldEqual Time(pointTime1)
    batch.get[Short](0, 1) shouldEqual 5.toShort
    batch.get[Double](0, 2) shouldEqual 7d
  }

  it should "support dimension id not eq" in withMock { (dao, dictionary, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs =
      Seq[Expression[_]](time, dimension(TestDims.DIM_B), metric(TestTableFields.TEST_FIELD), dimension(TestDims.DIM_X))

    val pointTime1 = 2000

    queryRunner
      .expects(
        scanMultiRanges(
          testTable3,
          from,
          to,
          Set(
            Seq(dimAHash("test me"), 42.toShort, 3L)
          )
        )
      )
      .returning(
        Iterator(
          HBaseTestUtils
            .row(pointTime1 - (pointTime1 % testTable3.rowTimeSpan), (1234, 12345678L), 5.toShort, 3L)
            .cell("d1", pointTime1 % testTable3.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 7d)
            .field(Table.DIM_TAG_OFFSET, "test12")
            .field(Table.DIM_TAG_OFFSET + 2, "Bar")
            .hbaseRow
        )
      )

    (dictionary.getIdsByValues _)
      .expects(TestDims.DIM_X, Set("Foo", "Bar", "Baz"))
      .returning(Map("Foo" -> 1L, "Bar" -> 3L))

    val res = dao
      .query(
        InternalQuery(
          testTable3,
          exprs.toSet,
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            equ(dimension(TestDims.DIM_A), const("test me")),
            equ(dimension(TestDims.DIM_B), const(42.toShort)),
            in(dimension(TestDims.DIM_X), Set("Foo", "Bar", "Baz")),
            neq(DimensionIdExpr(TestDims.DIM_X), const("0000000000000001"))
          )
        ),
        null,
        new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable3)),
        NoMetricCollector
      )
      .toList

    val batch = res.head
    batch.size shouldEqual 1

    batch.get[Time](0, 0) shouldEqual Time(pointTime1)
    batch.get[Short](0, 1) shouldEqual 5.toShort
    batch.get[Double](0, 2) shouldEqual 7d
    batch.get[String](0, 3) shouldEqual "Bar"
  }

  it should "support exact time values" in withMock { (dao, _, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression[_]](time, dimension(TestDims.DIM_A), metric(TestTableFields.TEST_FIELD))

    val pointTime = 2000

    queryRunner
      .expects(
        scan(testTable, from, to, Seq(dimAHash("tag_a")))
      )
      .returning(
        Iterator(
          HBaseTestUtils
            .row(pointTime - (pointTime % testTable.rowTimeSpan), dimAHash("tag_a"), 5.toShort)
            .cell("d1", pointTime % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 7d)
            .field(Table.DIM_TAG_OFFSET, "tag_a")
            .cell("d1", (pointTime + 1) % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 5d)
            .field(Table.DIM_TAG_OFFSET, "tag_a")
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
            equ(dimension(TestDims.DIM_A), const("tag_a"))
          )
        ),
        null,
        new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable)),
        NoMetricCollector
      )
      .toList

    val batch = res.head
    batch.size shouldEqual 2
    batch.get[Time](0, 0) shouldEqual Time(pointTime)
    batch.get[String](0, 1) shouldEqual "tag_a"
    batch.get[Double](0, 2) shouldEqual 7d
  }

  it should "support EQ filter for tuples" in withMock { (dao, _, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression[_]](time, dimension(TestDims.DIM_A), metric(TestTableFields.TEST_FIELD))

    val pointTime1 = 2000
    val pointTime2 = 2500

    queryRunner
      .expects(
        scan(testTable, from, to, Seq(dimAHash("test42")))
      )
      .returning(
        Iterator(
          HBaseTestUtils
            .row(pointTime1 - (pointTime1 % testTable.rowTimeSpan), dimAHash("test42"), 5.toShort)
            .cell("d1", pointTime1 % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 7d)
            .field(Table.DIM_TAG_OFFSET, "test42")
            .cell("d1", pointTime2 % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 5d)
            .field(Table.DIM_TAG_OFFSET, "test42")
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
            equ(tuple(time, dimension(TestDims.DIM_A)), const((Time(pointTime2), "test42")))
          )
        ),
        null,
        new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable)),
        NoMetricCollector
      )
      .toList

    val batch = res.head
    batch.size shouldEqual 2
    batch.isDeleted(0) shouldEqual true
    batch.get[Time](1, 0) shouldEqual Time(pointTime2)
    batch.get[String](1, 1) shouldEqual "test42"
    batch.get[Double](1, 2) shouldEqual 5d

  }

  it should "perform pre-filtering by IN for tuples" in withMock { (dao, _, queryRunner) =>
    val from = 1000
    val to = 5000
    val exprs = Seq[Expression[_]](time, dimension(TestDims.DIM_A), metric(TestTableFields.TEST_FIELD))

    val pointTime1 = 1010
    val pointTime2 = 1020
    val pointTime3 = 1030

    queryRunner
      .expects(
        scanMultiRanges(
          testTable,
          from,
          to,
          Set(
            Seq(dimAHash("test42")),
            Seq(dimAHash("test51"))
          )
        )
      )
      .returning(
        Iterator(
          HBaseTestUtils
            .row(pointTime1 - (pointTime1 % testTable.rowTimeSpan), dimAHash("test42"), 5.toShort)
            .cell("d1", pointTime1 % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 7d)
            .field(Table.DIM_TAG_OFFSET, "test42")
            .cell("d1", pointTime2 % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 5d)
            .field(Table.DIM_TAG_OFFSET, "test42")
            .cell("d1", pointTime3 % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 8d)
            .field(Table.DIM_TAG_OFFSET, "test42")
            .hbaseRow,
          HBaseTestUtils
            .row(pointTime1 - (pointTime1 % testTable.rowTimeSpan), dimAHash("test51"), 6.toShort)
            .cell("d1", pointTime1 % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 15d)
            .field(Table.DIM_TAG_OFFSET, "test51")
            .cell("d1", pointTime2 % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 33d)
            .field(Table.DIM_TAG_OFFSET, "test51")
            .cell("d1", pointTime3 % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 43d)
            .field(Table.DIM_TAG_OFFSET, "test51")
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
            in(tuple(time, dimension(TestDims.DIM_A)), Set((Time(pointTime2), "test42"), (Time(pointTime1), "test51")))
          )
        ),
        null,
        new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable)),
        NoMetricCollector
      )
      .toList

    val batch = res.head
    batch.size shouldEqual 6
    batch.isDeleted(0) shouldBe false
    batch.get[Time](0, 0) shouldEqual Time(pointTime1)
    batch.get[String](0, 1) shouldEqual "test42"
    batch.get[Double](0, 2) shouldEqual 7d

    batch.isDeleted(1) shouldBe false
    batch.get[Time](1, 0) shouldEqual Time(pointTime2)
    batch.get[String](1, 1) shouldEqual "test42"
    batch.get[Double](1, 2) shouldEqual 5d

    batch.isDeleted(2) shouldBe true

    batch.isDeleted(3) shouldBe false
    batch.get[Time](3, 0) shouldEqual Time(pointTime1)
    batch.get[String](3, 1) shouldEqual "test51"
    batch.get[Double](3, 2) shouldEqual 15d

    batch.isDeleted(4) shouldBe false

    batch.isDeleted(4) shouldBe false
    batch.get[Time](4, 0) shouldEqual Time(pointTime2)
    batch.get[String](4, 1) shouldEqual "test51"
    batch.get[Double](4, 2) shouldEqual 33d

    batch.isDeleted(5) shouldBe true
  }

  it should "union same dimensions in OR conditions" in withMock { (dao, _, queryRunner) =>

    val from = 100000L
    val to = 200000L
    val exprs = Seq[Expression[_]](time, dimension(TestDims.DIM_A), metric(TestTableFields.TEST_FIELD))

    val pointTime1 = 100500L

    queryRunner
      .expects(
        scanMultiRanges(testTable, from, to, Set(Seq(dimAHash("foo")), Seq(dimAHash("bar")), Seq(dimAHash("baz"))))
      )
      .returning(
        Iterator(
          HBaseTestUtils
            .row(pointTime1 - (pointTime1 % testTable.rowTimeSpan), dimAHash("foo"), 5.toShort)
            .cell("d1", pointTime1 % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 3d)
            .hbaseRow
        )
      )

    val res = dao.query(
      InternalQuery(
        testTable,
        exprs.toSet,
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          or(
            equ(dimension(TestDims.DIM_A), const("foo")),
            in(dimension(TestDims.DIM_A), Set("bar", "baz"))
          )
        )
      ),
      null,
      new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable)),
      NoMetricCollector
    )

    res.toList.head should have size 1
  }

  it should "support negative conditions in OR" in withMock { (dao, _, queryRunner) =>

    val from = 100000L
    val to = 200000L
    val exprs = Seq[Expression[_]](time, dimension(TestDims.DIM_A), metric(TestTableFields.TEST_FIELD))

    val pointTime1 = 100500L

    queryRunner
      .expects(scan(from, to))
      .returning(
        Iterator(
          HBaseTestUtils
            .row(pointTime1 - (pointTime1 % testTable.rowTimeSpan), dimAHash("foo"), 5.toShort)
            .cell("d1", pointTime1 % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 3d)
            .hbaseRow,
          HBaseTestUtils
            .row(pointTime1 - (pointTime1 % testTable.rowTimeSpan), dimAHash("bar"), 2.toShort)
            .cell("d1", pointTime1 % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 33d)
            .hbaseRow
        )
      )

    val res = dao.query(
      InternalQuery(
        testTable,
        exprs.toSet,
        and(
          ge(time, const(Time(from))),
          lt(time, const(Time(to))),
          or(
            neq(dimension(TestDims.DIM_A), const("foo")),
            notIn(dimension(TestDims.DIM_A), Set("foo", "bar"))
          )
        )
      ),
      null,
      new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable)),
      NoMetricCollector
    )

    res should have size 1
  }

  it should "handle multiple time bounds" in withMock { (dao, _, queryRunner) =>
    val from1 = 100000L
    val to1 = 200000L

    val from2 = 300000L
    val to2 = 400000L

    val exprs = Seq[Expression[_]](time, metric(TestTableFields.TEST_FIELD))

    val pointTime1 = 123500L
    val pointTime2 = 345678L

    queryRunner
      .expects(
        scanMultiRanges(testTable, from1, to2, Set(Seq(dimAHash("foo"))))
      )
      .returning(
        Iterator(
          HBaseTestUtils
            .row(pointTime1 - (pointTime1 % testTable.rowTimeSpan), dimAHash("foo"), 5.toShort)
            .cell("d1", pointTime1 % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 3d)
            .hbaseRow,
          HBaseTestUtils
            .row(pointTime2 - (pointTime2 % testTable.rowTimeSpan), dimAHash("foo"), 5.toShort)
            .cell("d1", pointTime2 % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 3d)
            .hbaseRow
        )
      )

    val res = dao.query(
      InternalQuery(
        testTable,
        exprs.toSet,
        and(
          equ(dimension(TestDims.DIM_A), const("foo")),
          or(
            and(ge(time, const(Time(from1))), lt(time, const(Time(to1)))),
            and(ge(time, const(Time(from2))), lt(time, const(Time(to2))))
          )
        )
      ),
      null,
      new DatasetSchema(valExprIndex(exprs), refExprIndex(exprs), Map.empty, Some(testTable)),
      NoMetricCollector
    )

    res.toList.head.size shouldEqual 2
  }

  class TestDao(override val dictionaryProvider: DictionaryProvider, queryRunner: QueryRunner)
      extends TSDaoHBaseBase[Iterator] {
    override def mapReduceEngine(metricQueryCollector: MetricQueryCollector): MapReducible[Iterator] =
      IteratorMapReducible.iteratorMR

    override def executeScans(
        queryContext: InternalQueryContext,
        intervals: Seq[(Long, Long)],
        rangeScanDims: Iterator[Map[Dimension, Seq[_]]]
    ): Iterator[HResult] = {

      val totalFrom = intervals.map(_._1).min
      val totalTo = intervals.map(_._2).max

      if (rangeScanDims.nonEmpty) {
        rangeScanDims.flatMap { dimIds =>
          val filter = HBaseUtils.multiRowRangeFilter(queryContext.table, intervals, dimIds)
          HBaseUtils.createScan(queryContext, filter, Seq.empty, totalFrom, totalTo) match {
            case Some(scan) => queryRunner(Seq(scan))
            case None       => Iterator.empty
          }
        }
      } else {
        Iterator.empty
      }

    }

    override val schema: Schema = TestSchema.schema

    override def put(
        mr: MapReducible[Iterator],
        dataPoints: Iterator[DataPoint],
        username: String
    ): Iterator[UpdateInterval] = ???

    override def putDataset(
        mr: MapReducible[Iterator],
        table: Table,
        dataset: Iterator[BatchDataset],
        username: String
    ): Iterator[UpdateInterval] = ???
  }

  def withMock(body: (TestDao, DictionaryDao, QueryRunner) => Unit): Unit = {
    val exec = mockFunction[Seq[Scan], Iterator[HResult]]
    val dictionaryDaoMock = mock[DictionaryDao]
    val dictionaryProvider = new DictionaryProviderImpl(dictionaryDaoMock)
    val dao = new TestDao(dictionaryProvider, exec)
    body(dao, dictionaryDaoMock, exec)
  }

  private def valExprIndex(exrs: Seq[Expression[_]]) = {
    exrs.zipWithIndex.filterNot(_._1.dataType.internalStorable.isRefType).toMap
  }

  private def refExprIndex(exrs: Seq[Expression[_]]) = {
    exrs.zipWithIndex.filter(_._1.dataType.internalStorable.isRefType).toMap
  }
}

package org.yupana.hbase

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Properties

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.util.Bytes
import org.joda.time.{ DateTime, DateTimeZone, LocalDateTime }
import org.scalamock.scalatest.MockFactory
import org.scalatest.{ BeforeAndAfterAll, OptionValues }
import org.yupana.api.query.DataPoint
import org.yupana.api.schema._
import org.yupana.core.cache.CacheFactory
import org.yupana.core.dao.{ DictionaryDao, DictionaryProviderImpl }

import scala.collection.JavaConverters._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HBaseUtilsTest extends AnyFlatSpec with Matchers with MockFactory with OptionValues with BeforeAndAfterAll {

  import HBaseUtilsTest._

  "HBaseUtils" should "serialize and parse row TsdRow keys" in {

    val dictionaryDaoMock = mock[DictionaryDao]
    val dictionaryProvider = new DictionaryProviderImpl(dictionaryDaoMock)

    val time = new DateTime(2020, 4, 22, 11, 21, 55, DateTimeZone.UTC).getMillis
    val dp = DataPoint(TestTable, time, Map(DIM_B -> "b value", DIM_A -> 4, DIM_C -> "c value"), Seq.empty)

    (dictionaryDaoMock.getIdByValue _).expects(DIM_B, "b value").returning(Some(1L))
    (dictionaryDaoMock.getIdByValue _).expects(DIM_C, "c value").returning(Some(31L))

    val bytes = HBaseUtils.rowKey(dp, TestTable, HBaseUtils.tableKeySize(TestTable), dictionaryProvider)
    val expectedRowKey = TSDRowKey(HBaseUtils.baseTime(time, TestTable), Array(Some(4), Some(1L), Some(31L)))
    val actualRowKey = HBaseUtils.parseRowKey(bytes, TestTable)

    actualRowKey should be(expectedRowKey)
  }

  it should "create TSDRows from datapoints" in {
    val time = new DateTime(2017, 10, 15, 12, 57, DateTimeZone.UTC).getMillis
    val dims: Map[Dimension, Any] = Map(DIM_A -> 1111, DIM_B -> "test2", DIM_C -> "test3")
    val dp1 = DataPoint(TestTable, time, dims, Seq(MetricValue(TEST_FIELD, 1.0)))
    val dp2 = DataPoint(TestTable2, time + 1, dims, Seq(MetricValue(TEST_FIELD, 2.0)))
    val dp3 = DataPoint(TestTable, time + 2, dims, Seq(MetricValue(TEST_FIELD, 3.0)))

    val dictionaryDaoMock = mock[DictionaryDao]
    val dictionaryProvider = new DictionaryProviderImpl(dictionaryDaoMock)

    (dictionaryDaoMock.getIdByValue _).expects(DIM_B, "test2").returning(Some(22))
    (dictionaryDaoMock.getIdByValue _).expects(DIM_C, "test3").returning(Some(33))

    val pbt = HBaseUtils.createPuts(Seq(dp1, dp2, dp3), dictionaryProvider)

    pbt should have size 2

    val puts = pbt.find(_._1 == TestTable).value._2
    puts should have size 2

    val put1 =
      puts.find(p => !p.get(HBaseUtils.family(1), Bytes.toBytes(HBaseUtils.restTime(time, TestTable))).isEmpty).get

    val cells1 = put1.get(HBaseUtils.family(1), Bytes.toBytes(HBaseUtils.restTime(time, TestTable))).asScala
    cells1 should have size 1

    val bb = ByteBuffer
      .allocate(256)
      .put(1.toByte)
      .putDouble(1.0)
      .put((Table.DIM_TAG_OFFSET + 1).toByte)
      .putInt("test2".length)
      .put("test2".getBytes(StandardCharsets.UTF_8))
      .put((Table.DIM_TAG_OFFSET + 2).toByte)
      .putInt("test3".length)
      .put("test3".getBytes(StandardCharsets.UTF_8))

    val expected1 = new Array[Byte](bb.position())
    bb.rewind()
    bb.get(expected1)

    CellUtil.cloneValue(cells1.head) shouldEqual expected1

    val put2 =
      puts.find(p => !p.get(HBaseUtils.family(1), Bytes.toBytes(HBaseUtils.restTime(time + 2, TestTable))).isEmpty).get

    val cells2 = put2.get(HBaseUtils.family(1), Bytes.toBytes(HBaseUtils.restTime(time + 2, TestTable))).asScala
    cells2 should have size 1

    bb.rewind()
    bb.put(1.toByte)
      .putDouble(3.0)
      .put((Table.DIM_TAG_OFFSET + 1).toByte)
      .putInt("test2".length)
      .put("test2".getBytes(StandardCharsets.UTF_8))
      .put((Table.DIM_TAG_OFFSET + 2).toByte)
      .putInt("test3".length)
      .put("test3".getBytes(StandardCharsets.UTF_8))

    val expected2 = new Array[Byte](bb.position())
    bb.rewind()
    bb.get(expected2)

    CellUtil.cloneValue(cells2.head) shouldEqual expected2

    val put3 = pbt
      .find(_._1 == TestTable2)
      .value
      ._2
      .find(p => !p.get(HBaseUtils.family(1), Bytes.toBytes(HBaseUtils.restTime(time + 1, TestTable2))).isEmpty)
      .get

    val cells3 = put3.get(HBaseUtils.family(1), Bytes.toBytes(HBaseUtils.restTime(time + 1, TestTable2))).asScala
    cells3 should have size 1

    bb.rewind()
    bb.put(1.toByte)
      .putDouble(2.0)
      .put(Table.DIM_TAG_OFFSET.toByte)
      .putInt("test2".length)
      .put("test2".getBytes(StandardCharsets.UTF_8))
      .put((Table.DIM_TAG_OFFSET + 2).toByte)
      .putInt("test3".length)
      .put("test3".getBytes(StandardCharsets.UTF_8))

    val expected3 = new Array[Byte](bb.position())
    bb.rewind()
    bb.get(expected3)

    CellUtil.cloneValue(cells3.head) shouldEqual expected3

    val expectedRow = new Array[Byte](HBaseUtils.tableKeySize(TestTable))

    bb.rewind()
    bb.putLong(HBaseUtils.baseTime(time, TestTable))
      .putInt(1111)
      .putLong(22)
      .putLong(33)
      .rewind()

    bb.get(expectedRow)

    put1.getRow shouldEqual expectedRow

    CacheFactory.flushCaches()
  }

  it should "test" in {
    val i1 = -1
    val i2 = 1

    Bytes.compareTo(Bytes.toBytes(i1), Bytes.toBytes(i2)) > 0 shouldEqual java.lang.Long.compareUnsigned(i1, i2) > 0
  }

  override protected def beforeAll(): Unit = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("app.properties"))
    CacheFactory.init(properties, "test")
    super.beforeAll()
  }
}

object HBaseUtilsTest {
  val DIM_A = RawDimension[Int]("A")
  val DIM_B = DictionaryDimension("B")
  val DIM_C = DictionaryDimension("C")
  val TEST_FIELD = Metric[Double]("testField", 1)

  val TestTable = new Table(
    name = "test_table",
    rowTimeSpan = 24 * 60 * 60 * 1000,
    dimensionSeq = Seq(DIM_A, DIM_B, DIM_C),
    metrics = Seq(TEST_FIELD),
    externalLinks = Seq.empty,
    new LocalDateTime(2016, 1, 1, 0, 0).toDateTime(DateTimeZone.UTC).getMillis
  )

  val TestTable2 = new Table(
    name = "test_table_2",
    rowTimeSpan = 24 * 60 * 60 * 1000,
    dimensionSeq = Seq(DIM_B, DIM_A, DIM_C),
    metrics = Seq(TEST_FIELD),
    externalLinks = Seq.empty,
    new LocalDateTime(2016, 1, 1, 0, 0).toDateTime(DateTimeZone.UTC).getMillis
  )
}

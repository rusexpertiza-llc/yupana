package org.yupana.hbase

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Properties

import org.apache.hadoop.hbase.util.Bytes
import org.joda.time.{ DateTime, DateTimeZone, LocalDateTime }
import org.scalamock.scalatest.MockFactory
import org.scalatest.{ BeforeAndAfterAll, FlatSpec, Matchers, OptionValues }
import org.yupana.api.query.DataPoint
import org.yupana.api.schema.{ DictionaryDimension, Dimension, Metric, MetricValue, RawDimension, Table }
import org.yupana.core.cache.CacheFactory
import org.yupana.core.dao.{ DictionaryDao, DictionaryProviderImpl }

class HBaseUtilsTest extends FlatSpec with Matchers with MockFactory with OptionValues with BeforeAndAfterAll {

  import HBaseUtilsTest._

  "HBaseUtils" should "serialize and parse row TsdRow keys" in {

    val dictionaryDaoMock = mock[DictionaryDao]
    val dictionaryProvider = new DictionaryProviderImpl(dictionaryDaoMock)

    val time = new DateTime(2020, 4, 22, 11, 21, 55, DateTimeZone.UTC).getMillis
    val dp = DataPoint(TestTable, time, Map(DIM_B -> "b value"), Seq.empty)

    (dictionaryDaoMock.getIdByValue _).expects(DIM_B, "b value").returning(Some(1L))

    val bytes = HBaseUtils.rowKey(dp, TestTable, HBaseUtils.tableKeySize(TestTable), dictionaryProvider)
    val expectedRowKey = TSDRowKey(HBaseUtils.baseTime(time, TestTable), Array(None, Some(1L), None))
    val actualRowKey = HBaseUtils.parseRowKey(bytes, TestTable)

    actualRowKey should be(expectedRowKey)
  }

  it should "create TSDRows from datapoints" in {
    val time = new DateTime(2017, 10, 15, 12, 57, DateTimeZone.UTC).getMillis
    val dims: Map[Dimension, Any] = Map(DIM_A -> "test1", DIM_B -> "test2")
    val dp1 = DataPoint(TestTable, time, dims, Seq(MetricValue(TEST_FIELD, 1.0)))
    val dp2 = DataPoint(TestTable2, time + 1, dims, Seq(MetricValue(TEST_FIELD, 2.0)))
    val dp3 = DataPoint(TestTable, time + 2, dims, Seq(MetricValue(TEST_FIELD, 3.0)))

    val dictionaryDaoMock = mock[DictionaryDao]
    val dictionaryProvider = new DictionaryProviderImpl(dictionaryDaoMock)

    (dictionaryDaoMock.getIdByValue _).expects(DIM_A, "test1").returning(Some(1))
    (dictionaryDaoMock.getIdByValue _).expects(DIM_B, "test2").returning(Some(2))

    val rbt = HBaseUtils.createPuts(Seq(dp1, dp2, dp3), dictionaryProvider)

    rbt should have size 2

    val rows = rbt.find(_._1 == TestTable).value._2
    rows should have size 1

//    val (time1, value1) = rows.head.values.valuesByGroup(1)(0)
//    val (time2, value2) = rows.head.values.valuesByGroup(1)(1)
//    time1 shouldEqual 46620000
//    value1.toSeq should (
//      equal(
//        ByteBuffer
//          .allocate(29)
//          .put(1.toByte)
//          .putDouble(1.0)
//          .put(Table.DIM_TAG_OFFSET.toByte)
//          .putInt("test1".length)
//          .put("test1".getBytes(StandardCharsets.UTF_8))
//          .put((Table.DIM_TAG_OFFSET + 1).toByte)
//          .putInt("test2".length)
//          .put("test2".getBytes(StandardCharsets.UTF_8))
//          .array()
//          .toSeq
//      )
//        or equal(
//          ByteBuffer
//            .allocate(29)
//            .put(1.toByte)
//            .putDouble(1.0)
//            .put((Table.DIM_TAG_OFFSET + 1).toByte)
//            .putInt("test2".length)
//            .put("test2".getBytes(StandardCharsets.UTF_8))
//            .put(Table.DIM_TAG_OFFSET.toByte)
//            .putInt("test1".length)
//            .put("test1".getBytes(StandardCharsets.UTF_8))
//            .array()
//            .toSeq
//        )
//    )
//
//    time2 shouldEqual 46620002
//    value2.toSeq should (
//      equal(
//        ByteBuffer
//          .allocate(29)
//          .put(1.toByte)
//          .putDouble(3.0)
//          .put(Table.DIM_TAG_OFFSET.toByte)
//          .putInt("test1".length)
//          .put("test1".getBytes(StandardCharsets.UTF_8))
//          .put((Table.DIM_TAG_OFFSET + 1).toByte)
//          .putInt("test2".length)
//          .put("test2".getBytes(StandardCharsets.UTF_8))
//          .array()
//          .toSeq
//      )
//        or equal(
//          ByteBuffer
//            .allocate(29)
//            .put(1.toByte)
//            .putDouble(3.0)
//            .put((Table.DIM_TAG_OFFSET + 1).toByte)
//            .putInt("test2".length)
//            .put("test2".getBytes(StandardCharsets.UTF_8))
//            .put(Table.DIM_TAG_OFFSET.toByte)
//            .putInt("test1".length)
//            .put("test1".getBytes(StandardCharsets.UTF_8))
//            .array()
//            .toSeq
//        )
//    )
//    rows.head.key shouldEqual TSDRowKey(1508025600000L, Array(Some(1), Some(2), None))
//
//    val rows2 = rbt.find(_._1 == TestTable2).value._2
//    rows2 should have size 1
//
//    val (time3, value3) = rows2.head.values.valuesByGroup(1)(0)
//
//    time3 shouldEqual 46620001
//    value3.toSeq should (
//      equal(
//        ByteBuffer
//          .allocate(29)
//          .put(1.toByte)
//          .putDouble(2.0)
//          .put((Table.DIM_TAG_OFFSET + 1).toByte)
//          .putInt("test1".length)
//          .put("test1".getBytes(StandardCharsets.UTF_8))
//          .put(Table.DIM_TAG_OFFSET.toByte)
//          .putInt("test2".length)
//          .put("test2".getBytes(StandardCharsets.UTF_8))
//          .array()
//          .toSeq
//      )
//        or equal(
//          ByteBuffer
//            .allocate(29)
//            .put(1.toByte)
//            .putDouble(2.0)
//            .put(Table.DIM_TAG_OFFSET.toByte)
//            .putInt("test2".length)
//            .put("test2".getBytes(StandardCharsets.UTF_8))
//            .put((Table.DIM_TAG_OFFSET + 1).toByte)
//            .putInt("test1".length)
//            .put("test1".getBytes(StandardCharsets.UTF_8))
//            .array()
//            .toSeq
//        )
//    )
//    rows.head.key shouldEqual TSDRowKey(1508025600000L, Array(Some(1), Some(2), None))

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
    externalLinks = Seq.empty
  )

  val TestTable2 = new Table(
    name = "test_table_2",
    rowTimeSpan = 24 * 60 * 60 * 1000,
    dimensionSeq = Seq(DIM_B, DIM_A, DIM_C),
    metrics = Seq(TEST_FIELD),
    externalLinks = Seq.empty
  )
}

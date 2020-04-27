package org.yupana.hbase

import java.nio.ByteBuffer
import java.util.Properties

import org.apache.hadoop.hbase.util.Bytes
import org.joda.time.{ DateTimeZone, LocalDateTime }
import org.scalamock.scalatest.MockFactory
import org.scalatest.{ FlatSpec, Matchers, OptionValues }
import org.yupana.api.query.DataPoint
import org.yupana.api.schema.{ Dimension, Metric, MetricValue, Table }
import org.yupana.core.cache.CacheFactory
import org.yupana.core.dao.{ DictionaryDao, DictionaryProviderImpl }

import scala.io.Source

class HBaseUtilsTest extends FlatSpec with Matchers with MockFactory with OptionValues {

  "HBaseUtils" should "serialize and parse row TSROW keys" in {

    val expectedRowKey = TSDRowKey[Long](123, Array(None, Some(1L), None))
    val bytes = HBaseUtils.rowKeyToBytes(expectedRowKey)
    val actualRowKey = HBaseUtils.parseRowKey(bytes, TestTable)

    actualRowKey should be(expectedRowKey)
  }

  it should "create fuzzy filter" in {

    val filter = HBaseUtils.createFuzzyFilter(Some(123), Array(None, Some(1)))
    filter.toString should be(
      """FuzzyRowFilter{fuzzyKeysData={\x00\x00\x00\x00\x00\x00\x00{\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01:\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x02\x02\x02\x02\x02\x02\x02\x02\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF}}, """
    )
  }

  it should "create TSDRows from datapoints" in {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("app.properties"))
    CacheFactory.init(properties, "test")

    val time = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC).getMillis
    val tags = Map(TAG_A -> "test1", TAG_B -> "test2")
    val dp1 = DataPoint(TestTable, time, tags, Seq(MetricValue(TEST_FIELD, 1.0)))
    val dp2 = DataPoint(TestTable2, time + 1, tags, Seq(MetricValue(TEST_FIELD, 2.0)))
    val dp3 = DataPoint(TestTable, time + 2, tags, Seq(MetricValue(TEST_FIELD, 3.0)))

    val dictionaryDaoMock = mock[DictionaryDao]
    val dictionaryProvider = new DictionaryProviderImpl(dictionaryDaoMock)

    (dictionaryDaoMock.getIdByValue _).expects(TAG_A, "test1").returning(Some(1))
    (dictionaryDaoMock.getIdByValue _).expects(TAG_B, "test2").returning(Some(2))

    val rbt = HBaseUtils.createTsdRows(Seq(dp1, dp2, dp3), dictionaryProvider)

    rbt should have size 2

    val rows = rbt.find(_._1 == TestTable).value._2
    rows should have size 1

    val (time1, value1) = rows.head.values.valuesByGroup(1)(0)
    val (time2, value2) = rows.head.values.valuesByGroup(1)(1)
    time1 shouldEqual 46620000
    value1.toSeq shouldEqual ByteBuffer.allocate(9).put(1.toByte).putDouble(1.0).array().toSeq

    time2 shouldEqual 46620002
    value2.toSeq shouldEqual ByteBuffer.allocate(9).put(1.toByte).putDouble(3.0).array().toSeq
    rows.head.key shouldEqual TSDRowKey[Int](1508025600000L, Array(Some(1), Some(2), None))

    val rows2 = rbt.find(_._1 == TestTable2).value._2
    rows2 should have size 1

    val (time3, value3) = rows2.head.values.valuesByGroup(1)(0)

    time3 shouldEqual 46620001
    value3.toSeq shouldEqual ByteBuffer.allocate(9).put(1.toByte).putDouble(2.0).array().toSeq
    rows.head.key shouldEqual TSDRowKey[Int](1508025600000L, Array(Some(1), Some(2), None))

    CacheFactory.flushCaches()
  }

  it should "test" in {
    val i1 = -1
    val i2 = 1

    Bytes.compareTo(Bytes.toBytes(i1), Bytes.toBytes(i2)) shouldEqual java.lang.Long.compareUnsigned(i1, i2)
  }

  val TAG_A = Dimension("TAG_A")
  val TAG_B = Dimension("TAG_B")
  val TAG_C = Dimension("TAG_C")
  val TEST_FIELD = Metric[Double]("testField", 1)

  val TestTable = new Table(
    name = "test_table",
    rowTimeSpan = 24 * 60 * 60 * 1000,
    dimensionSeq = Seq(TAG_A, TAG_B, TAG_C),
    metrics = Seq(TEST_FIELD),
    externalLinks = Seq.empty
  )

  val TestTable2 = new Table(
    name = "test_table_2",
    rowTimeSpan = 24 * 60 * 60 * 1000,
    dimensionSeq = Seq(TAG_B, TAG_A, TAG_C),
    metrics = Seq(TEST_FIELD),
    externalLinks = Seq.empty
  )

  it should "check intersections with row ranges" in {
    val ranges = List(
      (asBytes(Array(10, 11, 12)), asBytes(Array(20, 21, 22))),
      (asBytes(Array(50, 51, 52)), asBytes(Array(60, 61, 62)))
    )
    HBaseUtils.intersectWithRowRanges(asBytes(Array(10, 11, 12)), asBytes(Array(20, 21, 22)), ranges) shouldBe true
    HBaseUtils.intersectWithRowRanges(asBytes(Array(1, 2, 3)), asBytes(Array(10, 11, 12)), ranges) shouldBe true
    HBaseUtils.intersectWithRowRanges(asBytes(Array(1, 2, 3)), asBytes(Array(15, 16, 17)), ranges) shouldBe true
    HBaseUtils.intersectWithRowRanges(asBytes(Array(1, 2, 3)), asBytes(Array(25, 26, 27)), ranges) shouldBe true
    HBaseUtils.intersectWithRowRanges(asBytes(Array(10, 11, 12)), asBytes(Array(15, 16, 17)), ranges) shouldBe true
    HBaseUtils.intersectWithRowRanges(asBytes(Array(15, 16, 17)), asBytes(Array(18, 19, 20)), ranges) shouldBe true
    HBaseUtils.intersectWithRowRanges(asBytes(Array(15, 16, 17)), asBytes(Array(38, 39, 40)), ranges) shouldBe true
    HBaseUtils.intersectWithRowRanges(asBytes(Array(35, 36, 37)), asBytes(Array(58, 59, 60)), ranges) shouldBe true

    HBaseUtils.intersectWithRowRanges(asBytes(Array(1, 2, 3)), asBytes(Array(6, 7, 8)), ranges) shouldBe false
    HBaseUtils.intersectWithRowRanges(asBytes(Array(71, 72, 73)), asBytes(Array(76, 77, 78)), ranges) shouldBe false
    HBaseUtils.intersectWithRowRanges(asBytes(Array(9)), asBytes(Array(9)), ranges) shouldBe false
  }

  /*it should "check" in {
    val regions = getRegions

    val keys = getResultKeys

    keys.foreach { resultKey =>
      val r = regions.exists {
        case (regionStart, regionEnd) =>
          val found = isInPeriod(resultKey, regionStart, regionEnd)
          if (found) {
            println(s"resultKey: ${resultKey.mkString("[", ",", "]")} found in region ${regionStart
              .mkString("[", ",", "]")}  -  ${regionEnd.mkString("[", ",", "]")}")
          }
          found
      }
      if (!r) {
        println(resultKey.mkString("[", ",", "]"))
      }
    }
  }*/

  def asBytes(a: Array[Int]): Array[Byte] = a.map(_.toByte)

  def getRegions: List[(Array[Byte], Array[Byte])] = {
    Source
      .fromFile("/home/dloshkarev/Downloads/prod/regions_filtered.txt")
      .getLines()
      .map { s =>
        s.split("    -     ") match {
          case Array(start, end) =>
            (asArray(start), asArray(end))
          case Array(start) =>
            (asArray(start), Array.empty[Byte])
        }

      }
      .toList
  }

  def getResultKeys: List[Array[Byte]] = {
    Source.fromFile("/home/dloshkarev/Downloads/prod/result_keys.txt").getLines().map(asArray).toList
  }

  def isInPeriod(key: Array[Byte], start: Array[Byte], end: Array[Byte]): Boolean = {
    Bytes.compareTo(key, start) >= 0 && Bytes.compareTo(key, end) <= 0
  }

  def asArray(s: String): Array[Byte] = if (s.nonEmpty) s.split(",").map(_.toByte) else Array.empty[Byte]

}

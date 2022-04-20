package org.yupana.hbase

import org.apache.hadoop.hbase.client.{ ConnectionFactory, HBaseAdmin, Scan, Result => HResult }
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ HBaseConfiguration, TableName }
import org.scalatest.tagobjects.Slow
import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.api.query.syntax.All._
import org.yupana.api.schema.{ Dimension, Schema, Table }
import org.yupana.core.TestSchema.testTable
import org.yupana.core._
import org.yupana.core.cache.CacheFactory
import org.yupana.core.dao._
import org.yupana.core.utils.metric.{ ConsoleMetricReporter, MetricQueryCollector, StandaloneMetricCollector }

import java.util.Properties
import scala.util.Random
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.core.model.UpdateInterval

import java.time.{ LocalDateTime, ZoneOffset }

class TsdbBenchmark extends AnyFlatSpec with Matchers {

  "HBase" should "be fast" taggedAs Slow in {
    val hbaseConfiguration = HBaseConfiguration.create()
    hbaseConfiguration.set("hbase.zookeeper.quorum", "localhost:2181")
    hbaseConfiguration.set("zookeeper.session.timeout", "9000000")
    hbaseConfiguration.set("hbase.client.scanner.timeout.period", "9000000")
//    hbaseConfiguration.set("hbase.client.scanner.max.result.size", "50000000")
//    HdfsFileUtils.addHdfsPathToConfiguration(hbaseConfiguration, props)

    HBaseAdmin.available(hbaseConfiguration)
    val nameSpace = "schema43"

    val connection = ConnectionFactory.createConnection(hbaseConfiguration)
    val tableName = TableName.valueOf(nameSpace, "ts_kkm_items")
    val table = connection.getTable(tableName)
    val scan = new Scan()

    scan.addFamily(Bytes.toBytes("d1"))
    scan.setCaching(100000)
//    scan.setBatch(10000)
    scan.setMaxResultSize(100000000)
//    scan.setCacheBlocks(false)
//    scan.setCacheBlocks(false)
    scan.setScanMetricsEnabled(true)

    import scala.jdk.CollectionConverters._

    val start = System.currentTimeMillis()
    val scanner = table.getScanner(scan)
    val dps = scanner.iterator().asScala.foldLeft(0L) { (c, r) =>
      c + r.rawCells().length
    }

    println(dps)
    println(scanner.getScanMetrics.getMetricsMap.asScala.mkString("\r\n"))
    println("TIME: " + (System.currentTimeMillis() - start))
  }

  "CPU" should "be fast" taggedAs Slow in {
    val n = 50000000
    val arr = Array.fill[Long](n)(Random.nextLong())

    (1 until 30).foreach { _ =>
      val start = System.currentTimeMillis()
      val r = arr.iterator.foldLeft(Random.nextLong()) { _ + _ }
//      var r = 0L
//      var i = 0
//      while (i < n) {
//        r += arr(i % k)
//        i += 1
//      }

      println(s"r$r time: {${System.currentTimeMillis() - start}}")
    }

  }

  "TSDB" should "be fast" taggedAs Slow in {
    val qtime = LocalDateTime.of(2017, 10, 15, 12, 57).atOffset(ZoneOffset.UTC)

    val N = 1000000
//    val in = (1 to N).toArray

    val dictDao = new DictionaryDao {
      override def createSeqId(dimension: Dimension): Int = ???

      val vals = (0 until N / 10000).map { b =>
        (0 until 10000).map { i =>
          val id = b * 10000 + i
          id.toLong -> "Test"
        }.toMap
      }.toArray

      override def getIdByValue(dimension: Dimension, value: String): Option[Long] = ???

      override def getIdsByValues(dimension: Dimension, value: Set[String]): Map[String, Long] = ???

      override def checkAndPut(dimension: Dimension, id: Long, value: String): Boolean = ???
    }

    val dictProvider = new DictionaryProviderImpl(dictDao)

    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("app.properties"))
    CacheFactory.init(properties)

    val dao = new TSDaoHBaseBase[Iterator] with TSDao[Iterator, Long] {

      override def mapReduceEngine(metricQueryCollector: MetricQueryCollector): MapReducible[Iterator] = {
        IteratorMapReducible.iteratorMR
      }

      override def dictionaryProvider: DictionaryProvider = dictProvider

      val rows = {
        val time = qtime.toInstant.toEpochMilli + 24L * 60 * 60 * 1000
        (1 to N).map { i =>
          val dimId = i
          HBaseTestUtils
            .row(time - (time % testTable.rowTimeSpan), HBaseTestUtils.dimAHash(dimId.toString), dimId.toShort)
            .cell("d1", time % testTable.rowTimeSpan)
            .field(TestTableFields.TEST_FIELD.tag, 1d)
            .field(TestTableFields.TEST_BIGDECIMAL_FIELD.tag, BigDecimal(1d))
            .field(Table.DIM_TAG_OFFSET, "test1")
            .hbaseRow
        }
      }

      override def executeScans(
          queryContext: InternalQueryContext,
          from: Long,
          to: Long,
          rangeScanDims: Iterator[Map[Dimension, Seq[_]]]
      ): Iterator[HResult] = {
        rows.iterator
      }

//      var c = 0
//
//      override def executeScans(
//          queryContext: InternalQueryContext,
//          from: IdType,
//          to: IdType,
//          rangeScanDims: Iterator[Map[Dimension, Seq[IdType]]]
//      ): Iterator[TSDOutputRow[IdType]] = {
//        c += 1
//        val o = N * HitRate
//        val v = tagged(1, 1.toDouble)
//        val time = qtime.toDate.getTime + 24L * 60 * 60 * 1000
//        in.map { x =>
//          val dimId = if (x > o) c * N + x else x
//          TSDOutputRow[Long](
//            key = TSDRowKey(time - (time % testTable.rowTimeSpan), Array(Some(dimId), Some(dimId))),
//            values = Array((x % 1000000, v))
//          )
//        }.iterator
//      }

//      override def executeScans(
//          queryContext: InternalQueryContext,
//          from: IdType,
//          to: IdType,
//          rangeScanDims: Iterator[Map[Dimension, Seq[IdType]]]
//      ): Iterator[TSDOutputRow[IdType]] = {
//        val v = tagged(1, 1.toDouble)
//        val time = qtime.toDate.getTime + 24L * 60 * 60 * 1000
//        val row = TSDOutputRow[Long](
//          key = TSDRowKey(time - (time % testTable.rowTimeSpan), Array(Some(1), Some(1))),
//          values = Array((100, v))
//        )
//        in.map(_ => row).iterator
//      }

      override val schema: Schema = TestSchema.schema

      override def putBatch(username: String)(dataPointsBatch: Seq[DataPoint]): Seq[UpdateInterval] = ???
    }

    val changelogDao: ChangelogDao = new ChangelogDao {
      override def putUpdatesIntervals(intervals: Seq[UpdateInterval]): Unit = ???
      override def getUpdatesIntervals(
          tableName: Option[String],
          updatedAfter: Option[Long],
          updatedBefore: Option[Long],
          recalculatedAfter: Option[Long],
          recalculatedBefore: Option[Long],
          updatedBy: Option[String]
      ): Iterable[UpdateInterval] = ???
    }

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusYears(1))),
      Seq(
        truncDay(time) as "time",
        dimension(TestDims.DIM_A) as "tag_a",
        dimension(TestDims.DIM_B) as "tag_b",
        sum(metric(TestTableFields.TEST_FIELD)) as "sum_testField",
        sum(metric(TestTableFields.TEST_BIGDECIMAL_FIELD)) as "sum_testField"
      ),
      None,
      Seq(truncDay(time))
    )

    val mc = new StandaloneMetricCollector(query, "test", 10, new ConsoleMetricReporter)
//    val mc = NoMetricCollector
    class BenchTSDB
        extends TSDB(
          TestSchema.schema,
          dao,
          changelogDao,
          dictProvider,
          identity,
          SimpleTsdbConfig(putEnabled = true),
          { _ =>
            mc
          }
        ) {
      override def createMetricCollector(query: Query): MetricQueryCollector = {
        mc
      }
    }

    val tsdb = new BenchTSDB

    (1 to 1500) foreach { p =>
      val s = System.nanoTime()
      val result = tsdb.query(query)

      val r1 = result.next()
      r1.get[Double]("sum_testField") shouldBe N.toDouble
      r1.get[String]("tag_a") shouldBe "test1"

      println(s"$p. Time: " + (System.nanoTime() - s) / (1000 * 1000))

    }

    mc.finish()
  }

//  "InternalRowBuilder" should "fast" taggedAs (Slow) in {
//    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
//
//    val query = Query(
//      TestSchema.testTable,
//      const(Time(qtime)),
//      const(Time(qtime.plusYears(1))),
//      Seq(
//        function(UnaryOperation.truncDay, time) as "time",
//        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField"
//      ),
//      None,
//      Seq(function(UnaryOperation.truncDay, time), dimension(TestDims.TAG_B))
//    )
//
//    val queryContext = QueryContext(query, ConstantExpr(true))
//    val builder = new InternalRowBuilder(queryContext)
//
//    def t() = {
//      val pointTime = 1000000
//      (1 to 10000000).toIterator.map(
//        i =>
//          builder
//            .set(time, Some(Time(pointTime + i)))
//            //              .set(dimension(TestDims.TAG_A), Some("test1"))
//            //              .set(dimension(TestDims.TAG_B), Some("test2"))
//            .set(metric(TestTableFields.TEST_FIELD), Some(1d))
//            .buildAndReset()
//      )
//    }
//
//    (1 to 10) foreach { p =>
//      val s = System.nanoTime()
//      val result = t()
//
//      result.size shouldBe 10000000L
//
//      println("Time: " + (System.nanoTime() - s) / (1000 * 1000))
//
//    }
//  }

//  "Metric collector" should "fast" taggedAs (Slow) in {
//    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
//
//    val query = Query(
//      TestSchema.testTable,
//      const(Time(qtime)),
//      const(Time(qtime.plusYears(1))),
//      Seq(
//        function(UnaryOperation.truncDay, time) as "time",
//        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField"
//      ),
//      None,
//      Seq(function(UnaryOperation.truncDay, time), dimension(TestDims.TAG_B))
//    )
//
//    val mc = new ConsoleMetricQueryCollector(query, "test")
//
//    def d = {
//
//      (1 to 10000000).iterator.map { i =>
//        mc.reduceOperation.measure(1)(i)
//        mc.reduceOperation.measure(1)(i)
//        mc.reduceOperation.measure(1)(i)
//        mc.extractDataComputation.measure(1)(i)
//        mc.postFilter.measure(1)(i)
//        mc.collectResultRows.measure(1)(i)
//      }
//    }
//
//    (1 to 10) foreach { p =>
//      val s = System.nanoTime()
//
//      d.size shouldBe 10000000L
//
//      println("Time: " + (System.nanoTime() - s) / (1000 * 1000))
//
//    }
//
//  }

//  "ReduceByKey" should "be fast" taggedAs (Slow) in {
//    val in = (1 to 10000000).map(i => 1 -> i).toArray
//
//    def d = {
//      val kvs = in.toIterator
//      CollectionUtils.reduceByKey(kvs)(_ + _)
//    }
//
//    (1 to 100) foreach { p =>
//      val s = System.nanoTime()
//
//      d.next._2 shouldBe -2004260032
////      d.count(_ => true) shouldBe 10000000 //-2004260032
//
//      println("Time: " + (System.nanoTime() - s) / (1000 * 1000))
//
//    }
//  }

}

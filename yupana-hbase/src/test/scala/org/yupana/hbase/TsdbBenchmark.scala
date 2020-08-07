package org.yupana.hbase

import java.util.Properties

import org.apache.hadoop.hbase.client.{ ConnectionFactory, Scan, Result => HResult }
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ HBaseConfiguration, TableName }
import org.joda.time.{ DateTimeZone, LocalDateTime }
import org.scalatest.tagobjects.Slow
import org.scalatest.{ FlatSpec, Matchers }
import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.api.query.syntax.All._
import org.yupana.api.schema.{ Dimension, Table }
import org.yupana.api.types.{ Aggregation, UnaryOperation }
import org.yupana.core.TestSchema.testTable
import org.yupana.core._
import org.yupana.core.cache.CacheFactory
import org.yupana.core.dao._
import org.yupana.core.model._
import org.yupana.core.utils.metric.{ ConsoleMetricQueryCollector, MetricQueryCollector }

import scala.util.Random

class TsdbBenchmark extends FlatSpec with Matchers {

  "HBase" should "be fast" taggedAs Slow in {
    val hbaseConfiguration = HBaseConfiguration.create()
    hbaseConfiguration.set("hbase.zookeeper.quorum", "localhost:2181")
    hbaseConfiguration.set("zookeeper.session.timeout", "9000000")
    hbaseConfiguration.set("hbase.client.scanner.timeout.period", "9000000")
//    hbaseConfiguration.set("hbase.client.scanner.max.result.size", "50000000")
//    HdfsFileUtils.addHdfsPathToConfiguration(hbaseConfiguration, props)

//    HBaseAdmin.checkHBaseAvailable(hbaseConfiguration)

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

    import scala.collection.JavaConverters._

    val start = System.currentTimeMillis()
    val result = table.getScanner(scan)
    val dps = result.iterator().asScala.foldLeft(0L) { (c, r) =>
      c + r.rawCells().length
    }

    println(dps)
    println(result.getScanMetrics.getMetricsMap.asScala.mkString("\r\n"))
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
    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)

    val N = 1000000
//    val in = (1 to N).toArray

    val metricDao = new TsdbQueryMetricsDao {
      override def initializeQueryMetrics(query: Query, sparkQuery: Boolean): Unit = ???

      override def queriesByFilter(filter: Option[QueryMetricsFilter], limit: Option[Int]): Iterable[TsdbQueryMetrics] =
        ???

      override def updateQueryMetrics(
          rowKey: String,
          queryState: QueryStates.QueryState,
          totalDuration: Double,
          metricValues: Map[String, MetricData],
          sparkQuery: Boolean
      ): Unit = ???

      override def setRunningPartitions(queryRowKey: String, partitions: Int): Unit = ???

      override def decrementRunningPartitions(queryRowKey: String): Int = ???

      override def setQueryState(filter: QueryMetricsFilter, queryState: QueryStates.QueryState): Unit = ???

      override def deleteMetrics(filter: QueryMetricsFilter): Int = ???
    }

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
    CacheFactory.init(properties, "test")

    val dao = new TSDaoHBaseBase[Iterator] with TSDao[Iterator, Long] {

      override def mapReduceEngine(metricQueryCollector: MetricQueryCollector): MapReducible[Iterator] = {
        MapReducible.iteratorMR
      }

      override def dictionaryProvider: DictionaryProvider = dictProvider

      val rows = {
        val time = qtime.toDate.getTime + 24L * 60 * 60 * 1000
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

      override def put(dataPoints: Seq[DataPoint]): Unit = ???

      override def getRollupStatuses(fromTime: Long, toTime: Long, table: Table): Seq[(Long, String)] = ???

      override def putRollupStatuses(statuses: Seq[(Long, String)], table: Table): Unit = ???

      override def checkAndPutRollupStatus(
          time: Long,
          oldStatus: Option[String],
          newStatus: String,
          table: Table
      ): Boolean = ???

      override def getRollupSpecialField(fieldName: String, table: Table): Option[Long] = ???

      override def putRollupSpecialField(fieldName: String, value: Long, table: Table): Unit = ???

    }

    val query = Query(
      TestSchema.testTable,
      const(Time(qtime)),
      const(Time(qtime.plusYears(1))),
      Seq(
        function(UnaryOperation.truncDay, time) as "time",
        dimension(TestDims.DIM_A) as "tag_a",
        dimension(TestDims.DIM_B) as "tag_b",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField",
        aggregate(Aggregation.sum[BigDecimal], TestTableFields.TEST_BIGDECIMAL_FIELD) as "sum_testField"
      ),
      None,
      Seq(function(UnaryOperation.truncDay, time))
    )

    val mc = new ConsoleMetricQueryCollector(query, "test")
//    val mc = NoMetricCollector
    class BenchTSDB extends TSDB(dao, metricDao, dictProvider, identity, SimpleTsdbConfig(putEnabled = true)) {
      override def createMetricCollector(query: Query): MetricQueryCollector = {
        mc
      }
    }

    val tsdb = new BenchTSDB

    (1 to 1500) foreach { p =>
      val s = System.nanoTime()
      val result = tsdb.query(query).iterator

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

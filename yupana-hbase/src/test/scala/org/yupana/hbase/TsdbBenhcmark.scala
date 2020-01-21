package org.yupana.hbase

import java.util.Properties

import org.joda.time.{ DateTimeZone, LocalDateTime }
import org.scalatest.tagobjects.Slow
import org.scalatest.{ FlatSpec, Matchers }
import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.api.query.syntax.All._
import org.yupana.api.schema.{ Dimension, Table }
import org.yupana.api.types.{ Aggregation, UnaryOperation, Writable }
import org.yupana.core.TestSchema.testTable
import org.yupana.core.cache.CacheFactory
import org.yupana.core.dao._
import org.yupana.core.model._
import org.yupana.core.utils.metric.{ ConsoleMetricQueryCollector, MetricQueryCollector }
import org.yupana.core._

class TsdbBenhcmark extends FlatSpec with Matchers {

  "TSDB" should "be fast" taggedAs (Slow) in {
    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)

    val N = 500000
    val HitRate = 1d
    val in = (1 to N).toArray

    val metricDao = new TsdbQueryMetricsDao {
      override def initializeQueryMetrics(query: Query, sparkQuery: Boolean): Long = ???

      override def queriesByFilter(filter: Option[QueryMetricsFilter], limit: Option[Int]): Iterable[TsdbQueryMetrics] =
        ???

      override def updateQueryMetrics(
          rowKey: Long,
          queryState: QueryStates.QueryState,
          totalDuration: Double,
          metricValues: Map[String, MetricData],
          sparkQuery: Boolean
      ): Unit = ???

      override def setRunningPartitions(queryRowKey: Long, partitions: Int): Unit = ???

      override def decrementRunningPartitions(queryRowKey: Long): Int = ???

      override def setQueryState(filter: QueryMetricsFilter, queryState: QueryStates.QueryState): Unit = ???

      override def deleteMetrics(filter: QueryMetricsFilter): Int = ???
    }

    val dictDao = new DictionaryDao {
      override def createSeqId(dimension: Dimension): Int = ???

      override def getValueById(dimension: Dimension, id: Long): Option[String] = ???

      val vals = (0 until N / 10000).map { b =>
        (0 until 10000).map { i =>
          val id = b * 10000 + i
          id.toLong -> "Test"
        }.toMap
      }.toArray

      override def getValuesByIds(
          dimension: Dimension,
          ids: Set[Long],
          metricCollector: MetricQueryCollector
      ): Map[Long, String] = {
        if (ids.nonEmpty) {
          vals((ids.head / 10000).toInt)
        } else {
          Map.empty
        }
//        ids.map(i => i -> "Test").toMap
      }

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
        val v = tagged(1, 1.toDouble)

        in.map { x =>
          val dimId = x
          TSDOutputRow[Long](
            key = TSDRowKey(time - (time % testTable.rowTimeSpan), Array(Some(dimId), Some(dimId))),
            values = Array((x % 1000000, v))
          )
        }
      }

      override def executeScans(
          queryContext: InternalQueryContext,
          from: IdType,
          to: IdType,
          rangeScanDims: Iterator[Map[Dimension, Seq[IdType]]]
      ): Iterator[TSDOutputRow[IdType]] = {
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

      def tagged[T](tag: Byte, value: T)(implicit writable: Writable[T]): Array[Byte] = {
        tag +: writable.write(value)
      }

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
        dimension(TestDims.TAG_A) as "tag_a",
        dimension(TestDims.TAG_B) as "tag_b",
        aggregate(Aggregation.sum[Double], TestTableFields.TEST_FIELD) as "sum_testField"
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

    (1 to 150) foreach { p =>
      val s = System.nanoTime()
      val result = tsdb.query(query).iterator

      val r1 = result.next()
      r1.fieldValueByName[Double]("sum_testField").get shouldBe N.toDouble

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

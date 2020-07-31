package org.epicsquad.analytics.bench

import java.util.Properties

import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, Scan, Result => HResult, Table => HTable}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.epicsquad.analytics.bench.TestSchema.testTable
import org.joda.time.{DateTimeZone, LocalDateTime}
import org.openjdk.jmh.annotations.{Benchmark, Scope, State}
import org.yupana.api.Time
import org.yupana.api.query.syntax.All._
import org.yupana.api.query.{DataPoint, Query}
import org.yupana.api.schema.{Dimension, Table}
import org.yupana.api.types.{Aggregation, UnaryOperation}
import org.yupana.core.cache.CacheFactory
import org.yupana.core.dao._
import org.yupana.core.model.{MetricData, QueryStates, TsdbQueryMetrics}
import org.yupana.core.utils.metric.{ConsoleMetricQueryCollector, MetricQueryCollector}
import org.yupana.core.{MapReducible, SimpleTsdbConfig, TSDB}
import org.yupana.hbase.{InternalQueryContext, TSDaoHBaseBase}

class TsdbBenchmarks {

  @Benchmark
  def hbaseBenchmark1(state: TsdbBenchmarksState): Unit = {
    import scala.collection.JavaConverters._

    val table = state.hbaseTable
    val result = table.getScanner(state.hbaseScan)
    result.iterator().asScala.foldLeft(0L) { (c, r) =>
      c + r.rawCells().length
    }
  }

  @Benchmark
  def queryBenchmark(state: TsdbBenchmarksState): Unit = {
    val result = state.tsdb.query(state.query).iterator
    val r1 = result.next()
    println(r1.fieldValueByName[BigDecimal]("sum_testField").get.doubleValue() == state.N.toDouble)
    println(r1.fieldValueByName[String]("tag_a").get == "test1")
  }

}

@State(Scope.Benchmark)
class TsdbBenchmarksState {

  lazy val hbaseTable: HTable = {
    val hbaseConfiguration = HBaseConfiguration.create()
    hbaseConfiguration.set("hbase.zookeeper.quorum", "localhost:2181")
    hbaseConfiguration.set("zookeeper.session.timeout", "9000000")
    hbaseConfiguration.set("hbase.client.scanner.timeout.period", "9000000")
    //    hbaseConfiguration.set("hbase.client.scanner.max.result.size", "50000000")
    //    HdfsFileUtils.addHdfsPathToConfiguration(hbaseConfiguration, props)

    HBaseAdmin.checkHBaseAvailable(hbaseConfiguration)
    val nameSpace = "schema40"

    val connection = ConnectionFactory.createConnection(hbaseConfiguration)
    val tableName = TableName.valueOf(nameSpace, "ts_kkm_items")
    connection.getTable(tableName)
  }

  lazy val hbaseScan: Scan = {
    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("d1"))
    scan.setCaching(100000)
    //    scan.setBatch(10000)
    scan.setMaxResultSize(100000000)
    //    scan.setCacheBlocks(false)
    //    scan.setCacheBlocks(false)
    scan.setScanMetricsEnabled(true)
    scan
  }

  def initTsdb: TSDB = {

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
          BenchHBaseTestUtils
            .row(time - (time % testTable.rowTimeSpan), BenchHBaseTestUtils.dimAHash(dimId.toString), dimId.toShort)
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

    val mc = new ConsoleMetricQueryCollector(query, "test")

    new TSDB(dao, metricDao, dictProvider, identity, SimpleTsdbConfig(putEnabled = true)) {
      override def createMetricCollector(query: Query): MetricQueryCollector = {
        mc
      }
    }
  }


  val N = 1000000
  val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
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

  val tsdb: TSDB = initTsdb

}
// -prof hs_gc
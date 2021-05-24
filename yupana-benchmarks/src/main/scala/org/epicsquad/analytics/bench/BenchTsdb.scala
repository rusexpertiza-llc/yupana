package org.epicsquad.analytics.bench

import org.joda.time.LocalDateTime
import org.yupana.api.Time
import org.yupana.api.query.{ DataPoint, MetricExpr, Query, TimeExpr }
import org.yupana.api.query.Expression.Condition
import org.yupana.api.schema.Dimension
import org.yupana.core.dao._
import org.yupana.core.model._
import org.yupana.core.utils.metric.MetricQueryCollector
import org.yupana.core.{ MapReducible, QueryContext, SimpleTsdbConfig, TSDB }
import org.yupana.schema.{ ItemTableMetrics, SchemaRegistry, Tables }

object BenchTsdb {
  val query: Query = Query()
  private val postCondition = None
  private val queryContext = QueryContext(query, postCondition)
  private val rowBuilder = new InternalRowBuilder(queryContext)

  class BenchTsdb
      extends TSDB(
        SchemaRegistry.defaultSchema,
        new BenchDao(1000000),
        new BenchMetricDao,
        new DictionaryProviderImpl(new BenchDictDao),
        identity,
        SimpleTsdbConfig()
      ) {}

  class BenchDao(n: Int) extends TSDao[Iterator, Long] {

    val qtime = new LocalDateTime(2021, 5, 24, 22, 40, 0)
    private val rows = {
      val time = qtime.toDate.getTime + 24L * 60 * 60 * 1000
      (1 to n).map { i =>
        rowBuilder
          .set(TimeExpr, Time(time % Tables.itemsKkmTable.rowTimeSpan))
          .set(MetricExpr(ItemTableMetrics.sumField), BigDecimal(i.toDouble / 1000))
          .set(MetricExpr(ItemTableMetrics.quantityField), 101d - i.toDouble / 10000)
          .buildAndReset()
      }
    }

    override def put(dataPoints: Seq[DataPoint]): Unit = ???

    override def query(
        query: InternalQuery,
        valueDataBuilder: InternalRowBuilder,
        metricCollector: MetricQueryCollector
    ): Iterator[InternalRow] = rows.iterator

    override def mapReduceEngine(metricQueryCollector: MetricQueryCollector): MapReducible[Iterator] = ???

    override def isSupportedCondition(condition: Condition): Boolean = ???
  }

  class BenchDictDao extends DictionaryDao {
    override def createSeqId(dimension: Dimension): Int = ???
    override def getIdByValue(dimension: Dimension, value: String): Option[Long] = ???
    override def getIdsByValues(dimension: Dimension, value: Set[String]): Map[String, Long] = ???
    override def checkAndPut(dimension: Dimension, id: Long, value: String): Boolean = ???
  }

  class BenchMetricDao extends TsdbQueryMetricsDao {
    override def initializeQueryMetrics(query: Query, sparkQuery: Boolean): Unit = ???
    override def queriesByFilter(filter: Option[QueryMetricsFilter], limit: Option[Int]): Iterable[TsdbQueryMetrics] =
      ???

    override def updateQueryMetrics(
        queryId: String,
        queryState: QueryStates.QueryState,
        totalDuration: Double,
        metricValues: Map[String, MetricData],
        sparkQuery: Boolean
    ): Unit = ???

    override def setQueryState(filter: QueryMetricsFilter, queryState: QueryStates.QueryState): Unit = ???
    override def setRunningPartitions(queryId: String, partitions: Int): Unit = ???
    override def decrementRunningPartitions(queryId: String): Int = ???
    override def deleteMetrics(filter: QueryMetricsFilter): Int = ???
  }
}

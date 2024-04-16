package org.yupana.core.dao

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.query.DataPoint
import org.yupana.api.query.Expression.Condition
import org.yupana.api.schema.Table
import org.yupana.core.{IteratorMapReducible, MapReducible, QueryContext}
import org.yupana.core.model.{BatchDataset, DatasetSchema, InternalQuery, UpdateInterval}
import org.yupana.core.utils.metric.MetricQueryCollector

import java.time.{OffsetDateTime, ZoneOffset}

class TSDaoTest extends AnyFlatSpec with Matchers {

  "TSDao" should "pick only most recent update interval for each updated object after batch put" in {
    val intervals = (1 to 10).map { _ =>
      val from = OffsetDateTime.of(2021, 11, 1, 0, 0, 0, 0, ZoneOffset.UTC)
      val to = OffsetDateTime.of(2021, 11, 2, 0, 0, 0, 0, ZoneOffset.UTC)
      Seq(
        UpdateInterval("t1", from, to, OffsetDateTime.now(), "test"),
        UpdateInterval("t2", from, to, OffsetDateTime.now(), "test")
      )
    }

    val intervalsIterator = intervals.iterator

    val testDao = new TSDao[Iterator, Long] {
      override val dataPointsBatchSize: Int = 10
      override def putBatch(username: String)(dataPointsBatch: Seq[DataPoint]): Seq[UpdateInterval] = {
        intervalsIterator.next()
      }

      override def query(
          query: InternalQuery,
          queryContext: QueryContext,
          datasetSchema: DatasetSchema,
          metricCollector: MetricQueryCollector
      ): Iterator[BatchDataset] = ???

      override def mapReduceEngine(metricQueryCollector: MetricQueryCollector): MapReducible[Iterator] = ???
      override def isSupportedCondition(condition: Condition): Boolean = ???
    }

    val table = new Table("table", 1L, Seq.empty, Seq.empty, Seq.empty, 1L)
    val intervalsReturned = testDao.put(
      IteratorMapReducible.iteratorMR,
      List.fill(100)(DataPoint(table, 1L, Map.empty, Seq.empty)).iterator,
      "test"
    )

    intervalsReturned should have size 2
    intervalsReturned should contain theSameElementsAs intervals.last
  }
}

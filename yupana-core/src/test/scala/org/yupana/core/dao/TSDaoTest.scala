package org.yupana.core.dao

import org.joda.time.DateTime
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.query.DataPoint
import org.yupana.api.query.Expression.Condition
import org.yupana.api.schema.Table
import org.yupana.core.{ IteratorMapReducible, MapReducible }
import org.yupana.core.model.{ InternalQuery, InternalRow, InternalRowBuilder, UpdateInterval }
import org.yupana.core.utils.metric.MetricQueryCollector

class TSDaoTest extends AnyFlatSpec with Matchers {

  "TSDao" should "pick only most recent update interval for each updated object after batch put" in {
    val intervals = (1 to 10).map { _ =>
      val from = new DateTime("2021-11-01")
      val to = new DateTime("2021-11-02")
      Seq(
        UpdateInterval("t1", from, to, DateTime.now(), "test"),
        UpdateInterval("t2", from, to, DateTime.now(), "test")
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
          valueDataBuilder: InternalRowBuilder,
          metricCollector: MetricQueryCollector
      ): Iterator[InternalRow] = ???

      override def mapReduceEngine(metricQueryCollector: MetricQueryCollector): MapReducible[Iterator] = ???
      override def isSupportedCondition(condition: Condition): Boolean = ???
    }

    val table = new Table(1, "table", 1L, Seq.empty, Seq.empty, Seq.empty, 1L)
    val intervalsReturned = testDao.put(
      IteratorMapReducible.iteratorMR,
      List.fill(100)(DataPoint(table, 1L, Map.empty, Seq.empty)).iterator,
      "test"
    )

    intervalsReturned should have size 2
    intervalsReturned should contain theSameElementsAs intervals.last
  }
}

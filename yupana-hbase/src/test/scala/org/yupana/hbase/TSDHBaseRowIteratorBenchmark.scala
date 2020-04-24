package org.yupana.hbase

import org.joda.time.{ DateTimeZone, LocalDateTime }
import org.scalatest.tagobjects.Slow
import org.scalatest.{ FlatSpec, Matchers }
import org.yupana.api.Time
import org.yupana.api.query.Query
import org.yupana.api.query.syntax.All.{ and, const, dimension, ge, lt, metric, time }
import org.yupana.api.schema.Table
import org.yupana.core.{ QueryContext, TestDims, TestSchema, TestTableFields }
import org.yupana.core.TestSchema.testTable
import org.yupana.core.model.{ InternalQuery, InternalRowBuilder }
import org.yupana.core.utils.metric.NoMetricCollector

class TSDHBaseRowIteratorBenchmark extends FlatSpec with Matchers {

  "TSDHBaseRowIterator" should "be fast" taggedAs Slow in {
    val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
    val N = 10000000
    val rows = {
      val time = qtime.toDate.getTime + 24L * 60 * 60 * 1000
      (1 to N).map { i =>
        val dimId = i
        HBaseTestUtils
          .row(TSDRowKey(time - (time % testTable.rowTimeSpan), Array(Some(dimId), Some(dimId))))
          .cell("d1", time % testTable.rowTimeSpan)
          .field(TestTableFields.TEST_FIELD.tag, 1d)
          //          .field(TestTableFields.TEST_FIELD2.tag, 3d)
          .field(TestTableFields.TEST_BIGDECIMAL_FIELD.tag, BigDecimal(10.23))
          //          .field(TestTableFields.TEST_STRING_FIELD.tag, "test1")
          .field(Table.DIM_TAG_OFFSET, "test1")
          //          .field((Table.DIM_TAG_OFFSET + 1).toByte, "test2")
          .hbaseRow
      }
    }

    val exprs = Seq(
      time as "time_time",
      metric(TestTableFields.TEST_FIELD) as "testField",
      metric(TestTableFields.TEST_FIELD2) as "testField",
      metric(TestTableFields.TEST_BIGDECIMAL_FIELD) as "testFieldB",
      metric(TestTableFields.TEST_STRING_FIELD) as "testFieldB",
      dimension(TestDims.TAG_A) as "TAG_A",
      dimension(TestDims.TAG_B) as "TAG_B"
    )

    val query = Query(
      TestSchema.testTable,
      const(Time(10)),
      const(Time(20)),
      exprs,
      None,
      Seq.empty
    )

    val queryContext = QueryContext(query, None)

    val internalQuery =
      InternalQuery(
        TestSchema.testTable,
        exprs.map(_.expr).toSet,
        and(ge(time, const(Time(10))), lt(time, const(Time(20))))
      )
    val internalQueryContext = InternalQueryContext(internalQuery, NoMetricCollector)

    (1 to 100).foreach { i =>
      val start = System.currentTimeMillis()
      val it = new TSDHBaseRowIterator(internalQueryContext, rows.iterator, new InternalRowBuilder(queryContext))
      val c = it.foldLeft(i) { (a, r) =>
        a + 1
      }
      println(s"$c: time " + (System.currentTimeMillis() - start))
    }

  }
}

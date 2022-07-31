package org.yupana.hbase

import org.scalatest.tagobjects.Slow
import org.yupana.api.Time
import org.yupana.api.query.Query
import org.yupana.api.query.syntax.All._
import org.yupana.core.{ ExpressionCalculatorFactory, QueryContext, TestDims, TestSchema, TestTableFields }
import org.yupana.core.TestSchema.testTable
import org.yupana.core.model.{ InternalQuery, InternalRowBuilder }
import org.yupana.core.utils.metric.NoMetricCollector
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.{ LocalDateTime, ZoneOffset }

class TSDHBaseRowIteratorBenchmark extends AnyFlatSpec with Matchers {

  "TSDHBaseRowIterator" should "be fast" taggedAs Slow in {
    val qtime = LocalDateTime.of(2017, 10, 15, 12, 57).atOffset(ZoneOffset.UTC)
    val N = 10000000
    val rows = {
      val time = qtime.toInstant.toEpochMilli + 24L * 60 * 60 * 1000
      (1 to N).map { i =>
        val dimId = i
        HBaseTestUtils
          .row(time - (time % testTable.rowTimeSpan), HBaseTestUtils.dimAHash(dimId.toString), dimId.toShort)
          .cell("d1", time % testTable.rowTimeSpan)
          .field(TestTableFields.TEST_FIELD.tag, 1d)
          .field(TestTableFields.TEST_BIGDECIMAL_FIELD.tag, BigDecimal(10.23))
          .hbaseRow
      }
    }

    val exprs = Seq(
      time as "time_time",
      metric(TestTableFields.TEST_FIELD) as "testField",
      metric(TestTableFields.TEST_FIELD2) as "testField",
      metric(TestTableFields.TEST_BIGDECIMAL_FIELD) as "testFieldB",
      metric(TestTableFields.TEST_STRING_FIELD) as "testFieldB",
      dimension(TestDims.DIM_A) as "TAG_A",
      dimension(TestDims.DIM_B) as "TAG_B"
    )

    val query = Query(
      TestSchema.testTable,
      const(Time(10)),
      const(Time(20)),
      exprs,
      None,
      Seq.empty
    )

    val queryContext = new QueryContext(query, None, ExpressionCalculatorFactory, NoMetricCollector)

    val internalQuery =
      InternalQuery(
        TestSchema.testTable,
        exprs.map(_.expr).toSet,
        and(ge(time, const(Time(10))), lt(time, const(Time(20))))
      )
    val internalQueryContext = InternalQueryContext(internalQuery, NoMetricCollector)

    (1 to 1000).foreach { i =>
      val start = System.currentTimeMillis()
      val it = new TSDHBaseRowIterator(internalQueryContext, rows.iterator, new InternalRowBuilder(queryContext))
      val c = it.foldLeft(i) { (a, r) =>
        a + 1
      }
      println(s"$c: time " + (System.currentTimeMillis() - start))
    }

  }
}

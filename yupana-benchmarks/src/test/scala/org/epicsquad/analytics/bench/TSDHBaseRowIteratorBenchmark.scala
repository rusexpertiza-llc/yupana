package org.epicsquad.analytics.bench

import org.joda.time.{ DateTimeZone, LocalDateTime }
import org.openjdk.jmh.annotations.{ Benchmark, Scope, State }
import org.yupana.api.Time
import org.yupana.api.query.Query
import org.yupana.api.query.syntax.All._
import org.yupana.core.{ QueryContext, TestDims, TestSchema, TestTableFields }
import org.yupana.core.model.{ InternalQuery, InternalRowBuilder }
import org.yupana.core.utils.metric.NoMetricCollector
import org.yupana.hbase._

class TSDHBaseRowIteratorBenchmark {

  @Benchmark
  def iterate(state: TSDHBaseRowBencmarkState): Int = {
    val it = new TSDHBaseRowIterator(
      state.internalQueryContext,
      state.rows.iterator,
      new InternalRowBuilder(state.queryContext)
    )
    it.foldLeft(0) { (a, r) =>
      a + 1
    }
  }
}

@State(Scope.Benchmark)
class TSDHBaseRowBencmarkState {
  val qtime = new LocalDateTime(2017, 10, 15, 12, 57).toDateTime(DateTimeZone.UTC)
  val N = 10000000
  val rows = {
    val time = qtime.toDate.getTime + 24L * 60 * 60 * 1000
    (1 to N).map { i =>
      val dimId = i
      HBaseTestUtils
        .row(time - (time % TestSchema.testTable.rowTimeSpan), HBaseTestUtils.dimAHash(dimId.toString), dimId.toShort)
        .cell("d1", time % TestSchema.testTable.rowTimeSpan)
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

  val queryContext = QueryContext(query, None)

  val internalQuery =
    InternalQuery(
      TestSchema.testTable,
      exprs.map(_.expr).toSet,
      and(ge(time, const(Time(10))), lt(time, const(Time(20))))
    )
  val internalQueryContext = InternalQueryContext(internalQuery, NoMetricCollector)
}

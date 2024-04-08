///*
// * Copyright 2019 Rusexpertiza LLC
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.yupana.benchmarks
//
//import org.openjdk.jmh.annotations.{ Benchmark, Scope, State }
//import org.yupana.api.Time
//import org.yupana.api.query.Query
//import org.yupana.api.query.syntax.All._
//import org.yupana.core._
//import org.yupana.core.jit.JIT
//import org.yupana.core.model.{ InternalQuery, InternalRowBuilder }
//import org.yupana.core.utils.metric.NoMetricCollector
//import org.yupana.hbase.{ HBaseTestUtils, InternalQueryContext, TSDHBaseRowIterator }
//import org.yupana.utils.RussianTokenizer
//
//import java.time.{ LocalDateTime, ZoneOffset }
//
//class TSDHBaseRowIteratorBenchmark {
//
//  @Benchmark
//  def iterate(state: TSDHBaseRowBencmarkState): Int = {
//    val it = new TSDHBaseRowIterator(
//      state.internalQueryContext,
//      state.rows.iterator,
//      new InternalRowBuilder(state.queryContext)
//    )
//    it.foldLeft(0) { (a, r) =>
//      a + 1
//    }
//  }
//}
//
//@State(Scope.Benchmark)
//class TSDHBaseRowBencmarkState {
//  val qtime = LocalDateTime.of(2017, 10, 15, 12, 57).atOffset(ZoneOffset.UTC)
//  val N = 10000000
//  val rows = {
//    val time = qtime.toInstant.toEpochMilli + 24L * 60 * 60 * 1000
//    (1 to N).map { i =>
//      val dimId = i
//      HBaseTestUtils
//        .row(time - (time % TestSchema.testTable.rowTimeSpan), HBaseTestUtils.dimAHash(dimId.toString), dimId.toShort)
//        .cell("d1", time % TestSchema.testTable.rowTimeSpan)
//        .field(TestTableFields.TEST_FIELD.tag, 1d)
//        .field(TestTableFields.TEST_BIGDECIMAL_FIELD.tag, BigDecimal(10.23))
//        .hbaseRow
//    }
//  }
//
//  val exprs = Seq(
//    time as "time_time",
//    metric(TestTableFields.TEST_FIELD) as "testField",
//    metric(TestTableFields.TEST_FIELD2) as "testField",
//    metric(TestTableFields.TEST_BIGDECIMAL_FIELD) as "testFieldB",
//    metric(TestTableFields.TEST_STRING_FIELD) as "testFieldB",
//    dimension(TestDims.DIM_A) as "TAG_A",
//    dimension(TestDims.DIM_B) as "TAG_B"
//  )
//
//  val query = Query(
//    TestSchema.testTable,
//    const(Time(10)),
//    const(Time(20)),
//    exprs,
//    None,
//    Seq.empty
//  )
//
//  val queryContext = new QueryContext(query, None, JIT, NoMetricCollector)
//
//  implicit val calculator: ConstantCalculator = new ConstantCalculator(RussianTokenizer)
//
//  val internalQuery =
//    InternalQuery(
//      TestSchema.testTable,
//      exprs.map(_.expr).toSet,
//      and(ge(time, const(Time(10))), lt(time, const(Time(20))))
//    )
//  val internalQueryContext = InternalQueryContext(internalQuery, NoMetricCollector)
//}

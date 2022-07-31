package org.yupana.hbase

import org.yupana.api.Time
import org.yupana.api.query.Query
import org.yupana.core.{ ExpressionCalculatorFactory, QueryContext, TestDims, TestSchema, TestTableFields }
import org.yupana.core.model.{ InternalQuery, InternalRowBuilder }
import org.yupana.api.query.syntax.All.{ and, const, dimension, ge, lt, metric, time }
import org.yupana.core.utils.metric.NoMetricCollector
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TsdHBaseRowIteratorTest extends AnyFlatSpec with Matchers {

  val from = 100
  val to = 101

  val exprs = Seq(
    time as "time_time",
    metric(TestTableFields.TEST_FIELD) as "testField",
    metric(TestTableFields.TEST_FIELD2) as "testField2",
    metric(TestTableFields.TEST_STRING_FIELD) as "testStringField",
    metric(TestTableFields.TEST_LONG_FIELD) as "testlongField",
    dimension(TestDims.DIM_A) as "DIM_A",
    dimension(TestDims.DIM_B) as "DIM_B"
  )

  val query = Query(
    TestSchema.testTable,
    const(Time(from)),
    const(Time(to)),
    exprs,
    None,
    Seq.empty
  )

  val queryContext = new QueryContext(query, None, ExpressionCalculatorFactory, NoMetricCollector)

  val internalQuery =
    InternalQuery(
      TestSchema.testTable,
      exprs.map(_.expr).toSet,
      and(ge(time, const(Time(from))), lt(time, const(Time(to))))
    )
  val internalQueryContext = InternalQueryContext(internalQuery, NoMetricCollector)

  it should "iterate on one hbase row and one DataPoint" in {

    val rows = Iterator(
      HBaseTestUtils
        .row(1000, (10, 10L), 10.toShort)
        .cell("d1", 1)
        .field(TestTableFields.TEST_FIELD.tag, 42d)
        .field(TestTableFields.TEST_STRING_FIELD.tag, "e")
        .field(TestTableFields.TEST_FIELD2.tag, 43d)
        .field(TestTableFields.TEST_LONG_FIELD.tag, 44L)
        .hbaseRow
    )

    val it = new TSDHBaseRowIterator(internalQueryContext, rows, new InternalRowBuilder(queryContext))

    val dp1 = it.next()
    dp1.get[Double](queryContext, metric(TestTableFields.TEST_FIELD)) shouldBe 42d
    dp1.get[Double](queryContext, metric(TestTableFields.TEST_FIELD2)) shouldBe 43d
    dp1.get[String](queryContext, metric(TestTableFields.TEST_STRING_FIELD)) shouldBe "e"
    dp1.get[Long](queryContext, metric(TestTableFields.TEST_LONG_FIELD)) shouldBe 44L
    dp1.get[Time](queryContext, time) shouldBe Time(1001)
  }

  it should "iterate on one hbase row and one DataPoint adn two column families" in {

    val rows = Iterator(
      HBaseTestUtils
        .row(1000, (10, 10L), 10.toShort)
        .cell("d1", 1)
        .field(TestTableFields.TEST_FIELD.tag, 42d)
        .field(TestTableFields.TEST_STRING_FIELD.tag, "e")
        .cell("d2", 1)
        .field(TestTableFields.TEST_FIELD2.tag, 43d)
        .field(TestTableFields.TEST_LONG_FIELD.tag, 44L)
        .hbaseRow
    )

    val it = new TSDHBaseRowIterator(internalQueryContext, rows, new InternalRowBuilder(queryContext))

    val dp1 = it.next()
    dp1.get[Double](queryContext, metric(TestTableFields.TEST_FIELD)) shouldBe 42d
    dp1.get[Double](queryContext, metric(TestTableFields.TEST_FIELD2)) shouldBe 43d
    dp1.get[String](queryContext, metric(TestTableFields.TEST_STRING_FIELD)) shouldBe "e"
    dp1.get[Long](queryContext, metric(TestTableFields.TEST_LONG_FIELD)) shouldBe 44L
    dp1.get[Time](queryContext, time) shouldBe Time(1001)
  }

  it should "iterate on one hbase row and two DataPoints" in {

    val rows = Iterator(
      HBaseTestUtils
        .row(1000, (10, 10L), 10.toShort)
        .cell("d1", 1)
        .field(TestTableFields.TEST_FIELD.tag, 42d)
        .field(TestTableFields.TEST_STRING_FIELD.tag, "e")
        .field(TestTableFields.TEST_FIELD2.tag, 43d)
        .field(TestTableFields.TEST_LONG_FIELD.tag, 44L)
        .cell("d1", 2)
        .field(TestTableFields.TEST_FIELD.tag, 52d)
        .field(TestTableFields.TEST_STRING_FIELD.tag, "ee")
        .field(TestTableFields.TEST_FIELD2.tag, 53d)
        .field(TestTableFields.TEST_LONG_FIELD.tag, 54L)
        .hbaseRow
    )

    val it = new TSDHBaseRowIterator(internalQueryContext, rows, new InternalRowBuilder(queryContext))

    val dp1 = it.next()
    dp1.get[Double](queryContext, metric(TestTableFields.TEST_FIELD)) shouldBe 42d
    dp1.get[Double](queryContext, metric(TestTableFields.TEST_FIELD2)) shouldBe 43d
    dp1.get[String](queryContext, metric(TestTableFields.TEST_STRING_FIELD)) shouldBe "e"
    dp1.get[Long](queryContext, metric(TestTableFields.TEST_LONG_FIELD)) shouldBe 44L
    dp1.get[Time](queryContext, time) shouldBe Time(1001)

    val dp2 = it.next()
    dp2.get[Double](queryContext, metric(TestTableFields.TEST_FIELD)) shouldBe 52d
    dp2.get[Double](queryContext, metric(TestTableFields.TEST_FIELD2)) shouldBe 53d
    dp2.get[String](queryContext, metric(TestTableFields.TEST_STRING_FIELD)) shouldBe "ee"
    dp2.get[Long](queryContext, metric(TestTableFields.TEST_LONG_FIELD)) shouldBe 54L
    dp2.get[Time](queryContext, time) shouldBe Time(1002)
  }

  it should "iterate on one hbase row and two DataPoints and two families" in {

    val rows = Iterator(
      HBaseTestUtils
        .row(1000, (10, 10L), 10.toShort)
        .cell("d1", 1)
        .field(TestTableFields.TEST_FIELD.tag, 42d)
        .field(TestTableFields.TEST_STRING_FIELD.tag, "e")
        .cell("d1", 2)
        .field(TestTableFields.TEST_FIELD.tag, 52d)
        .field(TestTableFields.TEST_STRING_FIELD.tag, "ee")
        .cell("d2", 1)
        .field(TestTableFields.TEST_FIELD2.tag, 43d)
        .field(TestTableFields.TEST_LONG_FIELD.tag, 44L)
        .cell("d2", 2)
        .field(TestTableFields.TEST_FIELD2.tag, 53d)
        .field(TestTableFields.TEST_LONG_FIELD.tag, 54L)
        .hbaseRow
    )

    val it = new TSDHBaseRowIterator(internalQueryContext, rows, new InternalRowBuilder(queryContext))

    val dp1 = it.next()
    dp1.get[Double](queryContext, metric(TestTableFields.TEST_FIELD)) shouldBe 42d
    dp1.get[Double](queryContext, metric(TestTableFields.TEST_FIELD2)) shouldBe 43d
    dp1.get[String](queryContext, metric(TestTableFields.TEST_STRING_FIELD)) shouldBe "e"
    dp1.get[Long](queryContext, metric(TestTableFields.TEST_LONG_FIELD)) shouldBe 44L
    dp1.get[Time](queryContext, time) shouldBe Time(1001)

    val dp2 = it.next()
    dp2.get[Double](queryContext, metric(TestTableFields.TEST_FIELD)) shouldBe 52d
    dp2.get[Double](queryContext, metric(TestTableFields.TEST_FIELD2)) shouldBe 53d
    dp2.get[String](queryContext, metric(TestTableFields.TEST_STRING_FIELD)) shouldBe "ee"
    dp2.get[Long](queryContext, metric(TestTableFields.TEST_LONG_FIELD)) shouldBe 54L
    dp2.get[Time](queryContext, time) shouldBe Time(1002)
  }

  it should "iterate on two hbase row and two DataPoints and two families" in {

    val rows = Iterator(
      HBaseTestUtils
        .row(1000, (10, 10L), 10.toShort)
        .cell("d1", 1)
        .field(TestTableFields.TEST_FIELD.tag, 42d)
        .field(TestTableFields.TEST_STRING_FIELD.tag, "e")
        .cell("d1", 2)
        .field(TestTableFields.TEST_FIELD.tag, 52d)
        .field(TestTableFields.TEST_STRING_FIELD.tag, "ee")
        .cell("d2", 1)
        .field(TestTableFields.TEST_FIELD2.tag, 43d)
        .field(TestTableFields.TEST_LONG_FIELD.tag, 44L)
        .cell("d2", 2)
        .field(TestTableFields.TEST_FIELD2.tag, 53d)
        .field(TestTableFields.TEST_LONG_FIELD.tag, 54L)
        .hbaseRow,
      HBaseTestUtils
        .row(1000, (10, 10L), 10.toShort)
        .cell("d1", 1)
        .field(TestTableFields.TEST_FIELD.tag, 142d)
        .field(TestTableFields.TEST_STRING_FIELD.tag, "2e")
        .cell("d1", 2)
        .field(TestTableFields.TEST_FIELD.tag, 152d)
        .field(TestTableFields.TEST_STRING_FIELD.tag, "2ee")
        .cell("d2", 1)
        .field(TestTableFields.TEST_FIELD2.tag, 143d)
        .field(TestTableFields.TEST_LONG_FIELD.tag, 144L)
        .cell("d2", 2)
        .field(TestTableFields.TEST_FIELD2.tag, 153d)
        .field(TestTableFields.TEST_LONG_FIELD.tag, 154L)
        .hbaseRow
    )

    val it = new TSDHBaseRowIterator(internalQueryContext, rows, new InternalRowBuilder(queryContext))

    val dp1 = it.next()
    dp1.get[Double](queryContext, metric(TestTableFields.TEST_FIELD)) shouldBe 42d
    dp1.get[Double](queryContext, metric(TestTableFields.TEST_FIELD2)) shouldBe 43d
    dp1.get[String](queryContext, metric(TestTableFields.TEST_STRING_FIELD)) shouldBe "e"
    dp1.get[Long](queryContext, metric(TestTableFields.TEST_LONG_FIELD)) shouldBe 44L
    dp1.get[Time](queryContext, time) shouldBe Time(1001)

    val dp2 = it.next()
    dp2.get[Double](queryContext, metric(TestTableFields.TEST_FIELD)) shouldBe 52d
    dp2.get[Double](queryContext, metric(TestTableFields.TEST_FIELD2)) shouldBe 53d
    dp2.get[String](queryContext, metric(TestTableFields.TEST_STRING_FIELD)) shouldBe "ee"
    dp2.get[Long](queryContext, metric(TestTableFields.TEST_LONG_FIELD)) shouldBe 54L
    dp2.get[Time](queryContext, time) shouldBe Time(1002)

    val dp3 = it.next()
    dp3.get[Double](queryContext, metric(TestTableFields.TEST_FIELD)) shouldBe 142d
    dp3.get[Double](queryContext, metric(TestTableFields.TEST_FIELD2)) shouldBe 143d
    dp3.get[String](queryContext, metric(TestTableFields.TEST_STRING_FIELD)) shouldBe "2e"
    dp3.get[Long](queryContext, metric(TestTableFields.TEST_LONG_FIELD)) shouldBe 144L
    dp3.get[Time](queryContext, time) shouldBe Time(1001)

    val dp4 = it.next()
    dp4.get[Double](queryContext, metric(TestTableFields.TEST_FIELD)) shouldBe 152d
    dp4.get[Double](queryContext, metric(TestTableFields.TEST_FIELD2)) shouldBe 153d
    dp4.get[String](queryContext, metric(TestTableFields.TEST_STRING_FIELD)) shouldBe "2ee"
    dp4.get[Long](queryContext, metric(TestTableFields.TEST_LONG_FIELD)) shouldBe 154L
    dp4.get[Time](queryContext, time) shouldBe Time(1002)
  }

}

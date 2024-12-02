package org.yupana.hbase

import org.yupana.api.Time
import org.yupana.api.query.Query
import org.yupana.core._
import org.yupana.core.model.InternalQuery
import org.yupana.api.query.syntax.All.{ and, const, dimension, ge, lt, metric, time }
import org.yupana.core.utils.metric.NoMetricCollector
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.core.jit.JIT
import org.yupana.utils.RussianTokenizer

import java.time.LocalDateTime

class TsdHBaseRowIteratorTest extends AnyFlatSpec with Matchers {

  val from = 100
  val to = 101
  val now = Time(LocalDateTime.now())

  implicit private val calculator: ConstantCalculator = new ConstantCalculator(RussianTokenizer)

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

  val queryContext = new QueryContext(query, now, None, RussianTokenizer, JIT, NoMetricCollector)

  val datasetSchema = queryContext.datasetSchema

  val internalQuery =
    InternalQuery(
      TestSchema.testTable,
      exprs.map(_.expr).toSet,
      and(ge(time, const(Time(from))), lt(time, const(Time(to)))),
      IndexedSeq.empty
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

    val it = new TSDHBaseRowIterator(internalQueryContext, rows, datasetSchema)

    val batch = it.next()
    batch.get[Double](0, metric(TestTableFields.TEST_FIELD)) shouldBe 42d
    batch.get[Double](0, metric(TestTableFields.TEST_FIELD2)) shouldBe 43d
    batch.get[String](0, metric(TestTableFields.TEST_STRING_FIELD)) shouldBe "e"
    batch.get[Long](0, metric(TestTableFields.TEST_LONG_FIELD)) shouldBe 44L
    batch.get[Time](0, time) shouldBe Time(1001)
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

    val it = new TSDHBaseRowIterator(internalQueryContext, rows, datasetSchema)

    val batch = it.next()
    batch.get[Double](0, metric(TestTableFields.TEST_FIELD)) shouldBe 42d
    batch.get[Double](0, metric(TestTableFields.TEST_FIELD2)) shouldBe 43d
    batch.get[String](0, metric(TestTableFields.TEST_STRING_FIELD)) shouldBe "e"
    batch.get[Long](0, metric(TestTableFields.TEST_LONG_FIELD)) shouldBe 44L
    batch.get[Time](0, time) shouldBe Time(1001)
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

    val it = new TSDHBaseRowIterator(internalQueryContext, rows, datasetSchema)

    val batch = it.next()
    batch.get[Double](0, metric(TestTableFields.TEST_FIELD)) shouldBe 42d
    batch.get[Double](0, metric(TestTableFields.TEST_FIELD2)) shouldBe 43d
    batch.get[String](0, metric(TestTableFields.TEST_STRING_FIELD)) shouldBe "e"
    batch.get[Long](0, metric(TestTableFields.TEST_LONG_FIELD)) shouldBe 44L
    batch.get[Time](0, time) shouldBe Time(1001)

    batch.get[Double](1, metric(TestTableFields.TEST_FIELD)) shouldBe 52d
    batch.get[Double](1, metric(TestTableFields.TEST_FIELD2)) shouldBe 53d
    batch.get[String](1, metric(TestTableFields.TEST_STRING_FIELD)) shouldBe "ee"
    batch.get[Long](1, metric(TestTableFields.TEST_LONG_FIELD)) shouldBe 54L
    batch.get[Time](1, time) shouldBe Time(1002)
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

    val it = new TSDHBaseRowIterator(internalQueryContext, rows, datasetSchema)

    val batch = it.next()
    batch.get[Double](0, metric(TestTableFields.TEST_FIELD)) shouldBe 42d
    batch.get[Double](0, metric(TestTableFields.TEST_FIELD2)) shouldBe 43d
    batch.get[String](0, metric(TestTableFields.TEST_STRING_FIELD)) shouldBe "e"
    batch.get[Long](0, metric(TestTableFields.TEST_LONG_FIELD)) shouldBe 44L
    batch.get[Time](0, time) shouldBe Time(1001)

    batch.get[Double](1, metric(TestTableFields.TEST_FIELD)) shouldBe 52d
    batch.get[Double](1, metric(TestTableFields.TEST_FIELD2)) shouldBe 53d
    batch.get[String](1, metric(TestTableFields.TEST_STRING_FIELD)) shouldBe "ee"
    batch.get[Long](1, metric(TestTableFields.TEST_LONG_FIELD)) shouldBe 54L
    batch.get[Time](1, time) shouldBe Time(1002)
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

    val it = new TSDHBaseRowIterator(internalQueryContext, rows, datasetSchema)

    val batch = it.next()
    batch.get[Double](0, metric(TestTableFields.TEST_FIELD)) shouldBe 42d
    batch.get[Double](0, metric(TestTableFields.TEST_FIELD2)) shouldBe 43d
    batch.get[String](0, metric(TestTableFields.TEST_STRING_FIELD)) shouldBe "e"
    batch.get[Long](0, metric(TestTableFields.TEST_LONG_FIELD)) shouldBe 44L
    batch.get[Time](0, time) shouldBe Time(1001)

    batch.get[Double](1, metric(TestTableFields.TEST_FIELD)) shouldBe 52d
    batch.get[Double](1, metric(TestTableFields.TEST_FIELD2)) shouldBe 53d
    batch.get[String](1, metric(TestTableFields.TEST_STRING_FIELD)) shouldBe "ee"
    batch.get[Long](1, metric(TestTableFields.TEST_LONG_FIELD)) shouldBe 54L
    batch.get[Time](1, time) shouldBe Time(1002)

    batch.get[Double](2, metric(TestTableFields.TEST_FIELD)) shouldBe 142d
    batch.get[Double](2, metric(TestTableFields.TEST_FIELD2)) shouldBe 143d
    batch.get[String](2, metric(TestTableFields.TEST_STRING_FIELD)) shouldBe "2e"
    batch.get[Long](2, metric(TestTableFields.TEST_LONG_FIELD)) shouldBe 144L
    batch.get[Time](2, time) shouldBe Time(1001)

    batch.get[Double](3, metric(TestTableFields.TEST_FIELD)) shouldBe 152d
    batch.get[Double](3, metric(TestTableFields.TEST_FIELD2)) shouldBe 153d
    batch.get[String](3, metric(TestTableFields.TEST_STRING_FIELD)) shouldBe "2ee"
    batch.get[Long](3, metric(TestTableFields.TEST_LONG_FIELD)) shouldBe 154L
    batch.get[Time](3, time) shouldBe Time(1002)
  }

}

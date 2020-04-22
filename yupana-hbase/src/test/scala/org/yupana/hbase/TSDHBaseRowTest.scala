package org.yupana.hbase

import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.{ FlatSpec, Matchers }
import org.yupana.api.Time
import org.yupana.api.query.syntax.All.{ and, const, ge, lt, time }
import org.yupana.api.schema.Table
import org.yupana.api.types.DataType
import org.yupana.core.TestSchema
import org.yupana.core.model.InternalQuery
import org.yupana.core.utils.metric.NoMetricCollector

class TSDHBaseRowTest extends FlatSpec with Matchers {

  "TSDBHBaseRow" should "iterate rows" in {
    val from = 100
    val to = 101

    val query =
      InternalQuery(TestSchema.testTable, Set.empty, and(ge(time, const(Time(from))), lt(time, const(Time(to)))))
    val context = InternalQueryContext(query, NoMetricCollector)

    val hrow = HBaseTestUtils
      .row(TSDRowKey(10, Array(Some(10), Some(10))))
      .cell(1, 1, 10d)
      .cell(2, 1, 10d)
      .cell(3, 1, 10d)
      .cell(1, 2, "10")
      .cell(2, 2, "10")
      .cell(3, 2, "10")
      .cell(1, 3, 10d)
      .cell(4, 3, 10d)
      .cell(5, 3, 10d)
      .cell(1, 4, 10L)
      .hbaseRow

    val r = new TSDHBaseRow(
      context,
      hrow,
      Array.ofDim[Int](Table.MAX_TAGS),
      Array.ofDim[Int](Table.MAX_TAGS),
      Array.ofDim[Int](Table.MAX_TAGS)
    )

    val data = Array.ofDim[Option[Any]](255)
    val it = r.iterator(data)

    val (time1, row1) = it.next()
    time1 shouldBe 11
    row1(1) shouldBe Some(10d)
    row1(2) shouldBe Some("10")
    row1(3) shouldBe Some(10d)
    row1(4) shouldBe Some(10L)

    val (time2, row2) = it.next()
    time2 shouldBe 12
    row2(1) shouldBe Some(10d)
    row2(2) shouldBe Some("10")
    row2(3) shouldBe None
    row2(4) shouldBe None

    val (time3, row3) = it.next()
    time3 shouldBe 13
    row3(1) shouldBe Some(10d)
    row3(2) shouldBe Some("10")
    row3(3) shouldBe None
    row3(4) shouldBe None

    val (time4, row4) = it.next()
    time4 shouldBe 14
    row4(1) shouldBe None
    row4(2) shouldBe None
    row4(3) shouldBe Some(10d)
    row4(4) shouldBe None

    val (time5, row5) = it.next()
    time5 shouldBe 15
    row5(1) shouldBe None
    row5(2) shouldBe None
    row5(3) shouldBe Some(10)
    row5(4) shouldBe None
  }
}

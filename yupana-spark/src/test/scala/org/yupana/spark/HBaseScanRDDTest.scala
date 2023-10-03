package org.yupana.spark

import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HBaseScanRDDTest extends AnyFlatSpec with Matchers {

  it should "split byte ranges well" in {
    val ranges = Array(
      (Bytes.toBytes(0L), Bytes.toBytes(30L)),
      (Bytes.toBytes(30L), Bytes.toBytes(50L)),
      (Bytes.toBytes(60L), Bytes.toBytes(100L))
    )
    val splitted = HBaseScanRDD.splitRanges(10, ranges)
    splitted.length shouldBe 12
    splitted.flatMap {
      case (f, t) => Seq(Bytes.toLong(f), Bytes.toLong(t))
    }.distinct should contain theSameElementsAs Seq(0, 7, 15, 22, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100).map(_.toLong)
  }
}

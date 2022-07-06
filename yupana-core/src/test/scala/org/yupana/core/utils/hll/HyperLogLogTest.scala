package org.yupana.core.utils.hll

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class HyperLogLogTest extends AnyFlatSpec with Matchers {

  it should "work if some key is missing" in {
    val hll = HyperLogLog(14, 15)
    val values = (1 to 54200000).map(i => {
      val value = Random.nextInt(17939584)
      hll.addValue(value)
      value
    })
    val actualCount = values.toSet.size
    val estimatedCount = hll.getCount
    actualCount shouldEqual (estimatedCount)
  }

}

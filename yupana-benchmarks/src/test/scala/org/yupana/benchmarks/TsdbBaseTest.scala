package org.yupana.benchmarks

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TsdbBaseTest extends AnyFlatSpec with Matchers {

  val bench = new TsdbBaseBenchmark

  "TsdbBase" should "return correct value" in {
    bench.processRowsWithAgg(new TsdbBaseBenchmarkStateAgg) shouldEqual 1000
  }
}

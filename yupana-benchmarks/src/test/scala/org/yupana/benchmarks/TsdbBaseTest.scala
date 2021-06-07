package org.yupana.benchmarks

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TsdbBaseTest extends AnyFlatSpec with Matchers {

  val bench = new TsdbBaseBenchmark

  "TsdbBase" should "return correct value" in {
    bench.processRows(new TsdbBaseBenchmarkState()) should have size 1000
  }
}

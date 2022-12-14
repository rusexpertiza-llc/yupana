package org.yupana.benchmarks

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.core.cache.CacheFactory
import org.yupana.core.settings.Settings

import java.util.Properties

class TsdbBaseTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  val bench = new ProcessRowsWithAggBenchmark

  override protected def beforeAll(): Unit = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("app.properties"))
    CacheFactory.init(Settings(properties))
  }

  "TsdbBase" should "return correct value" in {
    bench.processRowsWithAgg(new TsdbBaseBenchmarkStateAgg) shouldEqual 1000
  }
}

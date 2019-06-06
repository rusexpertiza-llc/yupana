package org.yupana.hbase

import org.apache.hadoop.hbase.client.Scan
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers, OptionValues}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.yupana.api.schema.Table
import org.yupana.core.MapReducible
import org.yupana.core.dao.DictionaryProvider
import org.yupana.core.utils.metric.MetricQueryCollector

class TSDaoHbaseTest2 extends FlatSpec with Matchers with MockFactory with BeforeAndAfterAll with BeforeAndAfterEach with OptionValues with ScalaCheckDrivenPropertyChecks {

  val tsDaoHbase =  new TSDao

  "TSDaoHbaseTest2" should "calculate ranges for hashed dimensions" in {

    val d = Set(1,2,3,4, 10,20,30,40,50).map(x => x.toLong << 32)

    tsDaoHbase.hashedDimensionRanges(d, 2) shouldBe Set(1l -> 10l, 20l ->50l).map(x => (x._1.toLong << 32, x._2 << 32))

    tsDaoHbase.hashedDimensionRanges(Set(0, -10, 6).map(x => x.toLong << 32), 10) shouldBe Set(-10l -> -10l, 0l ->0l, 6l -> 6l).map(x => (x._1.toLong << 32, x._2 << 32))

    tsDaoHbase.hashedDimensionRanges(Set(0l, -9223372036854775808l, 9223372036854775807l, -4095256015281639068l, -6284357675865131842l, 2101087910754406243l, 104940634198531098l, -9152208279343540988l, -2913891688056621032l), 10) shouldBe Set(-9223372036854775808l -> -9223372036854775808l, 9223372036854775807l -> 9223372036854775807l, 0l -> 0l, 104940634198531098l -> 104940634198531098l, -6284357675865131842l -> -6284357675865131842l, -4095256015281639068l -> -4095256015281639068l, 2101087910754406243l -> 2101087910754406243l, -2913891688056621032l -> -2913891688056621032l, -9152208279343540988l -> -9152208279343540988l)

  }

  it should "perform range calculation for any set" in {
     forAll { xs: Set[Long] =>
       tsDaoHbase.hashedDimensionRanges(xs, 10) should have size math.min(xs.map(_ >>> 32).size, 10)
     }
  }

  class TSDao extends TSDaoHBaseBase[Seq] {
    override def mr: MapReducible[Seq] = ???

    override def dictionaryProvider: DictionaryProvider = ???

    override def executeScans(table: Table, scans: Seq[Scan], metricCollector: MetricQueryCollector): Seq[TSDOutputRow[IdType]] = ???
  }

}

package org.yupana.hbase

import org.apache.hadoop.hbase.client.Scan
import org.scalamock.scalatest.MockFactory
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers, OptionValues }
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.yupana.api.schema.Table
import org.yupana.core.MapReducible
import org.yupana.core.dao.DictionaryProvider
import org.yupana.core.utils.metric.MetricQueryCollector

class TSDaoHbaseTest2
    extends FlatSpec
    with Matchers
    with MockFactory
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with OptionValues
    with ScalaCheckDrivenPropertyChecks {

  val tsDaoHbase = new TSDao

  "TSDaoHbaseTest2" should "calculate ranges for hashed dimensions" in {

    val d = Set(1, 2, 3, 4, 10, 20, 30, 40, 50).map(x => x.toLong << 32)

    tsDaoHbase.hashedDimensionRanges(d, 2) shouldBe Set(1L -> 10L, 20L -> 50L).map(x => (x._1.toLong << 32, x._2 << 32))

    tsDaoHbase.hashedDimensionRanges(Set(0, -10, 6).map(x => x.toLong << 32), 10) shouldBe Set(
      -10L -> -10L,
      0L -> 0L,
      6L -> 6L
    ).map(x => (x._1.toLong << 32, x._2 << 32))

    tsDaoHbase.hashedDimensionRanges(
      Set(0L, -9223372036854775808L, 9223372036854775807L, -4095256015281639068L, -6284357675865131842L,
        2101087910754406243L, 104940634198531098L, -9152208279343540988L, -2913891688056621032L),
      10
    ) shouldBe Set(
      -9223372036854775808L -> -9223372036854775808L,
      9223372036854775807L -> 9223372036854775807L,
      0L -> 0L,
      104940634198531098L -> 104940634198531098L,
      -6284357675865131842L -> -6284357675865131842L,
      -4095256015281639068L -> -4095256015281639068L,
      2101087910754406243L -> 2101087910754406243L,
      -2913891688056621032L -> -2913891688056621032L,
      -9152208279343540988L -> -9152208279343540988L
    )

  }

  it should "perform range calculation for any set" in {
    forAll { xs: Set[Long] =>
      tsDaoHbase.hashedDimensionRanges(xs, 10) should have size math.min(xs.map(_ >>> 32).size, 10)
    }
  }

  class TSDao extends TSDaoHBaseBase[Seq] {
    override def mr: MapReducible[Seq] = ???

    override def dictionaryProvider: DictionaryProvider = ???

    override def executeScans(
        table: Table,
        scans: Seq[Scan],
        metricCollector: MetricQueryCollector
    ): Seq[TSDOutputRow[IdType]] = ???
  }

}

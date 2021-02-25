package org.yupana.schema

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ItemDimensionTest extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks {
  "ItemDimension" should "split items by words" in {
    val items = Table(
      ("item", "split"),
      ("тарелка супа", Seq("тарелка", "супа")),
      ("15 кг овощей", Seq("кг", "овощей")),
      ("a50 слово 44ggg aa15bb 33ff22", Seq("слово")),
      ("a50 word 44ggg aa15bb", Seq("word")),
      ("a50 bird 44ggg", Seq("bird")),
      ("a50 heard ggg44", Seq("heard"))
    )

    forAll(items) { (item, split) =>
      ItemDimension.split(item) shouldEqual split
    }
  }
}

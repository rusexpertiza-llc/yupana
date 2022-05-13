package org.yupana.khipu

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import TestUtils._
import org.yupana.core.TestSchema

class TableTest extends AnyFlatSpec with Matchers {

  "A Table" should "put row" in {

    val table = KTable.empty(TestSchema.testTable)

    val row = testRowVal(1, 1)
    table.put(row)

    val c = table.scan()

    c.next() shouldBe true
    c.keyBytes() shouldBe row.key
    c.valueBytes() shouldBe row.value
  }

  it should "put a lot of rows" in {

    val table = KTable.empty(TestSchema.testTable)

    val rows = (1 to 1000).map(i => testRowVal(i, i))
    table.put(rows)

    val c = table.scan()
    rows.foreach { r =>
      c.next() shouldBe true
      c.keyBytes() shouldBe r.key
      c.valueBytes() shouldBe r.value
    }

  }

}

package org.yupana.khipu

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.utils.SortedSetIterator
import org.yupana.core.TestSchema
import org.yupana.khipu.storage.{ KTable, Prefix }

import java.nio.charset.StandardCharsets

class CursorTest extends AnyFlatSpec with Matchers {

  import TestUtils._

  "A Cursor" should "full scan" in {

    val table = KTable.allocateHeap(TestSchema.testTable)

    val rows = Seq(
      testRowStr("aaa", "aaa"),
      testRowStr("bbb", "bbb"),
      testRowStr("ccc", "ccc")
    )

    rows.foreach(table.put)

    val c = table.scan()

    rows.foreach { r =>
      c.next() shouldBe true

      c.keyBytes() shouldBe r.keyBytes
      c.valueBytes() shouldBe r.valueBytes
    }

    c.next() shouldBe false
  }

  it should "scan an empty table " in {
    val table = KTable.allocateHeap(TestSchema.testTable)
    val c = table.scan()
    c.next() shouldBe false
  }

  it should "prefix scan for single prefix" in {
    val table = KTable.allocateHeap(TestSchema.testTable)

    val rows = Seq(
      testRowStr("aaa", "aaa"),
      testRowStr("bbb", "bbb"),
      testRowStr("cca", "cca"),
      testRowStr("ccb", "ccb"),
      testRowStr("ddd", "ddd")
    )

    rows.foreach(table.put)

    val c1 = table.scan(SortedSetIterator(Prefix("bb")))

    c1.next() shouldBe true
    c1.keyBytes() shouldBe rows(1).keyBytes
    c1.valueBytes() shouldBe rows(1).valueBytes
    c1.next() shouldBe false

    val c2 = table.scan(SortedSetIterator(Prefix("cc")))

    c2.next() shouldBe true
    c2.keyBytes() shouldBe rows(2).keyBytes
    c2.valueBytes() shouldBe rows(2).valueBytes

    c2.next() shouldBe true
    c2.keyBytes() shouldBe rows(3).keyBytes
    c2.valueBytes() shouldBe rows(3).valueBytes

    c2.next() shouldBe false

    val c3 = table.scan(SortedSetIterator(Prefix("aa")))

    c3.next() shouldBe true
    c3.keyBytes() shouldBe rows(0).keyBytes
    c3.valueBytes() shouldBe rows(0).valueBytes
    c3.next() shouldBe false

    val c4 = table.scan(SortedSetIterator(Prefix("d")))

    c4.next() shouldBe true
    c4.keyBytes() shouldBe rows(4).keyBytes
    c4.valueBytes() shouldBe rows(4).valueBytes
    c4.next() shouldBe false
  }

  it should "prefix scan for multiple prefix" in {
    val table = KTable.allocateHeap(TestSchema.testTable)

    val rows = Seq(
      testRowStr("aaa", "aaa"),
      testRowStr("bbb", "bbb"),
      testRowStr("ccc", "ccc"),
      testRowStr("ddd", "ddd")
    )

    rows.foreach(table.put)

    val c = table.scan(SortedSetIterator(Prefix("bb"), Prefix("cc")))
    c.next() shouldBe true

    c.keyBytes() shouldBe rows(1).keyBytes
    c.valueBytes() shouldBe rows(1).valueBytes

    c.next() shouldBe true

    c.keyBytes() shouldBe rows(2).keyBytes
    c.valueBytes() shouldBe rows(2).valueBytes
    c.next() shouldBe false
  }

  it should "prefix scan multiple blocks table for multiple prefix" in {
    val table = KTable.allocateHeap(TestSchema.testTable)

    val prefixes = Seq("aaaa", "bbbb", "ccccc", "ddd", "ee")

    val keys = for {
      p <- prefixes
      i <- 0 to 10000
    } yield {
      s"$p$i".padTo(30, '0')
    }

    val rows = keys.map(k => testRowStr(k, k))

    table.put(rows)

    val c = table.scan(SortedSetIterator(Prefix("bb"), Prefix("cc")))

    val expectedKeys = keys.filter(k => k.startsWith("bb") || k.startsWith("cc")).sorted

    expectedKeys.foreach { expected =>
      c.next() shouldBe true
      c.keyBytes() shouldBe expected.getBytes(StandardCharsets.UTF_8)
      c.valueBytes() shouldBe expected.getBytes(StandardCharsets.UTF_8)
    }
  }
}

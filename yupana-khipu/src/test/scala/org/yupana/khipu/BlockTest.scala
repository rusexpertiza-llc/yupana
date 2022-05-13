package org.yupana.khipu

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.core.TestSchema

import TestUtils._

class BlockTest extends AnyFlatSpec with Matchers {

  "A Block" should "allocate new block and put rows" in {

    val block = LeafBlock.empty(TestSchema.testTable)

    val row = Row(Array.fill[Byte](block.header.keySize)(1.toByte), Array.fill[Byte](100)(2.toByte))
    val blocks1 = block.put(Seq(row))

    val c1 = new Cursor(TestSchema.testTable, blocks1, None)
    c1.next() shouldBe true
    c1.keyBytes().toSeq shouldBe row.key.toSeq
    c1.valueBytes().toSeq shouldBe row.value.toSeq

    val rows = Seq(testRowFill(1, 1), testRowFill(2, 2), testRowFill(3, 3))
    val blocks2 = block.put(rows)

    val c2 = new Cursor(TestSchema.testTable, blocks2, None)
    c2.next() shouldBe true
    c2.keyBytes() shouldBe rows(0).key
    c2.valueBytes() shouldBe rows(0).value

    c2.next() shouldBe true
    c2.keyBytes() shouldBe rows(1).key
    c2.valueBytes() shouldBe rows(1).value

    c2.next() shouldBe true
    c2.keyBytes() shouldBe rows(2).key
    c2.valueBytes() shouldBe rows(2).value

  }

  it should "put rows to existing not empty block" in {

    val initblock = LeafBlock.empty(TestSchema.testTable)
    val initRows = Seq(testRowFill(10, 15), testRowFill(20, 25), testRowFill(30, 35))

    val block = initblock.put(initRows).head

    val rows = Seq(testRowFill(1, 6), testRowFill(21, 26), testRowFill(31, 36))

    val r = block.put(rows)

    val c = new Cursor(TestSchema.testTable, r, None)

    c.next() shouldBe true
    c.keyBytes() shouldBe testRowFill(1, 6).key
    c.valueBytes() shouldBe testRowFill(1, 6).value

    c.next() shouldBe true
    c.keyBytes() shouldBe testRowFill(10, 15).key
    c.valueBytes() shouldBe testRowFill(10, 15).value

    c.next() shouldBe true
    c.keyBytes() shouldBe testRowFill(20, 25).key
    c.valueBytes() shouldBe testRowFill(20, 25).value

    c.next() shouldBe true
    c.keyBytes() shouldBe testRowFill(21, 26).key
    c.valueBytes() shouldBe testRowFill(21, 26).value

    c.next() shouldBe true
    c.keyBytes() shouldBe testRowFill(30, 35).key
    c.valueBytes() shouldBe testRowFill(30, 35).value

    c.next() shouldBe true
    c.keyBytes() shouldBe testRowFill(31, 36).key
    c.valueBytes() shouldBe testRowFill(31, 36).value
  }

  it should "replace rows for existing not empty block" in {

    val initblock = LeafBlock.empty(TestSchema.testTable)
    val initRows = Seq(testRowFill(10, 15), testRowFill(20, 25), testRowFill(30, 35))

    val block = initblock.put(initRows).head

    val rows = Seq(testRowFill(10, 16), testRowFill(20, 26))

    val r = block.put(rows)

    val c = new Cursor(TestSchema.testTable, r, None)

    c.next() shouldBe true
    c.keyBytes() shouldBe testRowFill(10, 16).key
    c.valueBytes() shouldBe testRowFill(10, 16).value

    c.next() shouldBe true
    c.keyBytes() shouldBe testRowFill(20, 26).key
    c.valueBytes() shouldBe testRowFill(20, 26).value

    c.next() shouldBe true
    c.keyBytes() shouldBe testRowFill(30, 35).key
    c.valueBytes() shouldBe testRowFill(30, 35).value

  }

  it should "put a lot new rows to empty block" in {

    val initblock = LeafBlock.empty(TestSchema.testTable)
    val rows =
      for (i <- 1 to 1000) yield {
        testRowVal(i, i)
      }

    val r = initblock.put(rows)
    val c = new Cursor(TestSchema.testTable, r, None)

    rows.foreach { row =>
      c.next() shouldBe true
      c.keyBytes() shouldBe row.key
      c.valueBytes() shouldBe row.value
    }
  }

  it should "put a lot new rows to existing non empty block" in {

    val initblock = LeafBlock.empty(TestSchema.testTable)
    val initVals = Seq(2, 22)
    val initRows = initVals.map(v => testRowVal(v, v))

    val block = initblock.put(initRows).head

    val vals = 1 to 1000 by 2
    val rows = vals.map(v => testRowVal(v, v))

    val r = block.put(rows)

    val expectedVals = (initVals ++ vals).sorted

    val c = new Cursor(TestSchema.testTable, r, None)

    expectedVals.foreach { v =>
      val expectedRow = testRowVal(v, v)
      c.next() shouldBe true
      c.keyBytes() shouldBe expectedRow.key
      c.valueBytes() shouldBe expectedRow.value
    }
  }

}

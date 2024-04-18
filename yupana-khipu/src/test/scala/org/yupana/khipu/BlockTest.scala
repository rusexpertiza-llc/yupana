package org.yupana.khipu

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.core.TestSchema
import TestUtils._
import org.yupana.khipu.storage.{ KTable, LeafBlock, Row }

class BlockTest extends AnyFlatSpec with Matchers {

  "A Block" should "allocate new block and put rows" in {

    val block = emptyTestBlock

    val row = Row(Array.fill[Byte](block.keySize)(1.toByte), Array.fill[Byte](100)(2.toByte))
    val blocks1 = block.put(Seq(row))

    blocks1 should have size 1
    val actual1 = blocks1.head.readRows()
    actual1 should have size 1
    actual1.head.keyBytes shouldBe row.keyBytes.toSeq
    actual1.head.valueBytes shouldBe row.valueBytes.toSeq

    val expected = Seq(testRowFill(1, 1), testRowFill(2, 2), testRowFill(3, 3))
    val blocks2 = block.put(expected)

    val actual2 = blocks2.head.readRows()

    actual2 should have size 3

    actual2(0).keyBytes shouldBe expected(0).keyBytes
    actual2(0).valueBytes shouldBe expected(0).valueBytes

    actual2(1).keyBytes shouldBe expected(1).keyBytes
    actual2(1).valueBytes shouldBe expected(1).valueBytes

    actual2(2).keyBytes shouldBe expected(2).keyBytes
    actual2(2).valueBytes shouldBe expected(2).valueBytes

  }

  it should "put rows to existing not empty block" in {

    val initblock = emptyTestBlock
    val initRows = Seq(testRowFill(10, 15), testRowFill(20, 25), testRowFill(30, 35))

    val block = initblock.put(initRows).head

    val rows = Seq(testRowFill(1, 6), testRowFill(21, 26), testRowFill(31, 36))

    val blocks = block.put(rows)

    blocks should have size 1

    val actual = blocks.head.readRows()

    actual(0).keyBytes shouldBe testRowFill(1, 6).keyBytes
    actual(0).valueBytes shouldBe testRowFill(1, 6).valueBytes

    actual(1).keyBytes shouldBe testRowFill(10, 15).keyBytes
    actual(1).valueBytes shouldBe testRowFill(10, 15).valueBytes

    actual(2).keyBytes shouldBe testRowFill(20, 25).keyBytes
    actual(2).valueBytes shouldBe testRowFill(20, 25).valueBytes

    actual(3).keyBytes shouldBe testRowFill(21, 26).keyBytes
    actual(3).valueBytes shouldBe testRowFill(21, 26).valueBytes

    actual(4).keyBytes shouldBe testRowFill(30, 35).keyBytes
    actual(4).valueBytes shouldBe testRowFill(30, 35).valueBytes

    actual(5).keyBytes shouldBe testRowFill(31, 36).keyBytes
    actual(5).valueBytes shouldBe testRowFill(31, 36).valueBytes
  }

  it should "replace rows in existing not empty block" in {

    val initblock = emptyTestBlock
    val initRows = Seq(testRowFill(10, 15), testRowFill(20, 25), testRowFill(30, 35))

    val block = initblock.put(initRows).head

    val rows = Seq(testRowFill(10, 16), testRowFill(20, 26))

    val r = block.put(rows)

    r should have size 1
    val actual = r.head.readRows()

    actual(0).keyBytes shouldBe testRowFill(10, 16).keyBytes
    actual(0).valueBytes shouldBe testRowFill(10, 16).valueBytes

    actual(1).keyBytes shouldBe testRowFill(20, 26).keyBytes
    actual(1).valueBytes shouldBe testRowFill(20, 26).valueBytes

    actual(2).keyBytes shouldBe testRowFill(30, 35).keyBytes
    actual(2).valueBytes shouldBe testRowFill(30, 35).valueBytes

  }

  it should "put a lot new rows to empty block" in {

    val initblock = emptyTestBlock
    val rows =
      for (i <- 1 to 1000) yield {
        testRowVal(i, i)
      }

    val r = initblock.put(rows)
    val actual = r.flatMap(_.readRows())

    actual should have size 1000
    rows.zipWithIndex.foreach {
      case (row, i) =>
        actual(i).keyBytes shouldBe row.keyBytes
        actual(i).valueBytes shouldBe row.valueBytes
    }
  }

  it should "put a lot new rows to existing non empty block" in {

    val initblock = emptyTestBlock
    val initVals = Seq(2, 22)
    val initRows = initVals.map(v => testRowVal(v, v))

    val block = initblock.put(initRows).head

    val vals = 1 to 1000 by 2
    val rows = vals.map(v => testRowVal(v, v))

    val r = block.put(rows)

    val expectedVals = (initVals ++ vals).sorted

    val actual = r.flatMap(_.readRows())

    expectedVals.zipWithIndex.foreach {
      case (v, i) =>
        val expectedRow = testRowVal(v, v)
        actual(i).keyBytes shouldBe expectedRow.keyBytes
        actual(i).valueBytes shouldBe expectedRow.valueBytes
    }
  }

  private def emptyTestBlock = {
    val table = KTable.allocateHeap(TestSchema.testTable)
    val id = table.allocateBlock
    LeafBlock.initEmptyBlock(id, table.blockSegment(id), TestSchema.testTable)
    new LeafBlock(id, table)
  }

}

package org.yupana.khipu

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import TestUtils._
import org.yupana.core.TestSchema
import org.yupana.khipu.storage.{ Block, KTable }

import scala.util.Random

class TableTest extends AnyFlatSpec with Matchers {

  "A Table" should "put row to empty table" in {

    val table = KTable.allocateHeap(TestSchema.testTable)

    val row = testRowVal(1, 1)
    table.put(row)

    val c = table.scan()

    c.next() shouldBe true
    c.keyBytes() shouldBe row.keyBytes
    c.valueBytes() shouldBe row.valueBytes
  }

  it should "put row to non empty table" in {

    val table = KTable.allocateHeap(TestSchema.testTable)

    val row1 = testRowVal(1, 1)
    table.put(row1)

    val row2 = testRowVal(2, 2)
    table.put(row2)

    val c = table.scan()

    c.next() shouldBe true
    c.keyBytes() shouldBe row1.keyBytes
    c.valueBytes() shouldBe row1.valueBytes

    c.next() shouldBe true
    c.keyBytes() shouldBe row2.keyBytes
    c.valueBytes() shouldBe row2.valueBytes

    c.next() shouldBe false

  }

  it should "put a lot of rows" in {

    val table = KTable.allocateHeap(TestSchema.testTable)

    val rows = (1 to 1000).map(i => testRowVal(i, i))
    table.put(rows)

    val c = table.scan()

    rows.foreach { r =>
      c.next() shouldBe true
      c.keyBytes() shouldBe r.keyBytes
      c.valueBytes() shouldBe r.valueBytes
    }
  }

  it should "perform 1000 puts of 1000 rows" in {

    val table = KTable.allocateHeap(TestSchema.testTable)

    val rows = (1 to 1000 * 1000).map(i => testRowVal(i, i))
    val shuffled = Random.shuffle(rows)

    val grouped = shuffled.grouped(50000)

    grouped.foreach { group =>
      table.put(group)
    }

    val c = table.scan()
    rows.foreach { r =>
      c.next() shouldBe true
      c.keyBytes() shouldBe r.keyBytes
      c.valueBytes() shouldBe r.valueBytes
    }

    val rowsSize = rows.foldLeft(0)((a, r) => a + r.keyBytes.length + r.valueBytes.length)
    val leafBlocks = table.leafBlocks.size
    val nodeBlocks = table.nodeBlocks.size

    val totalSize = (nodeBlocks + leafBlocks) * Block.BLOCK_SIZE

    println(s"Leaf blocks: ${leafBlocks} == ${leafBlocks * Block.BLOCK_SIZE} bytes")
    println(s"Node blocks: ${nodeBlocks} == ${nodeBlocks * Block.BLOCK_SIZE} bytes")
    println(s"Total blocks: ${nodeBlocks + leafBlocks} == ${totalSize} bytes")

    println(s"Rows: ${rows.size} == $rowsSize bytes")

    println(s"Overhead: ${totalSize - rowsSize} bytes (${totalSize.toLong * 100 / rowsSize - 100}%)")
    KhipuMetricCollector.logStat()

    println(table.explain())

  }
}

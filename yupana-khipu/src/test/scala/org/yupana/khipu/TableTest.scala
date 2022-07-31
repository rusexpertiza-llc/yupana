package org.yupana.khipu

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import TestUtils._
import org.yupana.core.TestSchema

import scala.util.Random

class TableTest extends AnyFlatSpec with Matchers {

  "A Table" should "put row to empty table" in {

    val table = KTable.allocateHeap(TestSchema.testTable)

    val row = testRowVal(1, 1)
    table.put(row)

    val c = table.scan()

    c.next() shouldBe true
    c.keyBytes() shouldBe row.key
    c.valueBytes() shouldBe row.value
  }

  it should "put row to non empty table" in {

    val table = KTable.allocateHeap(TestSchema.testTable)

    val row1 = testRowVal(1, 1)
    table.put(row1)

    val row2 = testRowVal(2, 2)
    table.put(row2)

    val c = table.scan()

    c.next() shouldBe true
    c.keyBytes() shouldBe row1.key
    c.valueBytes() shouldBe row1.value

    c.next() shouldBe true
    c.keyBytes() shouldBe row2.key
    c.valueBytes() shouldBe row2.value

    c.next() shouldBe false

  }

  it should "put a lot of rows" in {

    val table = KTable.allocateHeap(TestSchema.testTable)

    val rows = (1 to 1000).map(i => testRowVal(i, i))
    table.put(rows)

    val blocks = table.getLeafBlocks()
    println(blocks.size)

    val c = table.scan()

    rows.foreach { r =>
      c.next() shouldBe true
      c.keyBytes() shouldBe r.key
      c.valueBytes() shouldBe r.value
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
      c.keyBytes() shouldBe r.key
      c.valueBytes() shouldBe r.value
    }

    val rowsSize = rows.foldLeft(0)((a, r) => a + r.key.size + r.value.size)
    val leafBlocks = table.getLeafBlocks().size
    val nodeBlocks = table.getNodeBlocks().size

    val totalSize = (nodeBlocks + leafBlocks) * Block.BLOCK_SIZE

    println(s"Leaf blocks: ${leafBlocks} == ${leafBlocks * Block.BLOCK_SIZE} bytes")
    println(s"Node blocks: ${nodeBlocks} == ${nodeBlocks * Block.BLOCK_SIZE} bytes")
    println(s"Total blocks: ${nodeBlocks + leafBlocks} == ${totalSize} bytes")

    println(s"Rows: ${rows.size} == $rowsSize bytes")

    println(s"Overhead: ${totalSize - rowsSize} bytes (${totalSize.toLong * 100 / rowsSize}%)")
    KhipuMetricCollector.logStat()

    table.explain()
  }

}

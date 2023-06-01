package org.yupana.khipu

import jdk.incubator.foreign.MemorySegment
import org.yupana.api.schema.Table
import org.yupana.api.utils.SortedSetIterator
import StorageFormat.byteArrayDimOrdering

class Cursor(table: Table, blocks: Iterator[LeafBlock], prefixes: SortedSetIterator[Array[Byte]]) {

  private val keySize = StorageFormat.keySize(table)

  private var prefixBuf: MemorySegment = _
  private var currentBlock = Option.empty[LeafBlock]
  private var currentSegment: MemorySegment = _

  private var pos = -1
  private var offset = 0
  private var rowSize = 0

  nextPrefix

  def keyBytes(): Array[Byte] = {
    StorageFormat.getBytes(currentSegment, offset + 4, keySize)
  }

  def valueBytes(): Array[Byte] = {
    StorageFormat.getBytes(currentSegment, offset + 4 + keySize, rowSize - keySize - 4)
  }

  def row(): MemorySegment = {
    currentSegment
  }

  def next(): Boolean = {

    var fl = nextRow
    var c = 0
    do {
      c = StorageFormat.compareTo(prefixBuf, currentSegment, keySize)
      fl = if (c < 0) nextPrefix else if (c > 0) nextRow else fl
    } while (c != 0 && fl)
    fl
  }

  private def nextRow: Boolean = {
    currentBlock match {
      case Some(bl) =>
        if (pos < bl.numOfRecords - 1) {
          pos += 1
          offset += rowSize
          rowSize = readRowSize()
          true
        } else {
          nextBlock()
        }
      case None =>
        first()
    }
  }

  private def readRowSize() = {
    StorageFormat.getInt(currentSegment, offset)
  }

//  private def matchKey(): Boolean = {
//    prefixBuf.byteSize() == 0 || StorageFormat.startsWith(currentSegment, prefixBuf, keySize)
//  }

  private def first(): Boolean = {
    if (blocks.nonEmpty) {
      nextBlock()
      true
    } else {
      false
    }
  }

  private def nextBlock(): Boolean = {
    if (blocks.nonEmpty) {
      val block = blocks.next()
      currentBlock = Some(block)
      currentSegment = block.payload
      pos = 0
      offset = 0
      rowSize = readRowSize()
      true
    } else {
      false
    }
  }

  private def nextPrefix: Boolean = {
    val fl = prefixes.hasNext
    if (fl) {
      prefixBuf = MemorySegment.ofArray(prefixes.next())
    }
    fl
  }
}

object Cursor {
  def apply(table: Table, blocks: Iterator[LeafBlock]): Cursor = new Cursor(table, blocks, SortedSetIterator.empty)
}

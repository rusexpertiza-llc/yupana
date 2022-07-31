package org.yupana.khipu

import jdk.incubator.foreign.{ MemoryAccess, MemorySegment }
import org.yupana.api.schema.Table

import java.nio.ByteOrder

class Cursor(table: Table, blocks: Seq[LeafBlock], prefix: Option[Array[Byte]]) {

  private val keySize = StorageFormat.keySize(table)
  private val prefixBuf = MemorySegment.ofArray(prefix.getOrElse(Array.empty[Byte]))

  private var tailBlocks = Seq.empty[LeafBlock]
  private var currentBlock = Option.empty[LeafBlock]
  private var currentSegment: MemorySegment = _

  private var pos = -1
  private var offset = 0
  private var rowSize = 0

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
    var fl = false
    do {
      fl = nextLoop
    } while (!matchKey())
    fl
  }

  private def nextLoop: Boolean = {
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

  private def matchKey(): Boolean = {
    prefixBuf.byteSize() == 0 || StorageFormat.startsWith(currentSegment, prefixBuf, keySize)
  }

  private def first(): Boolean = {
    if (blocks.nonEmpty) {
      tailBlocks = blocks
      nextBlock()
      true
    } else {
      false
    }
  }

  private def nextBlock(): Boolean = {
    tailBlocks.headOption match {
      case blockOpt @ Some(block) =>
        currentBlock = blockOpt
        tailBlocks = tailBlocks.tail
        currentSegment = block.payload
        pos = 0
        offset = 0
        rowSize = readRowSize()
        true
      case None =>
        false
    }
  }
}

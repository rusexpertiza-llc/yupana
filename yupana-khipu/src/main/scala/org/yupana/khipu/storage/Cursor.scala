/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yupana.khipu.storage

import org.yupana.api.utils.SortedSetIterator

import java.lang.foreign.MemorySegment
import scala.annotation.tailrec
import scala.collection.mutable

class Cursor(table: KTable, bTree: BTree, prefixes: SortedSetIterator[Prefix]) {

  private val keySize = table.keySize

  private val blockChildrenStack: mutable.Stack[Iterator[NodeBlock.Child]] = mutable.Stack(bTree.root.children)

  private var currentPrefix: Prefix = _
  private var currentBlock = Option.empty[LeafBlock]
  private var currentSegment: MemorySegment = _

  private var pos = -1
  private var offset = 0
  private var rowSize = 0

  nextPrefix

  def keyBytes(): Array[Byte] = {
    StorageFormat.getBytes(currentSegment, offset + 8, keySize)
  }

  def key(): MemorySegment = {
    StorageFormat.getSegment(currentSegment, offset + 8, keySize)
  }

  def valueBytes(): Array[Byte] = {
    StorageFormat.getBytes(currentSegment, offset + 8 + keySize, rowSize - keySize - 8)
  }

  def value(): MemorySegment = {
    StorageFormat.getSegment(currentSegment, offset + 8 + keySize, rowSize - keySize - 8)
  }

  def row(): MemorySegment = {
    StorageFormat.getSegment(currentSegment, offset + 8, rowSize - 8)
  }

  def next(): Boolean = {

    var fl = nextRow
    var c = 0
    do {
      val s = if (currentPrefix != null) math.min(keySize, currentPrefix.size).toInt else keySize
      c =
        if (currentPrefix != null) StorageFormat.compareTo(currentPrefix.segment, 0, currentSegment, offset + 8, s)
        else 0
      fl = if (c < 0) nextPrefix else if (c > 0) nextRow else fl
    } while (c != 0 && fl)
    fl
  }

  private def nextRow: Boolean = {
    currentBlock match {
      case Some(bl) =>
        if (pos < bl.numOfRecords - 1) {
          pos += 1
          offset += StorageFormat.alignLong(rowSize)
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

  private def first(): Boolean = {
    if (blockChildrenStack.nonEmpty) {
      nextBlock()
    } else {
      false
    }
  }

  private def nextBlock(): Boolean = {

    @tailrec
    def loop(): Boolean = {
      if (blockChildrenStack.nonEmpty) {
        val children = blockChildrenStack.top
        if (children.hasNext) {
          val child = children.next()
          val c = compare(currentPrefix, child)

          if (c == 0) {
            table.block(child.id) match {
              case leaf: LeafBlock =>
                currentBlock = Some(leaf)
                currentSegment = leaf.payload
                pos = 0
                offset = 0
                rowSize = readRowSize()
                leaf.numOfRecords > 0
              case node: NodeBlock =>
                blockChildrenStack.push(node.children)
                loop()
              case x =>
                throw new IllegalStateException(s"Unexpected type of block ${x.getClass.getName}")
            }
          } else if (c > 0) {
            if (nextPrefix) loop() else false
          } else {
            loop()
          }
        } else {
          blockChildrenStack.pop()
          loop()
        }
      } else {
        false
      }
    }

    loop()
  }

  private def compare(prefix: Prefix, child: NodeBlock.Child): Int = {
    val s = if (prefix != null) math.min(keySize, prefix.size.toInt) else keySize

    val l = if (prefix != null) StorageFormat.compareTo(prefix.segment, child.startKey, s) else 0
    val r = if (prefix != null) StorageFormat.compareTo(prefix.segment, child.endKey, s) else 0

    if (l >= 0 && r <= 0) {
      0
    } else if (r > 0) {
      -1
    } else {
      1
    }
  }

  private def nextPrefix: Boolean = {
    val fl = prefixes.hasNext
    if (fl) {
      currentPrefix = prefixes.next()
    }
    fl
  }
}

object Cursor {
  def apply(table: KTable, bTree: BTree): Cursor = new Cursor(table, bTree, SortedSetIterator.empty)
}

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

import org.yupana.khipu.storage.Block._

trait Block {

  val kind: Byte = checkFormatAndGetBlockKind(id, table)
  val segment: MemorySegment = table.blockSegment(id)

  val keySize: Int = StorageFormat.getInt(segment, KEY_SIZE_OFFSET)
  require(keySize == table.keySize)

  val startKey: MemorySegment = StorageFormat.getSegment(segment, START_KEY_OFFSET, keySize)
  val endKey: MemorySegment = StorageFormat.getSegment(segment, endKeyOffset(keySize), keySize)

  val numOfRecords: Int = StorageFormat.getInt(segment, numOfRecordsOffset(keySize))
  val rowsDataSize: Int = StorageFormat.getInt(segment, rowsDataSizeOffset(keySize))

  def id: Int
  def table: KTable
  def put(rows: Seq[Row]): List[Block]
}

object Block {

  val MAGIC = 22859.toShort // StorageFormat.getShort(MemorySegment.ofArray("YK".getBytes(StandardCharsets.UTF_8)), 0)
  val BLOCK_SIZE = 65536

  val LEAF_KIND: Byte = 1
  val NODE_KIND: Byte = 2

  val BLOCK_ID_SIZE = 4
  val MAGIC_SIZE = 2
  val KIND_SIZE = 2
  val ID_SIZE = 4
  val KEY_SIZE_SIZE = 4

  val MAGIC_OFFSET = 0
  val KIND_OFFSET = 2
  val ID_OFFSET = 4
  val KEY_SIZE_OFFSET = 8
  val START_KEY_OFFSET = 16

  def endKeyOffset(keySize: Int): Int = {
    START_KEY_OFFSET + StorageFormat.alignLong(keySize)
  }

  def numOfRecordsOffset(keySize: Int): Int = endKeyOffset(keySize) + StorageFormat.alignInt(keySize)
  def rowsDataSizeOffset(keySize: Int): Int = numOfRecordsOffset(keySize) + 4
  def headerSize(keySize: Int): Int = StorageFormat.alignLong(rowsDataSizeOffset(keySize) + 4)

  def writeHeader(
      segment: MemorySegment,
      id: Int,
      kind: Byte,
      keySize: Int,
      startKey: MemorySegment,
      endKey: MemorySegment,
      numOfRecords: Int,
      payloadSize: Int
  ): Unit = {
    StorageFormat.setShort(Block.MAGIC, segment, MAGIC_OFFSET)
    StorageFormat.setByte(kind, segment, KIND_OFFSET)
    StorageFormat.setInt(id, segment, ID_OFFSET)
    StorageFormat.setInt(keySize, segment, KEY_SIZE_OFFSET)

    StorageFormat.copy(startKey, segment, START_KEY_OFFSET)
    StorageFormat.copy(endKey, segment, endKeyOffset(keySize))

    StorageFormat.setInt(numOfRecords, segment, numOfRecordsOffset(keySize))
    StorageFormat.setInt(payloadSize, segment, rowsDataSizeOffset(keySize))
  }

  def checkFormatAndGetBlockKind(id: Int, table: KTable): Byte = {
    val segment = table.blockSegment(id)

    val magic: Short = StorageFormat.getShort(segment, MAGIC_OFFSET)
    require(magic == Block.MAGIC, s"Wrong block format, bad magic word $magic, required ${Block.MAGIC} ")

    {
      val storedId = StorageFormat.getInt(segment, ID_OFFSET)
      require(id == storedId, s"Wrong block, bad stored id $id, required $id")
    }

    val kind: Byte = StorageFormat.getByte(segment, KIND_OFFSET)
    require(
      kind == LEAF_KIND || kind == NODE_KIND,
      s"Wrong block format, block kind magic $kind, required $LEAF_KIND or $NODE_KIND "
    )
    kind
  }
}

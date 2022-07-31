package org.yupana.khipu

import jdk.incubator.foreign.MemorySegment
import Block._

trait Block {

  val kind: Byte = checkFormatAndGetBlockKind(id, table)
  val segment: MemorySegment = table.blockSegment(id)

  val keySize: Int = StorageFormat.getInt(segment, KEY_SIZE_OFFSET)
  require(keySize == table.keySize)

  val startKey: Array[Byte] = Array.ofDim[Byte](keySize)
  StorageFormat.getBytes(segment, START_KEY_OFFSET, startKey, keySize)

  val endKey: Array[Byte] = Array.ofDim[Byte](keySize)
  StorageFormat.getBytes(segment, endKeyOffset(keySize), endKey, keySize)

  val numOfRecords: Int = StorageFormat.getInt(segment, numOfRecordsOffset(keySize))
  val rowsDataSize: Int = StorageFormat.getInt(segment, rowsDataSizeOffset(keySize))

  def id: Int
  def table: KTable
  def put(rows: Seq[Row]): List[Block]

}

object Block {

  val MAGIC = 22859.toShort // StorageFormat.getShort(MemorySegment.ofArray("YK".getBytes(StandardCharsets.UTF_8)), 0)
  val BLOCK_SIZE = 4096

  val LEAF_KIND: Byte = 1
  val NODE_KIND: Byte = 2

  val BLOCK_ID_SIZE = 4
  val MAGIC_SIZE = 2
  val KIND_SIZE = 2
  val ID_SIZE = 4
  val KEY_SIZE_SIZE = 4

  val MAGIC_OFFSET = 0
  val ID_OFFSET = MAGIC_SIZE
  val KIND_OFFSET = ID_OFFSET + ID_SIZE
  val KEY_SIZE_OFFSET = KIND_OFFSET + KIND_SIZE
  val START_KEY_OFFSET = KEY_SIZE_OFFSET + KEY_SIZE_SIZE

  def endKeyOffset(keySize: Int): Int = START_KEY_OFFSET + keySize
  def numOfRecordsOffset(keySize: Int): Int = endKeyOffset(keySize) + keySize
  def rowsDataSizeOffset(keySize: Int): Int = numOfRecordsOffset(keySize) + 4
  def headerSize(keySize: Int): Int = rowsDataSizeOffset(keySize) + 4

  def writeHeader(
      segment: MemorySegment,
      id: Int,
      kind: Byte,
      keySize: Int,
      startKey: Array[Byte],
      endKey: Array[Byte],
      numOfRecords: Int,
      payloadSize: Int
  ): Unit = {
    StorageFormat.setShort(Block.MAGIC, segment, MAGIC_OFFSET)
    StorageFormat.setInt(id, segment, ID_OFFSET)
    StorageFormat.setByte(kind, segment, KIND_OFFSET)
    StorageFormat.setInt(keySize, segment, KEY_SIZE_OFFSET)

    StorageFormat.setBytes(startKey, 0, segment, START_KEY_OFFSET, keySize)

    StorageFormat.setBytes(endKey, 0, segment, endKeyOffset(keySize), keySize)

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

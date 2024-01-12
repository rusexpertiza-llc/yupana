package org.yupana.khipu.storage

import java.lang.foreign.MemorySegment

case class Row(key: MemorySegment, value: MemorySegment) {
  def keyBytes: Array[Byte] = {
    StorageFormat.getBytes(key, 0, key.byteSize().toInt)
  }

  def valueBytes: Array[Byte] = {
    StorageFormat.getBytes(value, 0, value.byteSize().toInt)
  }
}

object Row {
  def apply(key: Array[Byte], value: Array[Byte]): Row = {

    val keySegment = StorageFormat.fromBytes(key)
    val valueSegment = StorageFormat.fromBytes(value)

    Row(keySegment, valueSegment)
  }
}

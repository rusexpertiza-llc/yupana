package org.yupana.khipu

import org.yupana.core.TestSchema
import org.yupana.khipu.storage.{ Row, StorageFormat }

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer

object TestUtils {

  def testRowFill(k: Int, v: Int) = {
    val keySize = StorageFormat.keySize(TestSchema.testTable)
    Row(
      Array.fill[Byte](keySize)(k.toByte),
      Array.fill[Byte](100)(v.toByte)
    )
  }

  def testRowVal(k: Int, v: Int) = {
    val keySize = StorageFormat.keySize(TestSchema.testTable)
    val keyBuf = ByteBuffer.wrap(Array.ofDim[Byte](keySize))
    keyBuf.putInt(k)
    val key = keyBuf.array()

    val valBuf = ByteBuffer.wrap(Array.ofDim[Byte](20))
    valBuf.putInt(v)
    val value = valBuf.array()
    Row(key, value)
  }

  def testRowStr(k: String, v: String): Row = {
    val keySize = StorageFormat.keySize(TestSchema.testTable)
    val keyBuf = ByteBuffer.wrap(Array.ofDim[Byte](keySize))
    keyBuf.put(k.getBytes(StandardCharsets.UTF_8))
    val key = keyBuf.array()

    Row(key, v.getBytes(StandardCharsets.UTF_8))
  }

}

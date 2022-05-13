package org.yupana.khipu

import org.yupana.core.TestSchema

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

    val valBuf = ByteBuffer.wrap(Array.ofDim[Byte](100))
    valBuf.putInt(v)
    val value = valBuf.array()
    Row(key, value)
  }

}

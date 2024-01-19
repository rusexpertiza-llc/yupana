package org.yupana.readerwriter

object Memory {
  def convertEndian(v: Int): Int = {
    java.lang.Integer.reverseBytes(v)
  }

  def convertEndian(v: Long): Long = {
    java.lang.Long.reverseBytes(v)
  }

  def convertEndian(v: Short): Short = {
    java.lang.Short.reverseBytes(v)
  }
}

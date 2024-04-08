package org.yupana.core.model

import jdk.internal.vm.annotation.ForceInline

object BitSetOps {

  @ForceInline
  def check(bitset: Array[Long], bitNum: Int): Boolean = {
    val index = bitNum / 64
    val bit = bitNum % 64
    val l = bitset(index)
    (l & (1L << bit)) != 0
  }

  @ForceInline
  def set(bitset: Array[Long], bitNum: Int): Unit = {
    val index = bitNum / 64
    val bit = bitNum % 64
    bitset(index) = bitset(index) | (1L << bit)
  }

  @ForceInline
  def clear(bitset: Array[Long], bitNum: Int): Unit = {
    val index = bitNum / 64
    val bit = bitNum % 64
    val old = bitset(index)
    val mask = 1L << bit
    if ((old & mask) != 0) {
      bitset(index) = old ^ mask
    }
  }
}

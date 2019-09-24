package org.yupana.hbase

import scala.util.hashing.MurmurHash3

case class TSDRowKey[T](
  baseTime: Long,
  dimIds: Array[Option[T]]
) {

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case that: TSDRowKey[T] => this.baseTime == that.baseTime && (this.dimIds sameElements that.dimIds)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    val h = MurmurHash3.arrayHash(dimIds)
    val h2 = MurmurHash3.mix(h, baseTime.##)
    MurmurHash3.finalizeHash(h2, dimIds.length + 1)
  }
}

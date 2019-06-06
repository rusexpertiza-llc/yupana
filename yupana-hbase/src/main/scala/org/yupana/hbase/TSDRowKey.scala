package org.yupana.hbase

import scala.util.hashing.MurmurHash3

case class TSDRowKey[T](
  baseTime: Long,
  tagsIds: Array[Option[T]]
) {

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case that: TSDRowKey[T] => this.baseTime == that.baseTime && (this.tagsIds sameElements that.tagsIds)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    val h = MurmurHash3.arrayHash(tagsIds)
    val h2 = MurmurHash3.mix(h, baseTime.##)
    MurmurHash3.finalizeHash(h2, tagsIds.length + 1)
  }
}

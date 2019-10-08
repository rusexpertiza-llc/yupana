package org.yupana.hbase

case class HBaseId(id: Long)

object HBaseId {
  implicit val ordering: Ordering[HBaseId] = new Ordering[HBaseId] {
    override def compare(x: HBaseId, y: HBaseId): Int = java.lang.Long.compareUnsigned(x.id, y.id)
  }
}

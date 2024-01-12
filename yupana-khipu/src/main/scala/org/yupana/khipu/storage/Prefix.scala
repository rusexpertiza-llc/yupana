package org.yupana.khipu.storage

import org.yupana.api.utils.DimOrdering

import java.lang.foreign.MemorySegment
import java.nio.charset.StandardCharsets
import java.util.Comparator

case class Prefix(segment: MemorySegment) {
  def size: Int = segment.byteSize().toInt
}

object Prefix {

  final val prefixComparator: Comparator[Prefix] = new Comparator[Prefix] {
    override def compare(o1: Prefix, o2: Prefix): Int = {
      StorageFormat.compareTo(o1.segment, o2.segment, math.max(o1.size, o2.size))
    }
  }

  implicit val prefixOrdering: DimOrdering[Prefix] =
    DimOrdering.fromCmp(prefixComparator.compare)

  def apply(baseTime: Long, dims: Seq[Array[Byte]]): Prefix = {
    val size = 8 + dims.foldLeft(0)(_ + _.length)
    val seg = StorageFormat.allocateHeap(size)
    StorageFormat.setLong(baseTime, seg, 0)
    var offset = 8
    dims.foreach { v =>
      StorageFormat.setBytes(v, 0, seg, offset, v.length)
      offset += v.length
    }
    Prefix(seg)
  }

  def apply(str: String): Prefix = {
    val seg = StorageFormat.allocateHeap(str.length)
    StorageFormat.setBytes(str.getBytes(StandardCharsets.UTF_8), 0, seg, 0, str.length)
    Prefix(seg)
  }
}

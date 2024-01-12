/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

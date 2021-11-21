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

package org.yupana.api.utils

import scala.collection.mutable.ListBuffer

/**
  * Iterator implementing groupBy operation for Iterators.  You should take in account that since iterator is lazy
  * only sequential elements are groped.  So there might be several groups with the same key.
  *
  * @param f extract key function
  * @param data iterator with data to be grouped
  * @tparam T data type
  * @tparam K key type
  */
class GroupByIterator[T, K](f: T => K, data: Iterator[T]) extends Iterator[(K, Seq[T])] {
  private val b = data.buffered

  override def hasNext: Boolean = b.hasNext

  override def next(): (K, Seq[T]) = {
    if (!hasNext) throw new NoSuchElementException("next on empty iterator")
    val buf = new ListBuffer[T]
    val key = f(b.head)
    buf += b.next()
    while (b.nonEmpty && f(b.head) == key) buf += b.next()
    key -> buf.toList
  }
}

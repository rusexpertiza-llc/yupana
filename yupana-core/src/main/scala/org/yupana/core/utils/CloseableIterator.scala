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

package org.yupana.core.utils

import scala.collection.AbstractIterator

abstract class CloseableIterator[+T](sub: Iterator[T]) extends AbstractIterator[T] with AutoCloseable {

  private var closed = false
  private[this] var iter = sub

  override def hasNext: Boolean = {
    val r = iter.hasNext
    if (!r && !closed) {
      closed = true
      // reassign to release resources of highly resource consuming iterators early
      iter = Iterator.empty
      close()
    }
    r
  }

  override def next(): T = iter.next()

  def close(): Unit
}

object CloseableIterator {
  def apply[A](inner: Iterator[A], closeFunction: => Unit): CloseableIterator[A] = {
    new CloseableIterator[A](inner) {
      def close(): Unit = closeFunction
    }
  }
}

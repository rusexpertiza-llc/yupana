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

trait CloseableIterator[+T] extends Iterator[T] with AutoCloseable

object CloseableIterator {

  private val _empty: CloseableIterator[Nothing] = pure(Iterator.empty[Nothing])

  def pure[A](inner: Iterator[A]): CloseableIterator[A] = new CloseableIterator[A] {
    override def close(): Unit = {}
    override def hasNext: Boolean = inner.hasNext
    override def next(): A = inner.next()
  }

  def apply[A](inner: Iterator[A], closeFunction: => Unit): CloseableIterator[A] = {
    new CloseableIterator[A] {

      private var closed = false
      private var iter = inner

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

      override def next(): A = iter.next()

      def close(): Unit = closeFunction
    }
  }

  @`inline` final def empty[T]: CloseableIterator[T] = _empty
}

package org.yupana.jdbc

package object compat {
  type LazyList[+T] = scala.collection.immutable.Stream[T]
  val LazyList = scala.collection.immutable.Stream
}

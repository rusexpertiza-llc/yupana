package org.yupana.jdbc

package object compat {
  type LazyList[+T] = scala.collection.immutable.LazyList[T]
  val LazyList = scala.collection.immutable.LazyList
}

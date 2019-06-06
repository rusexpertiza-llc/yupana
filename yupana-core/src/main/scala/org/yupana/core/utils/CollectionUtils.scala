package org.yupana.core.utils

import scala.annotation.tailrec

object CollectionUtils {

  def crossJoin[T](list: List[List[T]]): List[List[T]] = {

    @tailrec
    def crossJoin(acc: List[List[T]], rest: List[List[T]]): List[List[T]] = {
      rest match {
        case Nil => acc
        case x :: xs =>
          val next = for {
            i <- x
            j <- acc
          } yield i :: j
          crossJoin(next, xs)
      }
    }

    crossJoin(List(Nil), list.reverse)
  }

  def alignByKey[K, V](keyOrder: Seq[K], values: Seq[V], getKey: V => K): Seq[V] = {
    val orderMap = keyOrder.zipWithIndex.toMap
    values.sortBy { v =>
      val k = getKey(v)
      orderMap.getOrElse(k, throw new IllegalArgumentException(s"Unknown key $k for value $v"))
    }
  }

  def intersectAll[T](sets: Seq[Set[T]]): Set[T] = {
    if (sets.nonEmpty) sets.reduce(_ intersect _) else Set.empty
  }

  def collectErrors[T](ls: Seq[Either[String, T]]): Either[String, Seq[T]] = {
    val errors = ls.collect { case Left(e) => e }

    if (errors.isEmpty) {
      Right(ls.collect { case Right(d) => d })
    } else {
      Left(errors.mkString(". "))
    }
  }
}

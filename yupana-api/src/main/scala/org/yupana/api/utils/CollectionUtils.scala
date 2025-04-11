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

object CollectionUtils {
  def mkStringWithLimit(
      seq: Iterable[_],
      limit: Int = 10,
      start: String = "[",
      sep: String = ", ",
      end: String = "]"
  ): String = {
    val length = seq.size
    if (length <= limit) {
      seq.mkString(start, sep, end)
    } else {
      val firstN = seq.take(limit).mkString(start, sep, "")
      s"$firstN$sep... and ${length - limit} more$end"
    }
  }

  def traverseOpt[T, E, U](x: Option[T])(f: T => Either[E, U]): Either[E, Option[U]] = {
    x match {
      case Some(v) => f(v).map(Some.apply)
      case None    => Right(None)
    }
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

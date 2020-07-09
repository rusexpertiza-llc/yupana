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

package org.yupana.core.operations

import org.joda.time.DateTimeFieldType
import org.yupana.api.Time
import org.yupana.api.types.UnaryOperations
import org.yupana.utils.Tokenizer

import scala.collection.AbstractIterator

trait UnaryOperationsImpl extends UnaryOperations {
  override def unaryMinus[N](n: N)(implicit numeric: Numeric[N]): N = orNull[N, N](n)(numeric.negate)
  override def abs[N](n: N)(implicit numeric: Numeric[N]): N = orNull[N, N](n)(numeric.abs)

  override def isNull[T](t: T): Boolean = t == null
  override def isNotNull[T](t: T): Boolean = t != null

  override def not(x: Boolean): Boolean = !x

  override def extractYear(t: Time): Int = orNull[Time, Int](t)(_.toLocalDateTime.getYear)
  override def extractMonth(t: Time): Int = orNull[Time, Int](t)(_.toLocalDateTime.getMonthOfYear)
  override def extractDay(t: Time): Int = orNull[Time, Int](t)(_.toLocalDateTime.getDayOfMonth)
  override def extractHour(t: Time): Int = orNull[Time, Int](t)(_.toLocalDateTime.getHourOfDay)
  override def extractMinute(t: Time): Int = orNull[Time, Int](t)(_.toLocalDateTime.getMinuteOfHour)
  override def extractSecond(t: Time): Int = orNull[Time, Int](t)(_.toLocalDateTime.getSecondOfMinute)

  override def trunc(fieldType: DateTimeFieldType)(time: Time): Time =
    orNull[Time, Time](time)(truncateTime(_, fieldType))
  override def truncYear(t: Time): Time = orNull[Time, Time](t)(truncateTime(_, DateTimeFieldType.year()))
  override def truncMonth(t: Time): Time = orNull[Time, Time](t)(truncateTime(_, DateTimeFieldType.monthOfYear()))
  override def truncWeek(t: Time): Time = orNull[Time, Time](t)(truncateTime(_, DateTimeFieldType.weekOfWeekyear()))
  override def truncDay(t: Time): Time = orNull[Time, Time](t)(truncateTime(_, DateTimeFieldType.dayOfMonth()))
  override def truncHour(t: Time): Time = orNull[Time, Time](t)(truncateTime(_, DateTimeFieldType.hourOfDay()))
  override def truncMinute(t: Time): Time = orNull[Time, Time](t)(truncateTime(_, DateTimeFieldType.minuteOfHour()))
  override def truncSecond(t: Time): Time = orNull[Time, Time](t)(truncateTime(_, DateTimeFieldType.secondOfDay()))

  override def stringLength(s: String): Int = if (s != null) s.length else 0
  override def lower(s: String): String = orNull[String, String](s)(_.toLowerCase)
  override def upper(s: String): String = orNull[String, String](s)(_.toUpperCase)

  override def tokens(s: String): Array[String] = orNull[String, Array[String]](s)(tokenize)
  override def splitString(s: String): Array[String] =
    orNull[String, Array[String]](s)(splitBy(_, !_.isLetterOrDigit).toArray)

  override def arrayToString[T](a: Array[T]): String = orNull[Array[T], String](a)(_.mkString("(", ", ", ")"))
  override def arrayLength[T](a: Array[T]): Int = if (a != null) a.length else 0
  override def tokenizeArray(a: Array[String]): Array[String] =
    orNull[Array[String], Array[String]](a)(_.flatMap(tokenize))

  private def truncateTime(time: Time, interval: DateTimeFieldType): Time = {
    orNull[Time, Time](time)(t => Time(t.toDateTime.property(interval).roundFloorCopy().getMillis))
  }

  private def orNull[T, O](v: T)(f: T => O): O = {
    if (v != null) f(v) else null.asInstanceOf[O]
  }

  private def tokenize(s: String): Array[String] = Tokenizer.transliteratedTokens(s).toArray

  private def splitBy(s: String, p: Char => Boolean): Iterator[String] = new AbstractIterator[String] {
    private val len = s.length
    private var pos = 0

    override def hasNext: Boolean = pos < len

    override def next(): String = {
      if (pos >= len) throw new NoSuchElementException("next on empty iterator")
      val start = pos
      while (pos < len && !p(s(pos))) pos += 1
      val res = s.substring(start, pos min len)
      while (pos < len && p(s(pos))) pos += 1
      res
    }
  }
}

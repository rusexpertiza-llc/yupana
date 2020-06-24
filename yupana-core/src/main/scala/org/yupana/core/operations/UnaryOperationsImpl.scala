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
  //TODO: check null everywhere
  override def unaryMinus[N](n: N)(implicit numeric: Numeric[N]): N = numeric.negate(n)
  override def abs[N](n: N)(implicit numeric: Numeric[N]): N = numeric.abs(n)

  override def isNull[T](t: T): Boolean = t == null
  override def isNotNull[T](t: T): Boolean = t != null

  override def not(x: Boolean): Boolean = !x

  override def extractYear(t: Time): Int = t.toLocalDateTime.getYear
  override def extractMonth(t: Time): Int = t.toLocalDateTime.getMonthOfYear
  override def extractDay(t: Time): Int = t.toLocalDateTime.getDayOfMonth
  override def extractHour(t: Time): Int = t.toLocalDateTime.getHourOfDay
  override def extractMinute(t: Time): Int = t.toLocalDateTime.getMinuteOfHour
  override def extractSecond(t: Time): Int = t.toLocalDateTime.getSecondOfMinute

  override def trunc(fieldType: DateTimeFieldType)(time: Time): Time = truncateTime(time, fieldType)
  override def truncYear(t: Time): Time = truncateTime(t, DateTimeFieldType.year())
  override def truncMonth(t: Time): Time = truncateTime(t, DateTimeFieldType.monthOfYear())
  override def truncWeek(t: Time): Time = truncateTime(t, DateTimeFieldType.weekOfWeekyear())
  override def truncDay(t: Time): Time = truncateTime(t, DateTimeFieldType.dayOfMonth())
  override def truncHour(t: Time): Time = truncateTime(t, DateTimeFieldType.hourOfDay())
  override def truncMinute(t: Time): Time = truncateTime(t, DateTimeFieldType.minuteOfHour())
  override def truncSecond(t: Time): Time = truncateTime(t, DateTimeFieldType.secondOfDay())

  override def stringLength(s: String): Int = s.length
  override def lower(s: String): String = s.toLowerCase
  override def upper(s: String): String = s.toUpperCase

  override def tokens(s: String): Array[String] = tokenize(s)
  override def splitString(s: String): Array[String] =
    splitBy(s, !_.isLetterOrDigit).toArray

  override def arrayToString[T](a: Array[T]): String = a.mkString("(", ", ", ")")
  override def arrayLength[T](a: Array[T]): Int = a.length
  override def tokenizeArray(a: Array[String]): Array[String] = a.flatMap(tokenize)

  private def truncateTime(time: Time, interval: DateTimeFieldType): Time = {
    Time(time.toDateTime.property(interval).roundFloorCopy().getMillis)
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

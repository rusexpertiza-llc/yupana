package org.yupana.core.operations

import org.joda.time.DateTimeFieldType
import org.yupana.api.Time
import org.yupana.api.types.UnaryOperations
import org.yupana.utils.{ItemsStemmer, Transliterator}

import scala.collection.AbstractIterator

trait UnaryOperationsImpl extends UnaryOperations {
  override def unaryMinus[N](n: Option[N])(implicit numeric: Numeric[N]): Option[N] = n.map(numeric.negate)
  override def abs[N](n: Option[N])(implicit numeric: Numeric[N]): Option[N] = n.map(numeric.abs)

  override def isNull[T](t: Option[T]): Option[Boolean] = Some(t.isEmpty)
  override def isNotNull[T](t: Option[T]): Option[Boolean] = Some(t.isDefined)

  override def not(x: Option[Boolean]): Option[Boolean] = x.map(!_)

  override def extractYear(t: Option[Time]): Option[Int] = t.map(_.toLocalDateTime.getYear)
  override def extractMonth(t: Option[Time]): Option[Int] = t.map(_.toLocalDateTime.getMonthOfYear)
  override def extractDay(t: Option[Time]): Option[Int] = t.map(_.toLocalDateTime.getDayOfMonth)
  override def extractHour(t: Option[Time]): Option[Int] = t.map(_.toLocalDateTime.getHourOfDay)
  override def extractMinute(t: Option[Time]): Option[Int] = t.map(_.toLocalDateTime.getMinuteOfHour)
  override def extractSecond(t: Option[Time]): Option[Int] = t.map(_.toLocalDateTime.getSecondOfMinute)

  override def trunc(fieldType: DateTimeFieldType)(time: Option[Time]): Option[Time] = truncateTime(time, fieldType)
  override def truncYear(t: Option[Time]): Option[Time] = truncateTime(t, DateTimeFieldType.year())
  override def truncMonth(t: Option[Time]): Option[Time] = truncateTime(t, DateTimeFieldType.monthOfYear())
  override def truncWeek(t: Option[Time]): Option[Time] = truncateTime(t, DateTimeFieldType.weekOfWeekyear())
  override def truncDay(t: Option[Time]): Option[Time] = truncateTime(t, DateTimeFieldType.dayOfMonth())
  override def truncHour(t: Option[Time]): Option[Time] = truncateTime(t, DateTimeFieldType.hourOfDay())
  override def truncMinute(t: Option[Time]): Option[Time] = truncateTime(t, DateTimeFieldType.minuteOfHour())
  override def truncSecond(t: Option[Time]): Option[Time] = truncateTime(t, DateTimeFieldType.secondOfDay())

  override def stringLength(s: Option[String]): Option[Int] = s.map(_.length)

  override def stemString(s: Option[String]): Option[Array[String]] = s.map(stem)
  override def splitString(s: Option[String]): Option[Array[String]] = s.map(v => splitBy(v, !_.isLetterOrDigit).toArray)

  override def arrayToString[T](a: Option[Array[T]]): Option[String] = a.map(_.mkString("(", ", ", ")"))
  override def arrayLength[T](a: Option[Array[T]]): Option[Int] = a.map(_.length)
  override def stemArray(a: Option[Array[String]]): Option[Array[String]] = a.map(_.flatMap(stem))

  private def truncateTime(time: Option[Time], interval: DateTimeFieldType): Option[Time] = {
    time.map(t => Time(t.toDateTime.property(interval).roundFloorCopy().getMillis))
  }

  private def stem(s: String): Array[String] = ItemsStemmer.words(s).map(Transliterator.transliterate).toArray

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

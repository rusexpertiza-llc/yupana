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

package org.yupana.postgres.protocol

import org.threeten.extra.PeriodDuration
import org.yupana.api.types.StringReaderWriter
import org.yupana.api.{ Blob, Time }

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object PostgresStringReaderWriter extends StringReaderWriter {

  private val dtf = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss[.SSS][x]")

  override def readBoolean(s: String): Boolean =
    if (s == "t") true else if (s == "f") false else throw new IllegalArgumentException(s"Invalid boolean value $s")
  override def writeBoolean(v: Boolean): String = if (v) "t" else "f"

  override def readDouble(s: String): Double = s.toDouble
  override def writeDouble(v: Double): String = v.toString

  override def readDecimal(s: String): BigDecimal = BigDecimal(s)
  override def writeDecimal(v: BigDecimal): String = v.toString()

  override def readByte(s: String): Byte = s.toByte
  override def writeByte(v: Byte): String = v.toString

  override def readShort(s: String): Short = s.toShort
  override def writeShort(v: Short): String = v.toString

  override def readInt(s: String): Int = s.toInt
  override def writeInt(v: Int): String = v.toString

  override def readLong(s: String): Long = s.toLong
  override def writeLong(v: Long): String = v.toString

  override def readString(s: String): String = s
  override def writeString(v: String): String = v

  override def readTime(s: String): Time = Time(LocalDateTime.parse(s, dtf))
  override def writeTime(v: Time): String = v.toLocalDateTime.format(dtf)

  override def readPeriodDuration(s: String): PeriodDuration = PeriodDuration.parse(s)
  override def writePeriodDuration(v: PeriodDuration): String = v.toString

  override def readBlob(s: String): Blob = ???

  override def writeBlob(v: Blob): String = ???

  override def readSeq[T](s: String): Seq[T] = ???

  override def writeSeq[T](seq: Seq[T]): String = ???
}

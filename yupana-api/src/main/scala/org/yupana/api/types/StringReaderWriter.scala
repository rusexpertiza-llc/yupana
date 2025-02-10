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

package org.yupana.api.types

import org.threeten.extra.PeriodDuration
import org.yupana.api.{ Blob, Currency, Time }

trait StringReaderWriter {
  def readBoolean(s: String): Boolean
  def writeBoolean(v: Boolean): String

  def readDouble(s: String): Double
  def writeDouble(v: Double): String

  def readDecimal(s: String): BigDecimal
  def writeDecimal(v: BigDecimal): String

  def readByte(s: String): Byte
  def writeByte(v: Byte): String

  def readShort(s: String): Short
  def writeShort(v: Short): String

  def readInt(s: String): Int
  def writeInt(v: Int): String

  def readLong(s: String): Long
  def writeLong(v: Long): String

  def readString(s: String): String
  def writeString(v: String): String

  def readTime(s: String): Time
  def writeTime(v: Time): String

  def readCurrency(s: String): Currency
  def writeCurrency(v: Currency): String

  def readPeriodDuration(s: String): PeriodDuration
  def writePeriodDuration(v: PeriodDuration): String

  def readBlob(s: String): Blob
  def writeBlob(v: Blob): String

  def readSeq[T](s: String): Seq[T]
  def writeSeq[T](seq: Seq[T]): String

}

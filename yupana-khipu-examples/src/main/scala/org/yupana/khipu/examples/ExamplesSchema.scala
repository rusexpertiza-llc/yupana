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

package org.yupana.khipu.examples

import org.yupana.api.schema.{ Metric, RawDimension, Schema, Table }
import org.yupana.utils.{ OfdItemFixer, RussianTokenizer, RussianTransliterator }

import java.time.{ LocalDateTime, ZoneOffset }

object ExampleSchema {

  val epochTime: Long = LocalDateTime.of(2016, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli

  val CAT = RawDimension[Long]("cat")
  val BRAND = RawDimension[Int]("brand")
  val DEVICE = RawDimension[Int]("device")
  val OPERATION = RawDimension[Byte]("operation")
  val POS = RawDimension[Byte]("pos")
  val QUANTITY = Metric[Double]("quantity", 1)
  val SUM = Metric[Long]("sum", 2)

  val salesTable = new Table(
    name = "sales",
    rowTimeSpan = 86400000L * 30L,
    dimensionSeq = Seq(CAT, BRAND, DEVICE, OPERATION, POS),
    metrics = Seq(QUANTITY, SUM),
    externalLinks = Seq.empty,
    epochTime = epochTime
  )

  val tables = Seq(salesTable)
  def schema: Schema = Schema(tables, Seq.empty, OfdItemFixer, RussianTokenizer, RussianTransliterator)
}

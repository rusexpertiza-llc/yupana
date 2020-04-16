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

package org.yupana.examples.spark.etl

import org.joda.time.LocalDateTime

case class Receipt(
    date: LocalDateTime,
    kkmId: Int,
    receiptNumber: Int,
    operationType: Int,
    operator: String,
    shiftNumber: Int,
    items: Seq[Item],
    totalSum: BigDecimal,
    totalCardSum: Option[BigDecimal],
    totalCashSum: Option[BigDecimal],
    taxes: Map[String, BigDecimal],
    prePayment: Option[BigDecimal] = None,
    postPayment: Option[BigDecimal] = None,
    counterSubmission: Option[BigDecimal] = None
)

case class Item(
    name: String,
    sum: BigDecimal,
    quantity: Double,
    measure: Option[String],
    taxes: Map[String, BigDecimal],
    calcTypeSign: Option[Int] = None,
    calcSubjSing: Option[Int] = None,
    nomenclatureType: Option[String] = None,
    gtin: Option[String] = None
)

object Tax {
  val tax00000 = "TAX_00000"
  val tax09091 = "TAX_09091"
  val tax10000 = "TAX_10000"
  val tax15255 = "TAX_15225"
  val tax16667 = "TAX_16667"
  val tax18000 = "TAX_18000"
  val tax20000 = "TAX_20000"
  val taxNo = "TAX_NO"
}

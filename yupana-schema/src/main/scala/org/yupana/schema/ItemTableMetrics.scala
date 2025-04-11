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

package org.yupana.schema

import org.yupana.api.{ Blob, Currency, Time }
import org.yupana.api.schema.{ Metric, QueryFieldToDimension, QueryFieldToMetric }

trait ItemTableMetrics {

  import Metric.Groups._

  val quantityField: Metric.Aux[Double] = Metric[Double]("quantity", 2)
  val sumField: Metric.Aux[Currency] = Metric[Currency]("sum", 3)
  val tax00000Field: Metric.Aux[Currency] = Metric[Currency]("tax00000", 7, rarelyQueried)
  val tax09091Field: Metric.Aux[Currency] = Metric[Currency]("tax09091", 8, rarelyQueried)
  val tax10000Field: Metric.Aux[Currency] = Metric[Currency]("tax10000", 9, rarelyQueried)
  val tax15255Field: Metric.Aux[Currency] = Metric[Currency]("tax15255", 10, rarelyQueried)
  val tax18000Field: Metric.Aux[Currency] = Metric[Currency]("tax18000", 11, rarelyQueried)
  val taxNoField: Metric.Aux[Currency] = Metric[Currency]("taxNo", 20, rarelyQueried)
  val documentNumberField: Metric.Aux[Long] = Metric[Long]("documentNumber", 4, rarelyQueried)
  val totalReceiptSumField: Metric.Aux[Currency] = Metric[Currency]("totalReceiptSum", 5, rarelyQueried)
  val totalReceiptCardSumField: Metric.Aux[Currency] = Metric[Currency]("totalReceiptCardSum", 6, rarelyQueried)
  val calculationTypeSignField: Metric.Aux[Int] = Metric[Int]("calculationTypeSign", 21)
  val calculationSubjectSignField: Metric.Aux[Int] = Metric[Int]("calculationSubjectSign", 22)
  val measureField: Metric.Aux[String] = Metric[String]("measure", 23)
  val tax16667Field: Metric.Aux[Currency] = Metric[Currency]("tax16667", 24, rarelyQueried)
  val tax20000Field: Metric.Aux[Currency] = Metric[Currency]("tax20000", 25, rarelyQueried)
  val nomenclatureTypeField: Metric.Aux[String] = Metric[String]("nomenclatureType", tag = 26)
  val gtinField: Metric.Aux[String] = Metric[String](name = "GTIN", tag = 27)
  val shiftField: Metric.Aux[Int] = Metric[Int](name = "shift", tag = 28, rarelyQueried)

  val taxationTypeField: Metric.Aux[Int] = Metric[Int](name = "taxationType", tag = 29, rarelyQueried)
  val customerField: Metric.Aux[String] = Metric[String](name = "customer", tag = 30, rarelyQueried)
  val nomenclatureCodeField: Metric.Aux[Blob] = Metric[Blob](name = "nomenclatureCode", tag = 31, rarelyQueried)
  val ndsRateField: Metric.Aux[Int] = Metric[Int](name = "ndsRate", tag = 32, rarelyQueried)
  val totalReceiptCashSumField: Metric.Aux[Currency] = Metric[Currency]("totalReceiptCashSum", 33, rarelyQueried)
  val totalReceiptPrepaymentSumField: Metric.Aux[Currency] =
    Metric[Currency]("totalReceiptPrepaymentSum", 34, rarelyQueried)
  val totalReceiptPostpaymentSumField: Metric.Aux[Currency] =
    Metric[Currency]("totalReceiptPostpaymentSum", 35, rarelyQueried)
  val totalReceiptCounterSubmissionSumField: Metric.Aux[Currency] =
    Metric[Currency]("totalReceiptCounterSubmissionSum", 36, rarelyQueried)
  val agentModeField: Metric.Aux[Int] = Metric[Int](name = "agentMode", tag = 37, rarelyQueried)
  val paymentAgentOperationField: Metric.Aux[String] =
    Metric[String](name = "paymentAgentOperation", tag = 38, rarelyQueried)
  val operatorNameField: Metric.Aux[String] = Metric[String](name = "operatorName", tag = 39, rarelyQueried)
  val operatorInnField: Metric.Aux[String] = Metric[String](name = "operatorInn", tag = 40, rarelyQueried)
  val requestNumberField: Metric.Aux[Int] = Metric[Int](name = "requestNumber", tag = 41, rarelyQueried)

  val totalReceiptTax20000Field: Metric.Aux[Currency] = Metric[Currency]("totalReceiptTax20000", 49, rarelyQueried)
  val totalReceiptTax10000Field: Metric.Aux[Currency] = Metric[Currency]("totalReceiptTax10000", 44, rarelyQueried)
  val totalReceiptTax00000Field: Metric.Aux[Currency] = Metric[Currency]("totalReceiptTax00000", 42, rarelyQueried)
  val totalReceiptTaxNoField: Metric.Aux[Currency] = Metric[Currency]("totalReceiptTaxNo", 47, rarelyQueried)
  val totalReceiptTax16667Field: Metric.Aux[Currency] = Metric[Currency]("totalReceiptTax16667", 48, rarelyQueried)
  val totalReceiptTax09091Field: Metric.Aux[Currency] = Metric[Currency]("totalReceiptTax09091", 43, rarelyQueried)
  val totalReceiptTax15255Field: Metric.Aux[Currency] = Metric[Currency]("totalReceiptTax15255", 50, rarelyQueried)
  val totalReceiptTax18000Field: Metric.Aux[Currency] = Metric[Currency]("totalReceiptTax18000", 51, rarelyQueried)

  val documentNameField: Metric.Aux[Int] = Metric[Int]("documentName", 52, rarelyQueried)
  val acceptedAtField: Metric.Aux[Time] = Metric[Time]("acceptedAt", 53, rarelyQueried)

  val itemCountField: Metric.Aux[Long] = Metric[Long]("itemCount", 54)

  val metrics: Seq[Metric] = Seq(
    quantityField,
    sumField,
    tax00000Field,
    tax09091Field,
    tax10000Field,
    tax15255Field,
    tax16667Field,
    tax18000Field,
    tax20000Field,
    taxNoField,
    documentNumberField,
    totalReceiptSumField,
    totalReceiptCardSumField,
    calculationTypeSignField,
    calculationSubjectSignField,
    measureField,
    nomenclatureTypeField,
    gtinField,
    shiftField,
    taxationTypeField,
    customerField,
    nomenclatureCodeField,
    ndsRateField,
    totalReceiptCashSumField,
    totalReceiptPrepaymentSumField,
    totalReceiptPostpaymentSumField,
    totalReceiptCounterSubmissionSumField,
    agentModeField,
    paymentAgentOperationField,
    operatorNameField,
    operatorInnField,
    requestNumberField,
    totalReceiptTax20000Field,
    totalReceiptTax10000Field,
    totalReceiptTax00000Field,
    totalReceiptTaxNoField,
    totalReceiptTax16667Field,
    totalReceiptTax09091Field,
    totalReceiptTax15255Field,
    totalReceiptTax18000Field,
    documentNameField,
    acceptedAtField
  )

  import org.yupana.api.query.syntax.All._

  object ItemRollupFields {
    val baseFields = Seq(
      QueryFieldToDimension(dimension(Dimensions.ITEM) as Dimensions.ITEM.name, Dimensions.ITEM),
      QueryFieldToDimension(
        dimension(Dimensions.OPERATION_TYPE) as Dimensions.OPERATION_TYPE.name,
        Dimensions.OPERATION_TYPE
      ),
      QueryFieldToMetric(sum(metric(sumField)) as sumField.name, sumField),
      QueryFieldToMetric(sum(metric(quantityField)) as quantityField.name, quantityField)
    )
    val kkmIdDim = QueryFieldToDimension(dimension(Dimensions.KKM_ID) as Dimensions.KKM_ID.name, Dimensions.KKM_ID)
    val countFromRawData = QueryFieldToMetric(count(time) as itemCountField.name, itemCountField)
    val countFromRollup = QueryFieldToMetric(sum(metric(itemCountField)) as itemCountField.name, itemCountField)
  }
}

object ItemTableMetrics extends ItemTableMetrics

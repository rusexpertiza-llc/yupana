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

import org.yupana.api.{ Currency, Time }
import org.yupana.api.query.UnaryMinusExpr
import org.yupana.api.schema.{ Metric, QueryFieldToDimension, QueryFieldToMetric }

trait ReceiptTableMetrics {

  import Metric.Groups._

  val totalSumField = Metric[Currency]("totalSum", 1)
  val cashSumField = Metric[Currency]("cashSum", 2, rarelyQueried)
  val cardSumField = Metric[Currency]("cardSum", 3, rarelyQueried)
  val positionsCountField = Metric[Long]("positionsCount", 4)
  val totalTaxField = Metric[Currency]("totalTax", 5, rarelyQueried)
  val tax00000Field = Metric[Currency]("tax00000", 6, rarelyQueried)
  val tax09091Field = Metric[Currency]("tax09091", 7, rarelyQueried)
  val tax10000Field = Metric[Currency]("tax10000", 8, rarelyQueried)
  val tax15255Field = Metric[Currency]("tax15255", 9, rarelyQueried)
  val tax16667Field = Metric[Currency]("tax16667", 26, rarelyQueried)
  val tax18000Field = Metric[Currency]("tax18000", 10, rarelyQueried)
  val tax20000Field = Metric[Currency]("tax20000", 27, rarelyQueried)
  val taxNoField = Metric[Currency]("taxNo", 20, rarelyQueried)
  val itemsCountField = Metric[Long]("itemsCount", 11)
  val minTimeField = Metric[Time]("minTime", 13, rarelyQueried)
  val maxTimeField = Metric[Time]("maxTime", 14, rarelyQueried)
  val receiptCountField = Metric[Long]("receiptCount", 15)
  val kkmDistinctCountField = Metric[Int]("kkmDistinctCount", 16)
  val correctionBasis = Metric[String]("correctionBasis", 17, rarelyQueried)
  val correctionDocumentNumber = Metric[String]("correctionDocumentNumber", 18, rarelyQueried)
  val correctionDocumentDateTime = Metric[Long]("correctionDocumentDateTime", 19, rarelyQueried)
  val postpaymentSumField = Metric[Currency]("postpaymentSum", 21, rarelyQueried)
  val counterSubmissionSumField = Metric[Currency]("counterSubmissionSum", 22, rarelyQueried)
  val prepaymentSumField = Metric[Currency]("prepaymentSum", 23, rarelyQueried)
  val taxationType = Metric[Int]("taxationType", 24, rarelyQueried)
  val acceptedAt = Metric[Time]("acceptedAt", 25, rarelyQueried)
  val cashReceiptCountField = Metric[Long]("cashReceiptCount", 28)
  val cardReceiptCountField = Metric[Long]("cardReceiptCount", 29)
  val documentNumberField = Metric[Long]("documentNumber", 30, rarelyQueried)
  val operator = Metric[String]("operator", 31, rarelyQueried)
  val totalQuantityField = Metric[Double]("totalQuantity", 32, rarelyQueried)

  val baseFields: Seq[Metric] = Seq(
    totalSumField,
    cashSumField,
    cardSumField,
    prepaymentSumField,
    postpaymentSumField,
    counterSubmissionSumField,
    positionsCountField,
    totalTaxField,
    tax00000Field,
    tax09091Field,
    tax10000Field,
    tax15255Field,
    tax16667Field,
    tax18000Field,
    tax20000Field,
    taxNoField,
    itemsCountField,
    totalQuantityField
  )

  val rollupFields = Seq(
    minTimeField,
    maxTimeField,
    receiptCountField,
    cashReceiptCountField,
    cardReceiptCountField
  )

  val summaryFields = Seq(
    kkmDistinctCountField,
    totalSumField,
    totalQuantityField,
    cashSumField,
    cardSumField,
    positionsCountField,
    itemsCountField
  )

  import org.yupana.api.query.syntax.All._

  object ReceiptRollupFields {

    val baseRollupFields = Seq(
      QueryFieldToDimension(dimension(Dimensions.KKM_ID) as Dimensions.KKM_ID.name, Dimensions.KKM_ID),
      QueryFieldToDimension(
        dimension(Dimensions.OPERATION_TYPE) as Dimensions.OPERATION_TYPE.name,
        Dimensions.OPERATION_TYPE
      ),
      QueryFieldToMetric(sum(metric(totalSumField)) as totalSumField.name, totalSumField),
      QueryFieldToMetric(sum(metric(cashSumField)) as cashSumField.name, cashSumField),
      QueryFieldToMetric(sum(metric(cardSumField)) as cardSumField.name, cardSumField),
      QueryFieldToMetric(sum(metric(prepaymentSumField)) as prepaymentSumField.name, prepaymentSumField),
      QueryFieldToMetric(sum(metric(postpaymentSumField)) as postpaymentSumField.name, postpaymentSumField),
      QueryFieldToMetric(
        sum(metric(counterSubmissionSumField)) as counterSubmissionSumField.name,
        counterSubmissionSumField
      ),
      QueryFieldToMetric(sum(metric(positionsCountField)) as positionsCountField.name, positionsCountField),
      QueryFieldToMetric(sum(metric(totalTaxField)) as totalTaxField.name, totalTaxField),
      QueryFieldToMetric(sum(metric(tax00000Field)) as tax00000Field.name, tax00000Field),
      QueryFieldToMetric(sum(metric(tax09091Field)) as tax09091Field.name, tax09091Field),
      QueryFieldToMetric(sum(metric(tax10000Field)) as tax10000Field.name, tax10000Field),
      QueryFieldToMetric(sum(metric(tax15255Field)) as tax15255Field.name, tax15255Field),
      QueryFieldToMetric(sum(metric(tax16667Field)) as tax16667Field.name, tax16667Field),
      QueryFieldToMetric(sum(metric(tax18000Field)) as tax18000Field.name, tax18000Field),
      QueryFieldToMetric(sum(metric(tax20000Field)) as tax20000Field.name, tax20000Field),
      QueryFieldToMetric(sum(metric(itemsCountField)) as itemsCountField.name, itemsCountField),
      QueryFieldToMetric(sum(metric(taxNoField)) as taxNoField.name, taxNoField),
      QueryFieldToMetric(count(metric(documentNumberField)) as documentNumberField.name, receiptCountField),
      QueryFieldToMetric(sum(metric(totalQuantityField)) as totalQuantityField.name, totalQuantityField)
    )

    val shiftRollupFields = Seq(
      QueryFieldToDimension(dimension(Dimensions.SHIFT) as Dimensions.SHIFT.name, Dimensions.SHIFT)
    )

    val additionalRollupFieldsFromDetails = Seq(
      QueryFieldToMetric(min(time) as minTimeField.name, minTimeField),
      QueryFieldToMetric(max(time) as maxTimeField.name, maxTimeField),
      QueryFieldToMetric(count(time) as receiptCountField.name, receiptCountField),
      QueryFieldToMetric(
        sum(
          condition[Long](gt(metric(cashSumField), const(Currency(0))), const(1L), const(0L))
        ) as cashReceiptCountField.name,
        cashReceiptCountField
      ),
      QueryFieldToMetric(
        sum(
          condition[Long](gt(metric(cardSumField), const(Currency(0))), const(1L), const(0L))
        ) as cardReceiptCountField.name,
        cardReceiptCountField
      )
    )

    val additionalRollupFieldsFromRollups = Seq(
      QueryFieldToMetric(min(metric(minTimeField)) as minTimeField.name, minTimeField),
      QueryFieldToMetric(max(metric(maxTimeField)) as maxTimeField.name, maxTimeField),
      QueryFieldToMetric(sum(metric(receiptCountField)) as receiptCountField.name, receiptCountField),
      QueryFieldToMetric(sum(metric(cashReceiptCountField)) as cashReceiptCountField.name, cashReceiptCountField),
      QueryFieldToMetric(sum(metric(cardReceiptCountField)) as cardReceiptCountField.name, cardReceiptCountField)
    )

    val summaryRollupFields = Seq(
      QueryFieldToMetric(
        distinctCount(dimension(Dimensions.KKM_ID)) as kkmDistinctCountField.name,
        kkmDistinctCountField
      ),
      QueryFieldToMetric(
        sum(
          condition[Currency](
            equ(dimension(Dimensions.OPERATION_TYPE), const(2.toByte)),
            metric(totalSumField),
            condition[Currency](
              equ(dimension(Dimensions.OPERATION_TYPE), const(3.toByte)),
              UnaryMinusExpr(metric(totalSumField)),
              const(Currency(0))
            )
          )
        ) as totalSumField.name,
        totalSumField
      ),
      QueryFieldToMetric(
        sum(
          condition[Currency](
            equ(dimension(Dimensions.OPERATION_TYPE), const(2.toByte)),
            metric(cashSumField),
            condition[Currency](
              equ(dimension(Dimensions.OPERATION_TYPE), const(3.toByte)),
              UnaryMinusExpr(metric(cashSumField)),
              const(Currency(0))
            )
          )
        ) as cashSumField.name,
        cashSumField
      ),
      QueryFieldToMetric(
        sum(
          condition[Currency](
            equ(dimension(Dimensions.OPERATION_TYPE), const(2.toByte)),
            metric(cardSumField),
            condition[Currency](
              equ(dimension(Dimensions.OPERATION_TYPE), const(3.toByte)),
              UnaryMinusExpr(metric(cardSumField)),
              const(Currency(0))
            )
          )
        ) as cardSumField.name,
        cardSumField
      ),
      QueryFieldToMetric(
        sum(
          condition[Long](
            equ(dimension(Dimensions.OPERATION_TYPE), const(2.toByte)),
            metric(positionsCountField),
            condition[Long](
              equ(dimension(Dimensions.OPERATION_TYPE), const(3.toByte)),
              UnaryMinusExpr(metric(positionsCountField)),
              const(0L)
            )
          )
        ) as positionsCountField.name,
        positionsCountField
      ),
      QueryFieldToMetric(
        sum(
          condition[Long](
            equ(dimension(Dimensions.OPERATION_TYPE), const(2.toByte)),
            metric(itemsCountField),
            condition[Long](
              equ(dimension(Dimensions.OPERATION_TYPE), const(3.toByte)),
              UnaryMinusExpr(metric(itemsCountField)),
              const(0L)
            )
          )
        ) as itemsCountField.name,
        itemsCountField
      ),
      QueryFieldToMetric(
        sum(
          condition[Double](
            equ(dimension(Dimensions.OPERATION_TYPE), const(2.toByte)),
            metric(totalQuantityField),
            condition[Double](
              equ(dimension(Dimensions.OPERATION_TYPE), const(3.toByte)),
              UnaryMinusExpr(metric(totalQuantityField)),
              const(0d)
            )
          )
        ) as totalQuantityField.name,
        totalQuantityField
      )
    )
  }
}

object ReceiptTableMetrics extends ReceiptTableMetrics

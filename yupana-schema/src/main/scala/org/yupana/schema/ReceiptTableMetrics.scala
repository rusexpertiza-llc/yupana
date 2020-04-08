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

import org.yupana.api.Time
import org.yupana.api.schema.{ Metric, QueryFieldToDimension, QueryFieldToMetric }
import org.yupana.api.types.Aggregation

trait ReceiptTableMetrics {
  private val rarelyQueried = 2

  val totalSumField = Metric[BigDecimal]("totalSum", 1)
  val cashSumField = Metric[BigDecimal]("cashSum", 2, rarelyQueried)
  val cardSumField = Metric[BigDecimal]("cardSum", 3, rarelyQueried)
  val positionsCountField = Metric[Long]("positionsCount", 4)
  val totalTaxField = Metric[BigDecimal]("totalTax", 5, rarelyQueried)
  val tax00000Field = Metric[BigDecimal]("tax00000", 6, rarelyQueried)
  val tax09091Field = Metric[BigDecimal]("tax09091", 7, rarelyQueried)
  val tax10000Field = Metric[BigDecimal]("tax10000", 8, rarelyQueried)
  val tax15255Field = Metric[BigDecimal]("tax15255", 9, rarelyQueried)
  val tax16667Field = Metric[BigDecimal]("tax16667", 26, rarelyQueried)
  val tax18000Field = Metric[BigDecimal]("tax18000", 10, rarelyQueried)
  val tax20000Field = Metric[BigDecimal]("tax20000", 27, rarelyQueried)
  val taxNoField = Metric[BigDecimal]("taxNo", 20, rarelyQueried)
  val itemsCountField = Metric[Long]("itemsCount", 11)
  val minTimeField = Metric[Time]("minTime", 13, rarelyQueried)
  val maxTimeField = Metric[Time]("maxTime", 14, rarelyQueried)
  val receiptCountField = Metric[Long]("receiptCount", 15)
  val kkmDistinctCountField = Metric[Int]("kkmDistinctCount", 16)
  val correctionBasis = Metric[String]("correctionBasis", 17, rarelyQueried)
  val correctionDocumentNumber = Metric[String]("correctionDocumentNumber", 18, rarelyQueried)
  val correctionDocumentDateTime = Metric[Long]("correctionDocumentDateTime", 19, rarelyQueried)
  val postpaymentSumField = Metric[BigDecimal]("postpaymentSum", 21, rarelyQueried)
  val counterSubmissionSumField = Metric[BigDecimal]("counterSubmissionSum", 22, rarelyQueried)
  val prepaymentSumField = Metric[BigDecimal]("prepaymentSum", 23, rarelyQueried)
  val taxationType = Metric[Int]("taxationType", 24, rarelyQueried)
  val acceptedAt = Metric[Time]("acceptedAt", 25, rarelyQueried)
  val cashReceiptCountField = Metric[Long]("cashReceiptCount", 28)
  val cardReceiptCountField = Metric[Long]("cardReceiptCount", 29)

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
    itemsCountField
  )

  val rollupFields = Seq(
    minTimeField,
    maxTimeField,
    receiptCountField,
    cashReceiptCountField,
    cardReceiptCountField
  )
  import org.yupana.api.query.syntax.All._

  object ReceiptRollupFields {

    val baseRollupFields = Seq(
      QueryFieldToDimension(dimension(Dimensions.KKM_ID_TAG) as Dimensions.KKM_ID_TAG.name, Dimensions.KKM_ID_TAG),
      QueryFieldToDimension(
        dimension(Dimensions.OPERATION_TYPE_TAG) as Dimensions.OPERATION_TYPE_TAG.name,
        Dimensions.OPERATION_TYPE_TAG
      ),
      QueryFieldToMetric(
        aggregate(Aggregation.sum[BigDecimal], metric(totalSumField)) as totalSumField.name,
        totalSumField
      ),
      QueryFieldToMetric(
        aggregate(Aggregation.sum[BigDecimal], metric(cashSumField)) as cashSumField.name,
        cashSumField
      ),
      QueryFieldToMetric(
        aggregate(Aggregation.sum[BigDecimal], metric(cardSumField)) as cardSumField.name,
        cardSumField
      ),
      QueryFieldToMetric(
        aggregate(Aggregation.sum[BigDecimal], metric(prepaymentSumField)) as prepaymentSumField.name,
        prepaymentSumField
      ),
      QueryFieldToMetric(
        aggregate(Aggregation.sum[BigDecimal], metric(postpaymentSumField)) as postpaymentSumField.name,
        postpaymentSumField
      ),
      QueryFieldToMetric(
        aggregate(Aggregation.sum[BigDecimal], metric(counterSubmissionSumField)) as counterSubmissionSumField.name,
        counterSubmissionSumField
      ),
      QueryFieldToMetric(
        aggregate(Aggregation.sum[Long], metric(positionsCountField)) as positionsCountField.name,
        positionsCountField
      ),
      QueryFieldToMetric(
        aggregate(Aggregation.sum[BigDecimal], metric(totalTaxField)) as totalTaxField.name,
        totalTaxField
      ),
      QueryFieldToMetric(
        aggregate(Aggregation.sum[BigDecimal], metric(tax00000Field)) as tax00000Field.name,
        tax00000Field
      ),
      QueryFieldToMetric(
        aggregate(Aggregation.sum[BigDecimal], metric(tax09091Field)) as tax09091Field.name,
        tax09091Field
      ),
      QueryFieldToMetric(
        aggregate(Aggregation.sum[BigDecimal], metric(tax10000Field)) as tax10000Field.name,
        tax10000Field
      ),
      QueryFieldToMetric(
        aggregate(Aggregation.sum[BigDecimal], metric(tax15255Field)) as tax15255Field.name,
        tax15255Field
      ),
      QueryFieldToMetric(
        aggregate(Aggregation.sum[BigDecimal], metric(tax16667Field)) as tax16667Field.name,
        tax16667Field
      ),
      QueryFieldToMetric(
        aggregate(Aggregation.sum[BigDecimal], metric(tax18000Field)) as tax18000Field.name,
        tax18000Field
      ),
      QueryFieldToMetric(
        aggregate(Aggregation.sum[BigDecimal], metric(tax20000Field)) as tax20000Field.name,
        tax20000Field
      ),
      QueryFieldToMetric(
        aggregate(Aggregation.sum[Long], metric(itemsCountField)) as itemsCountField.name,
        itemsCountField
      ),
      QueryFieldToMetric(aggregate(Aggregation.sum[BigDecimal], metric(taxNoField)) as taxNoField.name, taxNoField)
    )

    val shiftRollupFields = Seq(
      QueryFieldToDimension(dimension(Dimensions.SHIFT_TAG) as Dimensions.SHIFT_TAG.name, Dimensions.SHIFT_TAG),
      QueryFieldToDimension(dimension(Dimensions.OPERATOR_TAG) as Dimensions.OPERATOR_TAG.name, Dimensions.OPERATOR_TAG)
    )

    val additionalRollupFieldsFromDetails = Seq(
      QueryFieldToMetric(aggregate(Aggregation.min[Time], time) as minTimeField.name, minTimeField),
      QueryFieldToMetric(aggregate(Aggregation.max[Time], time) as maxTimeField.name, maxTimeField),
      QueryFieldToMetric(
        aggregate(
          Aggregation.sum[Long],
          condition[Long](gt(metric(cashSumField), const(BigDecimal(0))), const(1L), const(0L))
        ) as cashReceiptCountField.name,
        cashReceiptCountField
      ),
      QueryFieldToMetric(
        aggregate(
          Aggregation.sum[Long],
          condition[Long](gt(metric(cardSumField), const(BigDecimal(0))), const(1L), const(0L))
        ) as cardReceiptCountField.name,
        cardReceiptCountField
      )
    )

    val additionalRollupFieldsFromRollups = Seq(
      QueryFieldToMetric(aggregate(Aggregation.min[Time], metric(minTimeField)) as minTimeField.name, minTimeField),
      QueryFieldToMetric(aggregate(Aggregation.max[Time], metric(maxTimeField)) as maxTimeField.name, maxTimeField),
      QueryFieldToMetric(
        aggregate(Aggregation.sum[Long], metric(receiptCountField)) as receiptCountField.name,
        receiptCountField
      ),
      QueryFieldToMetric(
        aggregate(Aggregation.sum[Long], metric(cashReceiptCountField)) as cashReceiptCountField.name,
        cashReceiptCountField
      ),
      QueryFieldToMetric(
        aggregate(Aggregation.sum[Long], metric(cardReceiptCountField)) as cardReceiptCountField.name,
        cardReceiptCountField
      )
    )

    val kkmDistinctCountRollupField = QueryFieldToMetric(
      aggregate(Aggregation.distinctCount[Int], dimension(Dimensions.KKM_ID_TAG)) as kkmDistinctCountField.name,
      kkmDistinctCountField
    )
  }
}

object ReceiptTableMetrics extends ReceiptTableMetrics

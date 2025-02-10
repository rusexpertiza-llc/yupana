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

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.{ SparkConf, SparkContext }
import org.yupana.api.{ Blob, Currency }
import org.yupana.api.query.DataPoint
import org.yupana.api.schema.{ Dimension, MetricValue }
import org.yupana.core.auth.TsdbRole.ReadWrite
import org.yupana.core.auth.YupanaUser
import org.yupana.examples.ExampleSchema
import org.yupana.schema._
import org.yupana.spark.{ EtlConfig, EtlContext, SparkConfUtils }

import java.time.ZoneOffset

object ETL extends StrictLogging {

  def main(args: Array[String]): Unit = {
    val user = YupanaUser("ETL", None, ReadWrite)
    val conf = new SparkConf().setAppName("Yupana-ETL")
    SparkConfUtils.removeSparkPrefix(conf)
    val sc = SparkContext.getOrCreate(conf)

    val cfg = new EtlConfig(conf)

    val ctx = new EtlContext(cfg, ExampleSchema.schema)

    import org.yupana.spark.ETLFunctions._

    val receiptsRdd = sc.parallelize(DataSource.getReceipts(1000))

    receiptsRdd
      .flatMap(toDataPoints)
      .saveDataPoints(ctx, user)

  }

  def toDataPoints(receipt: Receipt): Seq[DataPoint] = {
    toItemDataPoints(receipt) ++ toReceiptDataPoints(receipt)
  }

  def toItemDataPoints(receipt: Receipt): Seq[DataPoint] = {

    val commonDims: Map[Dimension, Any] = Map(
      Dimensions.KKM_ID -> receipt.kkmId,
      Dimensions.OPERATION_TYPE -> receipt.operationType.toByte,
      Dimensions.SHIFT -> receipt.shiftNumber
    )

    val commonMetrics = Seq(
      Some(MetricValue(ItemTableMetrics.documentNumberField, receipt.receiptNumber.toLong)),
      Some(MetricValue(ItemTableMetrics.totalReceiptSumField, Currency.of(receipt.totalSum))),
      receipt.totalCardSum.map(v => MetricValue(ItemTableMetrics.totalReceiptCardSumField, Currency.of(v))),
      Some(MetricValue(ItemTableMetrics.shiftField, receipt.shiftNumber))
    ).flatten

    receipt.items.zipWithIndex.flatMap {
      case (item, idx) =>
        val dims: Map[Dimension, Any] =
          commonDims ++ Map(Dimensions.ITEM -> item.name, Dimensions.POSITION -> idx.toShort)

        val itemMetrics = Seq(
          Some(MetricValue(ItemTableMetrics.sumField, Currency.of(item.sum))),
          Some(MetricValue(ItemTableMetrics.quantityField, item.quantity)),
          item.taxes.get(Tax.tax00000).map(v => MetricValue(ItemTableMetrics.tax00000Field, Currency.of(v))),
          item.taxes.get(Tax.tax09091).map(v => MetricValue(ItemTableMetrics.tax09091Field, Currency.of(v))),
          item.taxes.get(Tax.tax10000).map(v => MetricValue(ItemTableMetrics.tax10000Field, Currency.of(v))),
          item.taxes.get(Tax.tax15255).map(v => MetricValue(ItemTableMetrics.tax15255Field, Currency.of(v))),
          item.taxes.get(Tax.tax16667).map(v => MetricValue(ItemTableMetrics.tax16667Field, Currency.of(v))),
          item.taxes.get(Tax.tax18000).map(v => MetricValue(ItemTableMetrics.tax18000Field, Currency.of(v))),
          item.taxes.get(Tax.tax20000).map(v => MetricValue(ItemTableMetrics.tax20000Field, Currency.of(v))),
          item.taxes.get(Tax.taxNo).map(v => MetricValue(ItemTableMetrics.taxNoField, Currency.of(v))),
          item.calcTypeSign.map(v => MetricValue(ItemTableMetrics.calculationTypeSignField, v)),
          item.calcSubjSing.map(v => MetricValue(ItemTableMetrics.calculationSubjectSignField, v)),
          item.measure.map(v => MetricValue(ItemTableMetrics.measureField, v)),
          item.nomenclatureType.map(v => MetricValue(ItemTableMetrics.nomenclatureTypeField, v)),
          item.gtin.map(v => MetricValue(ItemTableMetrics.gtinField, v)),
          item.nomenclatureCode.map(v => MetricValue(ItemTableMetrics.nomenclatureCodeField, Blob(v)))
        ).flatten

        val metrics = commonMetrics ++ itemMetrics

        val dp = DataPoint(
          Tables.itemsKkmTable,
          receipt.date.toInstant(ZoneOffset.UTC).toEpochMilli,
          dims,
          metrics
        )

        Seq(dp, dp.copy(table = Tables.kkmItemsTable))
    }
  }

  def toReceiptDataPoints(receipt: Receipt): Seq[DataPoint] = {

    val dims: Map[Dimension, Any] = Map(
      Dimensions.KKM_ID -> receipt.kkmId,
      Dimensions.OPERATION_TYPE -> receipt.operationType.toByte,
      Dimensions.SHIFT -> receipt.shiftNumber
    )

    val metrics = Seq(
      Some(MetricValue(ReceiptTableMetrics.totalSumField, Currency.of(receipt.totalSum))),
      Some(MetricValue(ReceiptTableMetrics.operator, receipt.operator)),
      receipt.totalCashSum.map(v => MetricValue(ReceiptTableMetrics.cashSumField, Currency.of(v))),
      receipt.totalCardSum.map(v => MetricValue(ReceiptTableMetrics.cardSumField, Currency.of(v))),
      receipt.prePayment.map(v => MetricValue(ReceiptTableMetrics.prepaymentSumField, Currency.of(v))),
      receipt.postPayment.map(v => MetricValue(ReceiptTableMetrics.postpaymentSumField, Currency.of(v))),
      receipt.counterSubmission.map(v => MetricValue(ReceiptTableMetrics.counterSubmissionSumField, Currency.of(v))),
      Some(MetricValue(ReceiptTableMetrics.positionsCountField, receipt.items.size)),
      totalTax(receipt.taxes).map(v => MetricValue(ReceiptTableMetrics.totalTaxField, Currency.of(v))),
      receipt.taxes.get(Tax.tax00000).map(v => MetricValue(ReceiptTableMetrics.tax00000Field, Currency.of(v))),
      receipt.taxes.get(Tax.tax09091).map(v => MetricValue(ReceiptTableMetrics.tax09091Field, Currency.of(v))),
      receipt.taxes.get(Tax.tax10000).map(v => MetricValue(ReceiptTableMetrics.tax10000Field, Currency.of(v))),
      receipt.taxes.get(Tax.tax15255).map(v => MetricValue(ReceiptTableMetrics.tax15255Field, Currency.of(v))),
      receipt.taxes.get(Tax.tax16667).map(v => MetricValue(ReceiptTableMetrics.tax16667Field, Currency.of(v))),
      receipt.taxes.get(Tax.tax18000).map(v => MetricValue(ReceiptTableMetrics.tax18000Field, Currency.of(v))),
      receipt.taxes.get(Tax.tax20000).map(v => MetricValue(ReceiptTableMetrics.tax20000Field, Currency.of(v))),
      receipt.taxes.get(Tax.taxNo).map(v => MetricValue(ReceiptTableMetrics.taxNoField, Currency.of(v))),
      Some(MetricValue(ReceiptTableMetrics.itemsCountField, receipt.items.distinct.size))
    ).flatten

    val dp = DataPoint(
      Tables.receiptTable,
      receipt.date.toInstant(ZoneOffset.UTC).toEpochMilli,
      dims,
      metrics
    )

    Seq(dp)
  }

  private def totalTax(taxes: Map[String, BigDecimal]): Option[BigDecimal] = {
    val significant = taxes.filter { case (t, _) => t != Tax.taxNo && t != Tax.tax00000 }.values
    if (significant.nonEmpty) Some(significant.sum) else None
  }
}

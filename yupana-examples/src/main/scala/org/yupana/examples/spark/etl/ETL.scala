package org.yupana.examples.spark.etl

import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTimeZone
import org.yupana.api.query.DataPoint
import org.yupana.api.schema.MetricValue
import org.yupana.examples.ExampleSchema
import org.yupana.schema._
import org.yupana.spark.{EtlConfig, EtlContext, SparkConfUtils}

object ETL {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Yupana-ETL")
    SparkConfUtils.removeSparkPrefix(conf)
    val sc = SparkContext.getOrCreate(conf)

    val cfg = new EtlConfig(conf)
    val ctx = new EtlContext(cfg, ExampleSchema.schema)

    import org.yupana.spark.ETLFunctions._

    val receiptsRdd = sc.parallelize(DataSource.getReceipts(1000))

    receiptsRdd
      .flatMap(toDataPoints)
      .saveDataPoints(ctx, ExampleSchema.schema)

  }

  def toDataPoints(receipt: Receipt): Seq[DataPoint] = {
    toItemDataPoints(receipt) ++ toReceiptDataPoints(receipt)
  }

  def toItemDataPoints(receipt: Receipt): Seq[DataPoint] = {

    val commonDims = Map(
      Dimensions.KKM_ID_TAG -> receipt.kkmId.toString,
      Dimensions.OPERATION_TYPE_TAG -> receipt.operationType,
      Dimensions.SHIFT_TAG -> receipt.shiftNumber.toString,
      Dimensions.OPERATOR_TAG -> receipt.operator
    )

    val commonMetrics = Seq(
      Some(MetricValue(ItemTableMetrics.documentNumberField, receipt.receiptNumber.toLong)),
      Some(MetricValue(ItemTableMetrics.totalReceiptSumField,  receipt.totalSum)),
      receipt.totalCardSum.map(v => MetricValue(ItemTableMetrics.totalReceiptCardSumField, v)),
      Some(MetricValue(ItemTableMetrics.shiftField, receipt.shiftNumber))
    ).flatten

    receipt.items.zipWithIndex.flatMap { case (item, idx) =>

      val dims = commonDims ++ Map(Dimensions.ITEM_TAG -> item.name,  Dimensions.POSITION_TAG -> idx.toString)

      val itemMetrics = Seq(
        Some(MetricValue(ItemTableMetrics.sumField, item.sum)),
        Some(MetricValue(ItemTableMetrics.quantityField, item.quantity)),
        item.taxes.get(Tax.tax00000).map(v => MetricValue(ItemTableMetrics.tax00000Field, v)),
        item.taxes.get(Tax.tax09091).map(v => MetricValue(ItemTableMetrics.tax09091Field, v)),
        item.taxes.get(Tax.tax10000).map(v => MetricValue(ItemTableMetrics.tax10000Field, v)),
        item.taxes.get(Tax.tax15255).map(v => MetricValue(ItemTableMetrics.tax15255Field, v)),
        item.taxes.get(Tax.tax16667).map(v => MetricValue(ItemTableMetrics.tax16667Field, v)),
        item.taxes.get(Tax.tax18000).map(v => MetricValue(ItemTableMetrics.tax18000Field, v)),
        item.taxes.get(Tax.tax20000).map(v => MetricValue(ItemTableMetrics.tax20000Field, v)),
        item.taxes.get(Tax.taxNo).map(v => MetricValue(ItemTableMetrics.taxNoField, v)),
        item.calcTypeSign.map(v => MetricValue(ItemTableMetrics.calculationTypeSignField, v)),
        item.calcSubjSing.map(v => MetricValue(ItemTableMetrics.calculationSubjectSignField, v)),
        item.measure.map(v => MetricValue(ItemTableMetrics.measureField, v)),
        item.nomenclatureType.map(v => MetricValue(ItemTableMetrics.nomenclatureTypeField, v)),
        item.gtin.map(v => MetricValue(ItemTableMetrics.gtinField, v))
      ).flatten

      val metrics = commonMetrics ++ itemMetrics

      val dp = DataPoint(
        Tables.itemsKkmTable,
        receipt.date.toDateTime(DateTimeZone.UTC).getMillis,
        dims,
        metrics
      )

      Seq(dp, dp.copy(table = Tables.kkmItemsTable))
    }
  }

  def toReceiptDataPoints(receipt: Receipt): Seq[DataPoint] = {

    val dims = Map(
      Dimensions.KKM_ID_TAG -> receipt.kkmId.toString,
      Dimensions.OPERATION_TYPE_TAG -> receipt.operationType,
      Dimensions.OPERATOR_TAG -> receipt.operator,
      Dimensions.SHIFT_TAG -> receipt.shiftNumber.toString
    )

    val metrics = Seq(
      Some(MetricValue(ReceiptTableMetrics.totalSumField, receipt.totalSum)),
      receipt.totalCashSum.map(v => MetricValue(ReceiptTableMetrics.cashSumField, v)),
      receipt.totalCardSum.map(v => MetricValue(ReceiptTableMetrics.cardSumField, v)),
      receipt.prePayment.map(v => MetricValue(ReceiptTableMetrics.prepaymentSumField, v)),
      receipt.postPayment.map(v => MetricValue(ReceiptTableMetrics.postpaymentSumField, v)),
      receipt.counterSubmission.map(v => MetricValue(ReceiptTableMetrics.counterSubmissionSumField, v)),
      Some(MetricValue(ReceiptTableMetrics.positionsCountField, receipt.items.size)),
      totalTax(receipt.taxes).map(v => MetricValue(ReceiptTableMetrics.totalTaxField, v)),
      receipt.taxes.get(Tax.tax00000).map(v => MetricValue(ReceiptTableMetrics.tax00000Field, v)),
      receipt.taxes.get(Tax.tax09091).map(v => MetricValue(ReceiptTableMetrics.tax09091Field, v)),
      receipt.taxes.get(Tax.tax10000).map(v => MetricValue(ReceiptTableMetrics.tax10000Field, v)),
      receipt.taxes.get(Tax.tax15255).map(v => MetricValue(ReceiptTableMetrics.tax15255Field, v)),
      receipt.taxes.get(Tax.tax16667).map(v => MetricValue(ReceiptTableMetrics.tax16667Field, v)),
      receipt.taxes.get(Tax.tax18000).map(v => MetricValue(ReceiptTableMetrics.tax18000Field, v)),
      receipt.taxes.get(Tax.tax20000).map(v => MetricValue(ReceiptTableMetrics.tax20000Field, v)),
      receipt.taxes.get(Tax.taxNo).map(v => MetricValue(ReceiptTableMetrics.taxNoField, v)),
      Some(MetricValue(ReceiptTableMetrics.itemsCountField, receipt.items.distinct.size)),
      Some(MetricValue(ReceiptTableMetrics.transactionIdField, receipt.receiptNumber.toString))
    ).flatten

    val dp = DataPoint(
      Tables.receiptTable,
      receipt.date.toDateTime(DateTimeZone.UTC).getMillis,
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

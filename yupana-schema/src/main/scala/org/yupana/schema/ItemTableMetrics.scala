package org.yupana.schema

import org.yupana.api.schema.Metric

trait ItemTableMetrics {

  private val rarelyQueried = 2

  val quantityField: Metric.Aux[Double] = Metric[Double]("quantity", 2)
  val sumField: Metric.Aux[BigDecimal] = Metric[BigDecimal]("sum", 3)
  val tax00000Field: Metric.Aux[BigDecimal] = Metric[BigDecimal]("tax00000", 7, rarelyQueried)
  val tax09091Field: Metric.Aux[BigDecimal] = Metric[BigDecimal]("tax09091", 8, rarelyQueried)
  val tax10000Field: Metric.Aux[BigDecimal] = Metric[BigDecimal]("tax10000", 9, rarelyQueried)
  val tax15255Field: Metric.Aux[BigDecimal] = Metric[BigDecimal]("tax15255", 10, rarelyQueried)
  val tax18000Field: Metric.Aux[BigDecimal] = Metric[BigDecimal]("tax18000", 11, rarelyQueried)
  val taxNoField: Metric.Aux[BigDecimal] = Metric[BigDecimal]("taxNo", 20, rarelyQueried)
  val documentNumberField: Metric.Aux[Long] = Metric[Long]("documentNumber", 4, rarelyQueried)
  val totalReceiptSumField: Metric.Aux[BigDecimal] = Metric[BigDecimal]("totalReceiptSum", 5, rarelyQueried)
  val totalReceiptCardSumField: Metric.Aux[BigDecimal] = Metric[BigDecimal]("totalReceiptCardSum", 6, rarelyQueried)
  val calculationTypeSignField: Metric.Aux[Int] = Metric[Int]("calculationTypeSign", 21)
  val calculationSubjectSignField: Metric.Aux[Int] = Metric[Int]("calculationSubjectSign", 22)
  val measureField: Metric.Aux[String] = Metric[String]("measure", 23)
  val tax16667Field: Metric.Aux[BigDecimal] = Metric[BigDecimal]("tax16667", 24, rarelyQueried)
  val tax20000Field: Metric.Aux[BigDecimal] = Metric[BigDecimal]("tax20000", 25, rarelyQueried)
  val nomenclatureTypeField: Metric.Aux[String] = Metric[String]("nomenclatureType", tag = 26)
  val gtinField: Metric.Aux[String] = Metric[String](name = "GTIN", tag = 27)
  val shiftField: Metric.Aux[Int] = Metric[Int](name = "shift", tag = 28, rarelyQueried)

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
    shiftField
  )
}

object ItemTableMetrics extends ItemTableMetrics

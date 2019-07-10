package org.yupana.schema

import org.joda.time.DateTimeFieldType
import org.yupana.api.query.{DimensionExpr, Expression}
import org.yupana.api.schema.Rollup

object ReceiptRollups {
  import org.yupana.schema.ReceiptTableMetrics.ReceiptRollupFields._
  
  val receiptDayRollup = Rollup(
    name = "receiptByDay",
    filter = None,
    groupBy = Tables.receiptTable.dimensionSeq.map(DimensionExpr(_)),
    fields = baseRollupFields ++ shiftRollupFields ++ additionalRollupFieldsFromDetails,
    fromTable = Tables.receiptTable,
    toTable = Tables.receiptByDayTable,
    downsamplingInterval = Some(DateTimeFieldType.dayOfYear())
  )

  val receiptDayAllKkmsRollup = Rollup(
    name = "receiptByDayAllKkms",
    filter = None,
    groupBy = Seq.empty,
    fields = Seq(
      kkmDistinctCountRollupField
    ) ++ additionalRollupFieldsFromRollups,
    fromTable = Tables.receiptByDayTable,
    toTable = Tables.receiptByDayAllKkmsTable,
    downsamplingInterval = Some(DateTimeFieldType.dayOfYear())
  )

  val receiptWeekRollup = Rollup(
    name = "receiptByWeek",
    filter = None,
    groupBy = Seq[Expression](DimensionExpr(Dimensions.KKM_ID_TAG), DimensionExpr(Dimensions.OPERATION_TYPE_TAG)),
    fields = baseRollupFields ++ additionalRollupFieldsFromRollups,
    fromTable = Tables.receiptByDayTable,
    toTable = Tables.receiptByWeekTable,
    downsamplingInterval = Some(DateTimeFieldType.weekOfWeekyear())
  )

  val receiptMonthRollup = Rollup(
    name = "receiptByMonth",
    filter = None,
    groupBy = Seq[Expression](DimensionExpr(Dimensions.KKM_ID_TAG),
      DimensionExpr(Dimensions.OPERATION_TYPE_TAG)),
    fields = baseRollupFields ++ additionalRollupFieldsFromRollups,
    fromTable = Tables.receiptByDayTable,
    toTable = Tables.receiptByMonthTable,
    downsamplingInterval = Some(DateTimeFieldType.monthOfYear())
  )

}

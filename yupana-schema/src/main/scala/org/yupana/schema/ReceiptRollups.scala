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

import org.joda.time.DateTimeFieldType
import org.yupana.api.query.{ DimensionExpr, Expression }
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
    groupBy = Seq[Expression](DimensionExpr(Dimensions.KKM_ID_TAG), DimensionExpr(Dimensions.OPERATION_TYPE_TAG)),
    fields = baseRollupFields ++ additionalRollupFieldsFromRollups,
    fromTable = Tables.receiptByDayTable,
    toTable = Tables.receiptByMonthTable,
    downsamplingInterval = Some(DateTimeFieldType.monthOfYear())
  )

}

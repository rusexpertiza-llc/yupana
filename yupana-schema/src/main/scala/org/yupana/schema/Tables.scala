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

import org.yupana.api.schema.{Dimension, ExternalLink, Table}
import org.yupana.schema.externallinks._

object Tables {

  val itemExternalLinks: Seq[ExternalLink] = Seq(ItemsInvertedIndex, RelatedItemsCatalog)

  val itemsKkmTable = new Table(
    name = "items_kkm",
    rowTimeSpan = 86400000L * 30L,
    dimensionSeq = Seq(Dimensions.ITEM_TAG, Dimensions.KKM_ID_TAG, Dimensions.OPERATION_TYPE_TAG, Dimensions.POSITION_TAG),
    metrics = ItemTableMetrics.metrics,
    externalLinks = itemExternalLinks
  )

  val kkmItemsTable = new Table(
    name = "kkm_items",
    rowTimeSpan = 86400000L * 30L,
    dimensionSeq = Seq(Dimensions.KKM_ID_TAG, Dimensions.ITEM_TAG, Dimensions.OPERATION_TYPE_TAG, Dimensions.POSITION_TAG),
    metrics = ItemTableMetrics.metrics,
    externalLinks = itemExternalLinks
  )

  import ReceiptTableMetrics._

  val receiptDimensionSeq: Seq[Dimension] = Seq(Dimensions.KKM_ID_TAG, Dimensions.OPERATION_TYPE_TAG, Dimensions.SHIFT_TAG, Dimensions.OPERATOR_TAG)
  val receiptExternalLinks: Seq[ExternalLink] = Seq()

  val receiptTable = new Table(
    name = "receipt",
    rowTimeSpan = 86400000L,
    dimensionSeq = receiptDimensionSeq,
    metrics = baseFields ++ Seq(
      correctionBasis,
      correctionDocumentNumber,
      correctionDocumentDateTime,
      taxationType,
      acceptedAt
    ),
    externalLinks = receiptExternalLinks
  )

  val receiptByDayTable = new Table(
    name = "receipt_by_day",
    rowTimeSpan = 86400000L * 30,
    dimensionSeq = receiptDimensionSeq,
    metrics = baseFields ++ rollupFields,
    externalLinks = receiptExternalLinks
  )

  val receiptByWeekTable = new Table(
    name = "receipt_by_week",
    rowTimeSpan = 86400000L * 30,
    dimensionSeq = Seq(Dimensions.KKM_ID_TAG, Dimensions.OPERATION_TYPE_TAG),
    metrics = baseFields ++ rollupFields,
    externalLinks = receiptExternalLinks
  )

  val receiptByMonthTable = new Table(
    name = "receipt_by_month",
    rowTimeSpan = 86400000L * 30 * 12,
    dimensionSeq = Seq(Dimensions.KKM_ID_TAG, Dimensions.OPERATION_TYPE_TAG),
    metrics = baseFields ++ rollupFields,
    externalLinks = receiptExternalLinks
  )

  val receiptByDayAllKkmsTable = new Table(
    name = "receipt_by_day_all_kkms",
    rowTimeSpan = 86400000L * 30,
    dimensionSeq = Seq.empty,
    metrics = kkmDistinctCountField +: rollupFields,
    externalLinks = receiptExternalLinks
  )
}

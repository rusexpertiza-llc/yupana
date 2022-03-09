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

import org.yupana.api.schema.{ Dimension, ExternalLink, Table }
import org.yupana.schema.externallinks._

import java.time.{ LocalDateTime, ZoneOffset }

object Tables {

  val epochTime: Long = LocalDateTime.of(2016, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli

  val itemExternalLinks: Seq[ExternalLink] = Seq(ItemsInvertedIndex, RelatedItemsCatalog)
  val itemRollupsExternalLinks: Seq[ExternalLink] = itemExternalLinks.filterNot(_ == RelatedItemsCatalog)

  val itemsKkmTable = new Table(
    name = "items_kkm",
    rowTimeSpan = 86400000L * 30L,
    dimensionSeq = Seq(Dimensions.ITEM, Dimensions.KKM_ID, Dimensions.OPERATION_TYPE, Dimensions.POSITION),
    metrics = ItemTableMetrics.metrics,
    externalLinks = itemExternalLinks,
    epochTime
  )

  val kkmItemsTable = new Table(
    name = "kkm_items",
    rowTimeSpan = 86400000L * 30L,
    dimensionSeq = Seq(Dimensions.KKM_ID, Dimensions.ITEM, Dimensions.OPERATION_TYPE, Dimensions.POSITION),
    metrics = ItemTableMetrics.metrics,
    externalLinks = itemExternalLinks,
    epochTime
  )

  import ReceiptTableMetrics._

  val receiptDimensionSeq: Seq[Dimension] =
    Seq(Dimensions.KKM_ID, Dimensions.OPERATION_TYPE, Dimensions.SHIFT)
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
      acceptedAt,
      documentNumberField,
      operator
    ),
    externalLinks = receiptExternalLinks,
    epochTime
  )

  val receiptByDayTable = new Table(
    name = "receipt_by_day",
    rowTimeSpan = 86400000L * 30,
    dimensionSeq = receiptDimensionSeq,
    metrics = baseFields ++ rollupFields,
    externalLinks = receiptExternalLinks,
    epochTime
  )

  val receiptByWeekTable = new Table(
    name = "receipt_by_week",
    rowTimeSpan = 86400000L * 30,
    dimensionSeq = Seq(Dimensions.KKM_ID, Dimensions.OPERATION_TYPE),
    metrics = baseFields ++ rollupFields,
    externalLinks = receiptExternalLinks,
    epochTime
  )

  val receiptByMonthTable = new Table(
    name = "receipt_by_month",
    rowTimeSpan = 86400000L * 30 * 12,
    dimensionSeq = Seq(Dimensions.KKM_ID, Dimensions.OPERATION_TYPE),
    metrics = baseFields ++ rollupFields,
    externalLinks = receiptExternalLinks,
    epochTime
  )

  val receiptByDayAllKkmsTable = new Table(
    name = "receipt_by_day_all_kkms",
    rowTimeSpan = 86400000L * 30,
    dimensionSeq = Seq.empty,
    metrics = summaryFields ++ rollupFields,
    externalLinks = receiptExternalLinks,
    epochTime
  )

  val itemByDayTable = new Table(
    name = "item_by_day",
    rowTimeSpan = 86400000L * 30,
    dimensionSeq = Seq(Dimensions.ITEM, Dimensions.OPERATION_TYPE),
    metrics = Seq(ItemTableMetrics.sumField, ItemTableMetrics.quantityField, ItemTableMetrics.itemCountField),
    externalLinks = itemRollupsExternalLinks,
    epochTime
  )

  val itemByWeekTable = new Table(
    name = "item_by_week",
    rowTimeSpan = 86400000L * 30,
    dimensionSeq = Seq(Dimensions.ITEM, Dimensions.OPERATION_TYPE),
    metrics = Seq(ItemTableMetrics.sumField, ItemTableMetrics.quantityField, ItemTableMetrics.itemCountField),
    externalLinks = itemRollupsExternalLinks,
    epochTime
  )

  val itemByMonthTable = new Table(
    name = "item_by_month",
    rowTimeSpan = 86400000L * 30 * 12,
    dimensionSeq = Seq(Dimensions.ITEM, Dimensions.OPERATION_TYPE),
    metrics = Seq(ItemTableMetrics.sumField, ItemTableMetrics.quantityField, ItemTableMetrics.itemCountField),
    externalLinks = itemRollupsExternalLinks,
    epochTime
  )

  val itemKkmsByDayTable = new Table(
    name = "item_kkms_by_day",
    rowTimeSpan = 86400000L * 30,
    dimensionSeq = Seq(Dimensions.ITEM, Dimensions.KKM_ID, Dimensions.OPERATION_TYPE),
    metrics = Seq(ItemTableMetrics.sumField, ItemTableMetrics.quantityField, ItemTableMetrics.itemCountField),
    externalLinks = itemRollupsExternalLinks,
    epochTime
  )

  val itemKkmsByWeekTable = new Table(
    name = "item_kkms_by_week",
    rowTimeSpan = 86400000L * 30,
    dimensionSeq = Seq(Dimensions.ITEM, Dimensions.KKM_ID, Dimensions.OPERATION_TYPE),
    metrics = Seq(ItemTableMetrics.sumField, ItemTableMetrics.quantityField, ItemTableMetrics.itemCountField),
    externalLinks = itemRollupsExternalLinks,
    epochTime
  )

  val itemKkmsByMonthTable = new Table(
    name = "item_kkms_by_month",
    rowTimeSpan = 86400000L * 30 * 12,
    dimensionSeq = Seq(Dimensions.ITEM, Dimensions.KKM_ID, Dimensions.OPERATION_TYPE),
    metrics = Seq(ItemTableMetrics.sumField, ItemTableMetrics.quantityField, ItemTableMetrics.itemCountField),
    externalLinks = itemRollupsExternalLinks,
    epochTime
  )

  val kkmsItemByDayTable = new Table(
    name = "kkms_item_by_day",
    rowTimeSpan = 86400000L * 30,
    dimensionSeq = Seq(Dimensions.KKM_ID, Dimensions.ITEM, Dimensions.OPERATION_TYPE),
    metrics = Seq(ItemTableMetrics.sumField, ItemTableMetrics.quantityField, ItemTableMetrics.itemCountField),
    externalLinks = itemRollupsExternalLinks,
    epochTime
  )

  val kkmsItemByWeekTable = new Table(
    name = "kkms_item_by_week",
    rowTimeSpan = 86400000L * 30,
    dimensionSeq = Seq(Dimensions.KKM_ID, Dimensions.ITEM, Dimensions.OPERATION_TYPE),
    metrics = Seq(ItemTableMetrics.sumField, ItemTableMetrics.quantityField, ItemTableMetrics.itemCountField),
    externalLinks = itemRollupsExternalLinks,
    epochTime
  )

  val kkmsItemByMonthTable = new Table(
    name = "kkms_item_by_month",
    rowTimeSpan = 86400000L * 30 * 12,
    dimensionSeq = Seq(Dimensions.KKM_ID, Dimensions.ITEM, Dimensions.OPERATION_TYPE),
    metrics = Seq(ItemTableMetrics.sumField, ItemTableMetrics.quantityField, ItemTableMetrics.itemCountField),
    externalLinks = itemRollupsExternalLinks,
    epochTime
  )
}

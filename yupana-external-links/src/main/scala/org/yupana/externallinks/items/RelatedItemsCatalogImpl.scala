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

package org.yupana.externallinks.items

import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.api.schema.Schema
import org.yupana.core.auth.YupanaUser
import org.yupana.core.model.BatchDataset
import org.yupana.core.utils.metric.NoMetricCollector
import org.yupana.core.utils.{ CollectionUtils, FlatAndCondition }
import org.yupana.core.{ ExternalLinkService, TsdbBase }
import org.yupana.externallinks.ExternalLinkUtils
import org.yupana.schema.externallinks.{ ItemsInvertedIndex, RelatedItemsCatalog }
import org.yupana.schema.{ Dimensions, Tables }

class RelatedItemsCatalogImpl(tsdb: TsdbBase, override val externalLink: RelatedItemsCatalog)
    extends ExternalLinkService[RelatedItemsCatalog] {

  override val schema: Schema = tsdb.schema

  import org.yupana.api.query.syntax.All._

  private def includeTransform(
      fieldsValues: Seq[(SimpleCondition, String, Set[String])],
      from: Long,
      to: Long,
      user: YupanaUser,
      startTime: Time
  ): Seq[ConditionTransformation] = {
    val info = createFilter(fieldsValues).map(c => getTransactions(c, from, to, user, startTime).toSet)
    val tuples = CollectionUtils.intersectAll(info)
    ConditionTransformation.replace(fieldsValues.map(_._1), in(tuple(time, dimension(Dimensions.KKM_ID)), tuples))
  }

  private def excludeTransform(
      fieldsValues: Seq[(SimpleCondition, String, Set[String])],
      from: Long,
      to: Long,
      user: YupanaUser,
      startTime: Time
  ): Seq[ConditionTransformation] = {
    val info = createFilter(fieldsValues).map(c => getTransactions(c, from, to, user, startTime).toSet)
    val tuples = info.fold(Set.empty)(_ union _)
    ConditionTransformation.replace(fieldsValues.map(_._1), notIn(tuple(time, dimension(Dimensions.KKM_ID)), tuples))
  }

  override def transformCondition(tbc: FlatAndCondition): Seq[ConditionTransformation] = {

    // TODO: Here we can take KKM related conditions from other, to speed up transactions request

    val (includeExprValues, excludeExprValues, _) =
      ExternalLinkUtils.extractCatalogFieldsT[String](tbc, externalLink.linkName)

    val include = if (includeExprValues.nonEmpty) {
      includeTransform(includeExprValues, tbc.from, tbc.to, tbc.user, tbc.startTime)
    } else {
      Seq.empty
    }

    val exclude = if (excludeExprValues.nonEmpty) {
      excludeTransform(excludeExprValues, tbc.from, tbc.to, tbc.user, tbc.startTime)
    } else {
      Seq.empty
    }

    include ++ exclude
  }

  protected def createFilter(fieldValues: Seq[(SimpleCondition, String, Set[String])]): Seq[SimpleCondition] = {
    fieldValues.map { case (_, f, vs) => createFilter(f, vs) }
  }

  protected def createFilter(field: String, values: Set[String]): SimpleCondition = {
    field match {
      case externalLink.ITEM_FIELD   => in(lower(dimension(Dimensions.ITEM)), values)
      case externalLink.PHRASE_FIELD => in(lower(link(ItemsInvertedIndex, ItemsInvertedIndex.PHRASE_FIELD)), values)
      case f                         => throw new IllegalArgumentException(s"Unsupported field $f")
    }
  }

  private def getTransactions(
      filter: SimpleCondition,
      from: Long,
      to: Long,
      user: YupanaUser,
      startTime: Time
  ): Seq[(Time, Int)] = {
    val q = Query(
      table = Tables.itemsKkmTable,
      from = const(Time(from)),
      to = const(Time(to)),
      fields = Seq(dimension(Dimensions.KKM_ID).toField, time.toField),
      filter = filter
    )

    val result = tsdb.query(q, startTime, user)

    val timeIdx = result.queryContext.datasetSchema.exprIndex(time)
    val kkmIdIdx = result.queryContext.datasetSchema.exprIndex(dimension(Dimensions.KKM_ID))

    val extracted = tsdb.mapReduceEngine(NoMetricCollector).map(result.data) { batch =>
      var res = Set.empty[(Time, Dimensions.KKM_ID.T)]
      var i = 0
      while (i < batch.size) {
        if (!batch.isDeleted(i)) {
          val kkmId = batch.get[Dimensions.KKM_ID.T](i, kkmIdIdx)
          val time = batch.get[Time](i, timeIdx)
          res += (time -> kkmId)
        }
        i += 1
      }
      res
    }

    tsdb.mapReduceEngine(NoMetricCollector).fold(extracted)(Set.empty)(_ ++ _).toSeq
  }

  override def setLinkedValues(
      batch: BatchDataset,
      exprs: Set[LinkExpr[_]]
  ): Unit = {}
}

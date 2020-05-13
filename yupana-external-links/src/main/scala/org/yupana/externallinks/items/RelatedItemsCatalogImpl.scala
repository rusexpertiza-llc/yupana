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
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.core.model.InternalRow
import org.yupana.core.utils.metric.NoMetricCollector
import org.yupana.core.utils.{ CollectionUtils, TimeBoundedCondition }
import org.yupana.core.{ ExternalLinkService, TsdbBase }
import org.yupana.externallinks.ExternalLinkUtils
import org.yupana.schema.{ Dimensions, Tables }
import org.yupana.schema.externallinks.{ ItemsInvertedIndex, RelatedItemsCatalog }

class RelatedItemsCatalogImpl(tsdb: TsdbBase, override val externalLink: RelatedItemsCatalog)
    extends ExternalLinkService[RelatedItemsCatalog] {

  import org.yupana.api.query.syntax.All._

  def includeCondition(fieldsValues: Seq[(String, Set[String])], from: Long, to: Long): Condition = {
    val info = createFilter(fieldsValues).map(c => getTransactions(c, from, to).toSet)
    val tuples = CollectionUtils.intersectAll(info)
    in(tuple(time, dimension(Dimensions.KKM_ID)), tuples)
  }

  def excludeCondition(fieldsValues: Seq[(String, Set[String])], from: Long, to: Long): Condition = {
    val info = createFilter(fieldsValues).map(c => getTransactions(c, from, to).toSet)
    val tuples = info.fold(Set.empty)(_ union _)
    notIn(tuple(time, dimension(Dimensions.KKM_ID)), tuples)
  }

  override def condition(condition: Condition): Condition = {
    val tbcs = TimeBoundedCondition(condition)

    val r = tbcs.map { tbc =>
      val from = tbc.from.getOrElse(
        throw new IllegalArgumentException(s"FROM time is not defined for condition ${tbc.toCondition}")
      )
      val to =
        tbc.to.getOrElse(throw new IllegalArgumentException(s"TO time is not defined for condition ${tbc.toCondition}"))

      val (includeValues, excludeValues, other) = ExternalLinkUtils.extractCatalogFields(tbc, externalLink.linkName)

      // TODO: Here we can take KKM related conditions from other, to speed up transactions request

      val include = if (includeValues.nonEmpty) {
        includeCondition(includeValues, from, to)
      } else {
        const(true)
      }

      val exclude = if (excludeValues.nonEmpty) {
        excludeCondition(excludeValues, from, to)
      } else {
        const(true)
      }

      TimeBoundedCondition(tbc.from, tbc.to, include :: exclude :: other)
    }

    TimeBoundedCondition.merge(r).toCondition
  }

  protected def createFilter(fieldValues: Seq[(String, Set[String])]): Seq[Condition] = {
    fieldValues.map { case (f, vs) => createFilter(f, vs) }
  }

  protected def createFilter(field: String, values: Set[String]): Condition = {
    field match {
      case externalLink.ITEM_FIELD    => in(lower(dimension(Dimensions.ITEM)), values)
      case externalLink.PHRASE_FIELDS => in(lower(link(ItemsInvertedIndex, ItemsInvertedIndex.PHRASE_FIELD)), values)
      case f                          => throw new IllegalArgumentException(s"Unsupported field $f")
    }
  }

  private def getTransactions(filter: Condition, from: Long, to: Long): Seq[(Time, Int)] = {
    val q = Query(
      table = Tables.itemsKkmTable,
      from = const(Time(from)),
      to = const(Time(to)),
      fields = Seq(dimension(Dimensions.KKM_ID).toField, time.toField),
      filter = filter
    )

    val result = tsdb.query(q)

    val timeIdx = result.queryContext.exprsIndex(time)
    val kkmIdIdx = result.queryContext.exprsIndex(dimension(Dimensions.KKM_ID))

    val extracted = tsdb.mapReduceEngine(NoMetricCollector).flatMap(result.rows) { a =>
      for {
        kkmId <- a(kkmIdIdx)
        time <- a(timeIdx)
      } yield Set((time.asInstanceOf[Time], kkmId.asInstanceOf[Int]))
    }

    tsdb.mapReduceEngine(NoMetricCollector).fold(extracted)(Set.empty)(_ ++ _).toSeq
  }

  override def setLinkedValues(
      exprIndex: scala.collection.Map[Expression, Int],
      valueData: Seq[InternalRow],
      exprs: Set[LinkExpr]
  ): Unit = {
    // may be throw exception here?
  }
}

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
import org.yupana.api.schema.Schema
import org.yupana.core.model.InternalRow
import org.yupana.core.utils.metric.NoMetricCollector
import org.yupana.core.utils.{ CollectionUtils, TimeBoundedCondition }
import org.yupana.core.{ ExternalLinkService, TsdbBase }
import org.yupana.externallinks.ExternalLinkUtils
import org.yupana.schema.externallinks.{ ItemsInvertedIndex, RelatedItemsCatalog }
import org.yupana.schema.{ Dimensions, Tables }

class RelatedItemsCatalogImpl(tsdb: TsdbBase, override val externalLink: RelatedItemsCatalog)
    extends ExternalLinkService[RelatedItemsCatalog] {

  override val schema: Schema = tsdb.schema

  import org.yupana.api.query.syntax.All._

  def includeTransform(
      fieldsValues: Seq[(Condition, String, Set[String])],
      from: Long,
      to: Long
  ): TransformCondition = {
    val info = createFilter(fieldsValues).map(c => getTransactions(c, from, to).toSet)
    val tuples = CollectionUtils.intersectAll(info)
    Replace(
      fieldsValues.map(_._1).toSet,
      in(tuple(time, dimension(Dimensions.KKM_ID)), tuples)
    )
  }

  def excludeTransform(
      fieldsValues: Seq[(Condition, String, Set[String])],
      from: Long,
      to: Long
  ): TransformCondition = {
    val info = createFilter(fieldsValues).map(c => getTransactions(c, from, to).toSet)
    val tuples = info.fold(Set.empty)(_ union _)
    Replace(
      fieldsValues.map(_._1).toSet,
      notIn(tuple(time, dimension(Dimensions.KKM_ID)), tuples)
    )
  }

  override def transformCondition(tbc: TimeBoundedCondition): Seq[TransformCondition] = {

    val from = tbc.from.getOrElse(
      throw new IllegalArgumentException(s"FROM time is not defined for condition ${tbc.toCondition}")
    )
    val to =
      tbc.to.getOrElse(throw new IllegalArgumentException(s"TO time is not defined for condition ${tbc.toCondition}"))

    // TODO: Here we can take KKM related conditions from other, to speed up transactions request

    val (includeExprValues, excludeExprValues, other) =
      ExternalLinkUtils.extractCatalogFieldsT[String](tbc, externalLink.linkName)

    val include = if (includeExprValues.nonEmpty) {
      Some(includeTransform(includeExprValues, from, to))
    } else {
      None
    }

    val exclude = if (excludeExprValues.nonEmpty) {
      Some(excludeTransform(excludeExprValues, from, to))
    } else {
      None
    }

    val result =
      if (other.nonEmpty)
        Seq(include, exclude, Some(Original(other.toSet))).flatten
      else
        Seq(include, exclude).flatten

    result
  }

  protected def createFilter(fieldValues: Seq[(Condition, String, Set[String])]): Seq[Condition] = {
    fieldValues.map { case (_, f, vs) => createFilter(f, vs) }
  }

  protected def createFilter(field: String, values: Set[String]): Condition = {
    field match {
      case externalLink.ITEM_FIELD   => in(lower(dimension(Dimensions.ITEM)), values)
      case externalLink.PHRASE_FIELD => in(lower(link(ItemsInvertedIndex, ItemsInvertedIndex.PHRASE_FIELD)), values)
      case f                         => throw new IllegalArgumentException(s"Unsupported field $f")
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

    val extracted = tsdb.mapReduceEngine(NoMetricCollector).map(result.rows) { a =>
      val kkmId = a(kkmIdIdx)
      val time = a(timeIdx)
      Set((time.asInstanceOf[Time], kkmId.asInstanceOf[Int]))
    }

    tsdb.mapReduceEngine(NoMetricCollector).fold(extracted)(Set.empty)(_ ++ _).toSeq
  }

  override def setLinkedValues(
      exprIndex: scala.collection.Map[Expression[_], Int],
      valueData: Seq[InternalRow],
      exprs: Set[LinkExpr[_]]
  ): Unit = {
    // may be throw exception here?
  }
}

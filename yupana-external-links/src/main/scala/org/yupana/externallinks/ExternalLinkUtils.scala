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

package org.yupana.externallinks

import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.schema.ExternalLink
import org.yupana.api.types.{ ID, InternalStorable }
import org.yupana.api.utils.ConditionMatchers._
import org.yupana.core.model.{ BatchDataset, TimeSensitiveFieldValues }
import org.yupana.core.utils.{ CollectionUtils, FlatAndCondition, Table }

import scala.collection.mutable

object ExternalLinkUtils {

  /**
    * Extracts external link fields from time bounded condition
    * @note this function doesn't care if the field condition case sensitive or not
    *
    * @param simpleCondition condition to extract values from
    * @param linkName the external link name.
    * @return list of the fields and field values to be included, list of fields and field values to be excluded and
    *         unmatched part of the condition.
    */
  def extractCatalogFields(
      simpleCondition: FlatAndCondition,
      linkName: String
  ): (List[(SimpleCondition, String, Set[Any])], List[(SimpleCondition, String, Set[Any])], List[SimpleCondition]) = {
    simpleCondition.conditions.foldLeft(
      (
        List.empty[(SimpleCondition, String, Set[Any])],
        List.empty[(SimpleCondition, String, Set[Any])],
        List.empty[SimpleCondition]
      )
    ) {
      case ((cat, neg, oth), cond) =>
        cond match {
          case EqExpr(LinkExpr(c, field), ConstantExpr(v, _)) if c.linkName == linkName =>
            ((cond, field.name, Set[Any](v)) :: cat, neg, oth)

          case EqExpr(ConstantExpr(v, _), LinkExpr(c, field)) if c.linkName == linkName =>
            ((cond, field.name, Set[Any](v)) :: cat, neg, oth)

          case InExpr(LinkExpr(c, field), cs) if c.linkName == linkName =>
            ((cond, field.name, cs) :: cat, neg, oth)

          case NeqExpr(LinkExpr(c, field), ConstantExpr(v, _)) if c.linkName == linkName =>
            (cat, (cond, field.name, Set[Any](v)) :: neg, oth)

          case NeqExpr(ConstantExpr(v, _), LinkExpr(c, field)) if c.linkName == linkName =>
            (cat, (cond, field.name, Set[Any](v)) :: neg, oth)

          case NotInExpr(LinkExpr(c, field), cs) if c.linkName == linkName =>
            (cat, (cond, field.name, cs) :: neg, oth)

          case EqString(LowerExpr(LinkExpr(c, field)), ConstantExpr(v, _)) if c.linkName == linkName =>
            ((cond, field.name, Set[Any](v)) :: cat, neg, oth)

          case EqString(ConstantExpr(v, _), LowerExpr(LinkExpr(c, field))) if c.linkName == linkName =>
            ((cond, field.name, Set[Any](v)) :: cat, neg, oth)

          case InString(LowerExpr(LinkExpr(c, field)), cs) if c.linkName == linkName =>
            ((cond, field.name, cs.asInstanceOf[Set[Any]]) :: cat, neg, oth)

          case NeqString(LowerExpr(LinkExpr(c, field)), ConstantExpr(v, _)) if c.linkName == linkName =>
            (cat, (cond, field.name, Set[Any](v)) :: neg, oth)

          case NeqString(ConstantExpr(v, _), LowerExpr(LinkExpr(c, field))) if c.linkName == linkName =>
            (cat, (cond, field.name, Set[Any](v)) :: neg, oth)

          case NotInString(LowerExpr(LinkExpr(c, field)), cs) if c.linkName == linkName =>
            (cat, (cond, field.name, cs.asInstanceOf[Set[Any]]) :: neg, oth)

          case _ => (cat, neg, cond :: oth)
        }
    }
  }

  def extractCatalogFieldsT[T](
      simpleCondition: FlatAndCondition,
      linkName: String
  ): (List[(SimpleCondition, String, Set[T])], List[(SimpleCondition, String, Set[T])], List[SimpleCondition]) = {
    val (inc, exc, cond) = extractCatalogFields(simpleCondition, linkName)
    (
      inc.map { case (e, n, vs) => (e, n, vs.asInstanceOf[Set[T]]) },
      exc.map {
        case (e, n, vs) => (e, n, vs.asInstanceOf[Set[T]])
      },
      cond
    )
  }

  def transformConditionT[T](
      linkName: String,
      tbc: FlatAndCondition,
      includeExpression: Seq[(SimpleCondition, String, Set[T])] => Seq[ConditionTransformation],
      excludeExpression: Seq[(SimpleCondition, String, Set[T])] => Seq[ConditionTransformation]
  ): Seq[ConditionTransformation] = {
    transformCondition(
      linkName,
      tbc,
      { metricsWithValues: Seq[(SimpleCondition, String, Set[Any])] =>
        includeExpression(metricsWithValues.map { case (e, n, vs) => (e, n, vs.asInstanceOf[Set[T]]) })
      },
      { metricsWithValues: Seq[(SimpleCondition, String, Set[Any])] =>
        excludeExpression(metricsWithValues.map { case (e, n, vs) => (e, n, vs.asInstanceOf[Set[T]]) })
      }
    )
  }

  def transformCondition(
      linkName: String,
      tbc: FlatAndCondition,
      includeTransform: Seq[(SimpleCondition, String, Set[Any])] => Seq[ConditionTransformation],
      excludeTransform: Seq[(SimpleCondition, String, Set[Any])] => Seq[ConditionTransformation]
  ): Seq[ConditionTransformation] = {
    val (includeExprValues, excludeExprValues, _) = extractCatalogFields(tbc, linkName)

    val include = if (includeExprValues.nonEmpty) {
      includeTransform(includeExprValues)
    } else {
      Seq.empty
    }

    val exclude = if (excludeExprValues.nonEmpty) {
      excludeTransform(excludeExprValues)
    } else {
      Seq.empty
    }

    include ++ exclude
  }

  def setLinkedValues[R](
      externalLink: ExternalLink.Aux[R],
      batch: BatchDataset,
      linkExprs: Set[LinkExpr[_]],
      fieldValuesForDimValues: (Set[String], Set[R]) => Table[R, String, Any]
  ): Unit = {

    val dimExprIdx = batch.schema.fieldIndex(DimensionExpr(externalLink.dimension))
    val fields = linkExprs.map(_.linkField.name)

    val dimValues = mutable.Set.empty[R]
    batch.foreach { rowNum =>
      val v = batch.get(rowNum, dimExprIdx)(externalLink.dimension.dataType.internalStorable)
      dimValues.add(v)
    }

    val allFieldsValues = fieldValuesForDimValues(fields, dimValues.toSet)
    val linkExprsIdx = linkExprs.toSeq.map(e => e -> batch.schema.fieldIndex(e))

    batch.foreach { rowNum =>
      val dimValue = batch.get[R](rowNum, dimExprIdx)(externalLink.dimension.dataType.internalStorable)
      val values = allFieldsValues.row(dimValue)
      setValues(linkExprsIdx, batch, rowNum, values)
    }
  }

  def setLinkedValuesTimeSensitive[R](
      externalLink: ExternalLink.Aux[R],
      batch: BatchDataset,
      linkExprs: Set[LinkExpr[_]],
      fieldValuesForDimValues: (Set[String], Set[R], Time, Time) => Map[R, Array[TimeSensitiveFieldValues]]
  ): Unit = {

    val dimExpr = DimensionExpr(externalLink.dimension.aux)
    val fields = linkExprs.map(_.linkField.name)
    val dimIdIdx = batch.schema.fieldIndex(dimExpr)
    val timeIdx = batch.schema.fieldIndex(TimeExpr)

    implicit val internalStorable: InternalStorable[R] = externalLink.dimension.dataType.internalStorable

    def findFieldValuesByTime(
        allFieldsValues: Map[R, Array[TimeSensitiveFieldValues]],
        dimId: R,
        time: Time
    ): Map[String, Any] = {
      if (allFieldsValues.contains(dimId)) {
        val timeSensitiveFieldValues = allFieldsValues(dimId)
        var i = 0
        if (timeSensitiveFieldValues.length > 1) {
          if (timeSensitiveFieldValues.last.time < time) {
            i = timeSensitiveFieldValues.length - 1
          } else if (timeSensitiveFieldValues(0).time < time) {
            var found = false
            while (!found && i < timeSensitiveFieldValues.length - 1) {
              if (timeSensitiveFieldValues(i).time <= time && timeSensitiveFieldValues(i + 1).time > time) {
                found = true
              } else {
                i += 1
              }
            }
          }
        }
        timeSensitiveFieldValues(i).fieldValues
      } else {
        Map.empty
      }
    }

    def getDimValuesAndPeriod: (Set[R], Time, Time) = {
      val dimValues = mutable.Set.empty[R]
      var from = Time(Long.MaxValue)
      var to = Time(Long.MinValue)
      batch.foreach { rowNum =>
        val dimId = batch.get[R](rowNum, dimIdIdx)
        val time = batch.get[Time](rowNum, timeIdx)
        dimValues += dimId
        if (time < from) {
          from = time
        }
        if (time > to) {
          to = time
        }
      }
      (dimValues.toSet, from, to)
    }

    val (dimValues, from, to) = getDimValuesAndPeriod

    val allFieldsValues = fieldValuesForDimValues(fields, dimValues, from, to)
    val linkExprsIdx = linkExprs.toSeq.map(e => e -> batch.schema.fieldIndex(e))

    batch.foreach { rowNum =>
      val dimId = batch.get[R](rowNum, dimIdIdx)
      val time = batch.get[Time](rowNum, timeIdx)
      val values = findFieldValuesByTime(allFieldsValues, dimId, time)
      setValues(linkExprsIdx, batch, rowNum, values)
    }
  }

  private def setValues(
      exprIndex: Seq[(LinkExpr[_], Int)],
      batch: BatchDataset,
      rowNum: Int,
      values: Map[String, Any]
  ): Unit = {
    exprIndex.foreach {
      case (expr, idx) =>
        values.get(expr.linkField.name).foreach { value =>
          if (value != null) {
            val v = value.asInstanceOf[ID[expr.dataType.T]]
            batch.set(rowNum, idx, v)(expr.dataType.internalStorable)
          }
        }
    }
  }

  def crossJoinFieldValues[T](fieldsValues: Seq[(Condition, String, Set[T])]): List[Map[String, T]] = {
    val flatValues = fieldsValues
      .groupBy(_._2)
      .map {
        case (k, vs) =>
          CollectionUtils.intersectAll(vs.map(_._3)).toList.map(k -> _)
      }
      .toList

    CollectionUtils.crossJoin(flatValues).map(_.toMap)
  }
}

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
import org.yupana.api.utils.ConditionMatchers._
import org.yupana.core.model.{ InternalRow, TimeSensitiveFieldValues }
import org.yupana.core.utils.{ CollectionUtils, Table, TimeBoundedCondition }

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
      simpleCondition: TimeBoundedCondition,
      linkName: String
  ): (List[(Condition, String, Set[Any])], List[(Condition, String, Set[Any])], List[Condition]) = {
    simpleCondition.conditions.foldLeft(
      (List.empty[(Condition, String, Set[Any])], List.empty[(Condition, String, Set[Any])], List.empty[Condition])
    ) {
      case ((cat, neg, oth), cond) =>
        cond match {
          case EqExpr(LinkExpr(c, field), ConstantExpr(v, _)) if c.linkName == linkName =>
            ((cond, field.name, Set[Any](v)) :: cat, neg, oth)

          case EqExpr(ConstantExpr(v, _), LinkExpr(c, field)) if c.linkName == linkName =>
            ((cond, field.name, Set[Any](v)) :: cat, neg, oth)

          case InExpr(LinkExpr(c, field), cs) if c.linkName == linkName =>
            ((cond, field.name, cs.asInstanceOf[Set[Any]]) :: cat, neg, oth)

          case NeqExpr(LinkExpr(c, field), ConstantExpr(v, _)) if c.linkName == linkName =>
            (cat, (cond, field.name, Set[Any](v)) :: neg, oth)

          case NeqExpr(ConstantExpr(v, _), LinkExpr(c, field)) if c.linkName == linkName =>
            (cat, (cond, field.name, Set[Any](v)) :: neg, oth)

          case NotInExpr(LinkExpr(c, field), cs) if c.linkName == linkName =>
            (cat, (cond, field.name, cs.asInstanceOf[Set[Any]]) :: neg, oth)

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
      simpleCondition: TimeBoundedCondition,
      linkName: String
  ): (List[(Condition, String, Set[T])], List[(Condition, String, Set[T])], List[Condition]) = {
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
      tbc: TimeBoundedCondition,
      includeExpression: Seq[(Condition, String, Set[T])] => TransformCondition,
      excludeExpression: Seq[(Condition, String, Set[T])] => TransformCondition
  ): Seq[TransformCondition] = {
    transformCondition(
      linkName,
      tbc,
      { metricsWithValues: Seq[(Condition, String, Set[Any])] =>
        includeExpression(metricsWithValues.map { case (e, n, vs) => (e, n, vs.asInstanceOf[Set[T]]) })
      },
      { metricsWithValues: Seq[(Condition, String, Set[Any])] =>
        excludeExpression(metricsWithValues.map { case (e, n, vs) => (e, n, vs.asInstanceOf[Set[T]]) })
      }
    )
  }

  def transformCondition(
      linkName: String,
      tbc: TimeBoundedCondition,
      includeTransform: Seq[(Condition, String, Set[Any])] => TransformCondition,
      excludeTransform: Seq[(Condition, String, Set[Any])] => TransformCondition
  ): Seq[TransformCondition] = {
    val (includeExprValues, excludeExprValues, other) = extractCatalogFields(tbc, linkName)

    val include = if (includeExprValues.nonEmpty) {
      Some(includeTransform(includeExprValues))
    } else {
      None
    }

    val exclude = if (excludeExprValues.nonEmpty) {
      Some(excludeTransform(excludeExprValues))
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

  def setLinkedValues[R](
      externalLink: ExternalLink.Aux[R],
      exprIndex: scala.collection.Map[Expression[_], Int],
      rows: Seq[InternalRow],
      linkExprs: Set[LinkExpr[_]],
      fieldValuesForDimValues: (Set[String], Set[R]) => Table[R, String, Any]
  ): Unit = {
    val dimExprIdx = exprIndex(DimensionExpr(externalLink.dimension))
    val fields = linkExprs.map(_.linkField.name)
    val dimValues = rows.map(r => r.get[R](dimExprIdx)).toSet
    val allFieldsValues = fieldValuesForDimValues(fields, dimValues)
    val linkExprsIdx = linkExprs.toSeq.map(e => e -> exprIndex(e))
    rows.foreach { row =>
      val dimValue = row.get[R](dimExprIdx)
      val rowValues = allFieldsValues.row(dimValue)
      updateRow(row, linkExprsIdx, rowValues)
    }
  }

  def setLinkedValuesTimeSensitive[R](
      externalLink: ExternalLink.Aux[R],
      exprIndex: scala.collection.Map[Expression[_], Int],
      rows: Seq[InternalRow],
      linkExprs: Set[LinkExpr[_]],
      fieldValuesForDimValues: (Set[String], Set[R], Time, Time) => Map[R, Array[TimeSensitiveFieldValues]]
  ): Unit = {
    val dimExpr = DimensionExpr(externalLink.dimension.aux)
    val fields = linkExprs.map(_.linkField.name)
    val dimIdIdx = exprIndex(dimExpr)
    val timeIdx = exprIndex(TimeExpr)

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
      rows.foreach { row =>
        val dimId = row.get[R](dimIdIdx)
        val time = row.get[Time](timeIdx)
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
    val linkExprsIdx = linkExprs.toSeq.map(e => e -> exprIndex(e))

    rows.foreach { row =>
      val dimId = row.get[R](dimIdIdx)
      val time = row.get[Time](timeIdx)
      val values = findFieldValuesByTime(allFieldsValues, dimId, time)
      updateRow(row, linkExprsIdx, values)
    }
  }

  private def updateRow(row: InternalRow, exprIndex: Seq[(LinkExpr[_], Int)], values: Map[String, Any]): Unit = {
    exprIndex.foreach {
      case (expr, idx) =>
        values.get(expr.linkField.name).foreach { value =>
          if (value != null) {
            row.set(idx, value)
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

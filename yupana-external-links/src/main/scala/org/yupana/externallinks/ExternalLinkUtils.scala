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
import org.yupana.core.ConstantCalculator
import org.yupana.core.model.InternalRow
import org.yupana.api.utils.ConditionMatchers._
import org.yupana.core.utils.{ CollectionUtils, Table, TimeBoundedCondition }

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
  ): (List[(String, Set[Any])], List[(String, Set[Any])], List[Condition]) = {
    simpleCondition.conditions.foldLeft(
      (List.empty[(String, Set[Any])], List.empty[(String, Set[Any])], List.empty[Condition])
    ) {
      case ((cat, neg, oth), cond) =>
        cond match {
          case EqExpr(LinkExpr(c, field), ConstantExpr(v)) if c.linkName == linkName =>
            ((field.name, Set[Any](v)) :: cat, neg, oth)

          case EqExpr(ConstantExpr(v), LinkExpr(c, field)) if c.linkName == linkName =>
            ((field.name, Set[Any](v)) :: cat, neg, oth)

          case InExpr(LinkExpr(c, field), cs) if c.linkName == linkName =>
            ((field.name, cs.asInstanceOf[Set[Any]]) :: cat, neg, oth)

          case NeqExpr(LinkExpr(c, field), ConstantExpr(v)) if c.linkName == linkName =>
            (cat, (field.name, Set[Any](v)) :: neg, oth)

          case NeqExpr(ConstantExpr(v), LinkExpr(c, field)) if c.linkName == linkName =>
            (cat, (field.name, Set[Any](v)) :: neg, oth)

          case NotInExpr(LinkExpr(c, field), cs) if c.linkName == linkName =>
            (cat, (field.name, cs.asInstanceOf[Set[Any]]) :: neg, oth)

          case EqString(LowerExpr(LinkExpr(c, field)), ConstantExpr(v)) if c.linkName == linkName =>
            ((field.name, Set[Any](v)) :: cat, neg, oth)

          case EqString(ConstantExpr(v), LowerExpr(LinkExpr(c, field))) if c.linkName == linkName =>
            ((field.name, Set[Any](v)) :: cat, neg, oth)

          case InString(LowerExpr(LinkExpr(c, field)), cs) if c.linkName == linkName =>
            ((field.name, cs.asInstanceOf[Set[Any]]) :: cat, neg, oth)

          case NeqString(LowerExpr(LinkExpr(c, field)), ConstantExpr(v)) if c.linkName == linkName =>
            (cat, (field.name, Set[Any](v)) :: neg, oth)

          case NeqString(ConstantExpr(v), LowerExpr(LinkExpr(c, field))) if c.linkName == linkName =>
            (cat, (field.name, Set[Any](v)) :: neg, oth)

          case NotInString(LowerExpr(LinkExpr(c, field)), cs) if c.linkName == linkName =>
            (cat, (field.name, cs.asInstanceOf[Set[Any]]) :: neg, oth)

          case _ => (cat, neg, cond :: oth)
        }
    }
  }

  def extractCatalogFieldsT[T](
      simpleCondition: TimeBoundedCondition,
      linkName: String
  ): (List[(String, Set[T])], List[(String, Set[T])], List[Condition]) = {
    val (inc, exc, cond) = extractCatalogFields(simpleCondition, linkName)
    (inc.map { case (n, vs) => (n, vs.asInstanceOf[Set[T]]) }, exc.map { case (n, vs) => (n, vs.asInstanceOf[Set[T]]) }, cond)
  }

  def transformConditionT[T](
      expressionCalculator: ConstantCalculator,
      linkName: String,
      condition: Condition,
      includeCondition: Seq[(String, Set[T])] => Condition,
      excludeCondition: Seq[(String, Set[T])] => Condition
  ): Condition = {
    transformCondition(
      expressionCalculator,
      linkName,
      condition, { metricsWithValues: Seq[(String, Set[Any])] =>
        includeCondition(metricsWithValues.map { case (n, vs) => (n, vs.asInstanceOf[Set[T]]) })
      }, { metricsWithValues: Seq[(String, Set[Any])] =>
        excludeCondition(metricsWithValues.map { case (n, vs) => (n, vs.asInstanceOf[Set[T]]) })
      }
    )
  }

  def transformCondition(
      expressionCalculator: ConstantCalculator,
      linkName: String,
      condition: Condition,
      includeCondition: Seq[(String, Set[Any])] => Condition,
      excludeCondition: Seq[(String, Set[Any])] => Condition
  ): Condition = {
    val tbcs = TimeBoundedCondition(expressionCalculator, condition)

    val r = tbcs.map { tbc =>
      val (includeValues, excludeValues, other) = extractCatalogFields(tbc, linkName)

      val include = if (includeValues.nonEmpty) {
        includeCondition(includeValues)
      } else {
        ConstantExpr(true)
      }

      val exclude = if (excludeValues.nonEmpty) {
        excludeCondition(excludeValues)
      } else {
        ConstantExpr(true)
      }

      TimeBoundedCondition(tbc.from, tbc.to, include :: exclude :: other)
    }

    TimeBoundedCondition.merge(r).toCondition
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
      fieldValuesForDimValuesAndTimes: (Set[String], Set[(R, Time)]) => Table[(R, Time), String, Any]
  ): Unit = {
    val dimExpr = DimensionExpr(externalLink.dimension.aux)
    val fields = linkExprs.map(_.linkField.name)

    def extractDimValueWithTime(r: InternalRow): (R, Time) = {
      (r.get[R](exprIndex, dimExpr), r.get[Time](exprIndex, TimeExpr))
    }

    val dimValuesWithTimes = rows.map(extractDimValueWithTime)
    val allFieldsValues = fieldValuesForDimValuesAndTimes(fields, dimValuesWithTimes.toSet)
    val linkExprsIdx = linkExprs.toSeq.map(e => e -> exprIndex(e))

    rows.foreach { row =>
      val dimValueAtTime = extractDimValueWithTime(row)
      val values = allFieldsValues.row(dimValueAtTime)
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

  def crossJoinFieldValues[T](fieldsValues: Seq[(String, Set[T])]): List[Map[String, T]] = {
    val flatValues = fieldsValues
      .groupBy(_._1)
      .map {
        case (k, vs) =>
          CollectionUtils.intersectAll(vs.map(_._2)).toList.map(k -> _)
      }
      .toList

    CollectionUtils.crossJoin(flatValues).map(_.toMap)
  }
}

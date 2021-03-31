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

package org.yupana.externallinks.universal

import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.schema.ExternalLink
import org.yupana.core.ExternalLinkService
import org.yupana.core.model.{ InternalRow, InternalRowBuilder }
import org.yupana.externallinks.ExternalLinkUtils

abstract class InMemoryExternalLinkBase[T <: ExternalLink](orderedFields: Seq[String], data: Array[Array[String]])
    extends ExternalLinkService[T] {

  import org.yupana.api.query.syntax.All._

  def keyIndex: Int

  def fillKeyValues(indexMap: scala.collection.Map[Expression[_], Int], valueData: Seq[InternalRow]): Unit

  def conditionForKeyValues(condition: Condition): Condition

  def keyExpr: Expression[String]

  def validate(): Unit = {
    if (orderedFields.size != externalLink.fields.size)
      throw new IllegalArgumentException(s"orderedFields have to have ${externalLink.fields.size} items")

    orderedFields
      .find(x => !externalLink.fields.map(_.name).contains(x))
      .foreach(x => throw new IllegalArgumentException(s"Unknown field '$x'"))

    if (data.exists(_.length != orderedFields.size))
      throw new IllegalArgumentException(s"Data must have exactly ${orderedFields.size} columns")
  }

  private lazy val fieldIndex: Map[String, Int] = orderedFields.zipWithIndex.toMap

  private lazy val multiIndex: Array[Map[String, Set[Int]]] = {
    val result = Array.fill(externalLink.fields.size)(Map.empty[String, Set[Int]])
    data.indices foreach { idx =>
      val row = data(idx)
      row.indices foreach { col =>
        val value = row(col).toLowerCase
        result(col) += value -> (result(col).getOrElse(value, Set.empty) + idx)
      }
    }

    result
  }

  override def setLinkedValues(
      exprIndex: scala.collection.Map[Expression[_], Int],
      valueData: Seq[InternalRow],
      exprs: Set[LinkExpr[_]]
  ): Unit = {
    val dimExpr = DimensionExpr(externalLink.dimension.aux)
    val indexMap = Seq[Expression[_]](TimeExpr, dimExpr, keyExpr).distinct.zipWithIndex.toMap
    val valueDataBuilder = new InternalRowBuilder(indexMap, None)

    val keyValueData = valueData.map { vd =>
      valueDataBuilder
        .set(dimExpr, vd.get(exprIndex, dimExpr))
        .set(TimeExpr, vd.get[Time](exprIndex, TimeExpr))
        .buildAndReset()
    }

    fillKeyValues(indexMap, keyValueData)

    keyValueData.zip(valueData).foreach {
      case (kvd, vd) =>
        val keyValue = kvd.get[String](indexMap(keyExpr))
        exprs.foreach { expr =>
          vd.set(exprIndex, expr, fieldValueForKeyValue(expr.linkField.name)(keyValue))
        }
    }
  }

  override def condition(condition: Condition): Condition = {
    val keyCondition = ExternalLinkUtils.transformConditionT[String](
      expressionCalculator,
      externalLink.linkName,
      condition,
      includeCondition,
      excludeCondition
    )

    conditionForKeyValues(keyCondition)
  }

  private def includeCondition(values: Seq[(String, Set[String])]): Condition = {
    val keyValues = keyValuesForFieldValues(values, _ intersect _)
    in(lower(keyExpr), keyValues)
  }

  private def excludeCondition(values: Seq[(String, Set[String])]): Condition = {
    val keyValues = keyValuesForFieldValues(values, _ union _)
    notIn(lower(keyExpr), keyValues)
  }

  private def keyValuesForFieldValues(
      fieldValues: Seq[(String, Set[String])],
      reducer: (Set[Int], Set[Int]) => Set[Int]
  ): Set[String] = {
    if (fieldValues.nonEmpty) {
      val rows = fieldValues
        .map {
          case (field, values) =>
            val idx = getFieldIndex(field)
            values.flatMap(value => multiIndex(idx).getOrElse(value, Set.empty))
        }
        .reduceLeft(reducer)

      rows.map(row => data(row)(keyIndex))
    } else Set.empty
  }

  private def fieldValueForKeyValue(fieldName: String)(keyValue: String): String = {
    val idx = getFieldIndex(fieldName)
    val rows = multiIndex(keyIndex).getOrElse(keyValue, Set.empty)
    rows.map(row => data(row)(idx)).headOption.orNull
  }

  private def getFieldIndex(field: String): Int = {
    fieldIndex.getOrElse(
      field,
      throw new IllegalArgumentException(s"Field $field is not defined in catalog ${externalLink.linkName}")
    )
  }
}

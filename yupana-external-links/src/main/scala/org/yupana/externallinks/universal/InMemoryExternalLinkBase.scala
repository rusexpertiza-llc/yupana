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
  def keyIndex: Int

  def fillKeyValues(indexMap: scala.collection.Map[Expression, Int], valueData: Seq[InternalRow]): Unit

  def conditionForKeyValues(condition: Condition): Condition

  def keyExpr: Expression.Aux[String]

  def validate(): Unit = {
    if (orderedFields.size != externalLink.fieldsNames.size)
      throw new IllegalArgumentException(s"orderedFields have to have ${externalLink.fieldsNames.size} items")

    orderedFields
      .find(x => !externalLink.fieldsNames.contains(x))
      .foreach(x => throw new IllegalArgumentException(s"Unknown field '$x'"))

    if (data.exists(_.length != orderedFields.size))
      throw new IllegalArgumentException(s"Data must have exactly ${orderedFields.size} columns")
  }

  private lazy val fieldIndex: Map[String, Int] = orderedFields.zipWithIndex.toMap

  private lazy val multiIndex: Array[Map[String, Set[Int]]] = {
    val result = Array.fill(externalLink.fieldsNames.size)(Map.empty[String, Set[Int]])
    data.indices foreach { idx =>
      val row = data(idx)
      row.indices foreach { col =>
        val value = row(col)
        result(col) += value -> (result(col).getOrElse(value, Set.empty) + idx)
      }
    }

    result
  }

  override def setLinkedValues(
      exprIndex: scala.collection.Map[Expression, Int],
      valueData: Seq[InternalRow],
      exprs: Set[LinkExpr]
  ): Unit = {
    val tagExpr = new DimensionExpr(externalLink.dimension)
    val indexMap = Seq[Expression](TimeExpr, tagExpr, keyExpr).distinct.zipWithIndex.toMap
    val valueDataBuilder = new InternalRowBuilder(indexMap)

    val keyValueData = valueData.map { vd =>
      valueDataBuilder
        .set(tagExpr, vd.get[String](exprIndex, tagExpr))
        .set(TimeExpr, vd.get[Time](exprIndex, TimeExpr))
        .buildAndReset()
    }

    fillKeyValues(indexMap, keyValueData)

    keyValueData.zip(valueData).foreach {
      case (kvd, vd) =>
        kvd.get[String](indexMap(keyExpr)).foreach { keyValue =>
          exprs.foreach { expr =>
            vd.set(exprIndex, expr, fieldValueForKeyValue(expr.linkField)(keyValue))
          }
        }
    }
  }

  override def condition(condition: Condition): Condition = {
    val keyCondition = ExternalLinkUtils.transformCondition(
      externalLink.linkName,
      condition,
      includeCondition,
      excludeCondition
    )

    conditionForKeyValues(keyCondition)
  }

  private def includeCondition(values: Seq[(String, Set[String])]): Condition = {
    val keyValues = keyValuesForFieldValues(values, _ intersect _)
    InExpr(keyExpr, keyValues)
  }

  private def excludeCondition(values: Seq[(String, Set[String])]): Condition = {
    val keyValues = keyValuesForFieldValues(values, _ union _)
    NotInExpr(keyExpr, keyValues)
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

  private def fieldValueForKeyValue(fieldName: String)(keyValue: String): Option[String] = {
    val idx = getFieldIndex(fieldName)
    val rows = multiIndex(keyIndex).getOrElse(keyValue, Set.empty)
    rows.map(row => data(row)(idx)).headOption
  }

  private def getFieldIndex(field: String): Int = {
    fieldIndex.getOrElse(
      field,
      throw new IllegalArgumentException(s"Field $field is not defined in catalog ${externalLink.linkName}")
    )
  }
}

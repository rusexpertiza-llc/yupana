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
import org.yupana.api.query._
import org.yupana.api.schema.ExternalLink
import org.yupana.core.ExternalLinkService
import org.yupana.core.auth.YupanaUser
import org.yupana.core.model.BatchDataset
import org.yupana.core.utils.FlatAndCondition
import org.yupana.externallinks.ExternalLinkUtils

abstract class InMemoryExternalLinkBase[T <: ExternalLink](orderedFields: Seq[String], data: Array[Array[String]])
    extends ExternalLinkService[T] {

  import org.yupana.api.query.syntax.All._

  def keyIndex: Int

  def fillKeyValues(batch: BatchDataset): Unit

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
      batch: BatchDataset,
      exprs: Set[LinkExpr[_]]
  ): Unit = {

    fillKeyValues(batch)

    batch.foreach { rowNum =>
      val keyValue = batch.get[String](rowNum, keyExpr)
      exprs.foreach { expr =>
        val v = fieldValueForKeyValue(expr.linkField.name)(keyValue)
        if (v != null) {
          batch.set(rowNum, expr.asInstanceOf[Expression[String]], v)
        } else {
          batch.setNull(rowNum, expr.asInstanceOf[Expression[String]])
        }
      }
    }
  }

  override def transformCondition(
      condition: FlatAndCondition,
      startTime: Time,
      user: YupanaUser
  ): Seq[ConditionTransformation] = {
    ExternalLinkUtils.transformConditionT[String](
      externalLink.linkName,
      condition,
      includeTransform,
      excludeTransform
    )
  }

  private def includeTransform(values: Seq[(SimpleCondition, String, Set[String])]): Seq[ConditionTransformation] = {
    val keyValues = keyValuesForFieldValues(values, _ intersect _)
    ConditionTransformation.replace(values.map(_._1), in(lower(keyExpr), keyValues))
  }

  private def excludeTransform(values: Seq[(SimpleCondition, String, Set[String])]): Seq[ConditionTransformation] = {
    val keyValues = keyValuesForFieldValues(values, _ union _)
    ConditionTransformation.replace(values.map(_._1), notIn(lower(keyExpr), keyValues))
  }

  private def keyValuesForFieldValues(
      fieldValues: Seq[(SimpleCondition, String, Set[String])],
      reducer: (Set[Int], Set[Int]) => Set[Int]
  ): Set[String] = {
    if (fieldValues.nonEmpty) {
      val rows = fieldValues
        .map {
          case (_, field, values) =>
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

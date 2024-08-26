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

package org.yupana.core.model

import org.yupana.api.query.{ DimensionExpr, DimensionIdExpr, Expression, MetricExpr, TimeExpr }
import org.yupana.api.schema.{ Dimension, Table }

final class DatasetSchema(
    valueExprIndex: Map[Expression[_], Int],
    refExprIndex: Map[Expression[_], Int],
    nameMapping: Map[String, Int],
    table: Option[Table]
) extends Serializable {

  val exprIndex: Map[Expression[_], Int] = refExprIndex ++ valueExprIndex

  val timeFieldIndex: Int = valueExprIndex.getOrElse(TimeExpr, -1)

  val numOfFields: Int =
    Seq(valueExprIndex.values.maxOption, refExprIndex.values.maxOption).flatten.maxOption.map(_ + 1).getOrElse(0)

  val numOfRefFields: Int = refExprIndex.size

  val fixedLengthFieldsBytesSize: Int = getFixedAreaSize(valueExprIndex)

  private val tagIndexMappingArray: Array[Int] = createTagByIndexMappingArray(exprIndex, table)
  private val dimIdMappingArray: Array[Int] = createDimIdMappingArray(exprIndex, table)

  private val refFieldsMapping: Array[Int] = createRefFieldsMappingArray(refExprIndex)

  private val hasFixedFieldMappingArray: Array[Boolean] = createFixedFieldMappingArray(valueExprIndex)

  private val fixedFieldsSizes: Array[Int] = getFixedFieldSizeArray(valueExprIndex)
  private val fieldsOffsets: Array[Int] = getFieldsOffsets(fixedFieldsSizes)

  def fieldOffset(index: Int): Int = {
    fieldsOffsets(index)
  }

  def fieldIndex(expr: Expression[_]): Int = {
    valueExprIndex.getOrElse(expr, refExprIndex(expr))
  }

  def refFieldOrdinal(expr: Expression[_]): Int = {
    refFieldOrdinal(refExprIndex(expr))
  }

  def refFieldOrdinal(fieldIndex: Int): Int = {
    refFieldsMapping(fieldIndex)
  }

  def fieldSize(index: Int): Int = {
    fixedFieldsSizes(index)
  }

  def isFixed(index: Int): Boolean = {
    hasFixedFieldMappingArray(index)
  }

  def isRef(fieldIndex: Int): Boolean = {
    refFieldsMapping(fieldIndex) != -1
  }

  def isValue(fieldIndex: Int): Boolean = {
    fieldSize(fieldIndex) > 0
  }

  def fieldIndex(tag: Byte): Int = {
    tagIndexMappingArray(tag & 0xFF)
  }

  def fieldIndex(fieldName: String): Int = {
    nameMapping(fieldName)
  }

  def getExpr(index: Int): Expression[_] = {
    valueExprIndex.find(_._2 == index).orElse(refExprIndex.find(_._2 == index)).get._1
  }

  def dimIdFieldIndex(tag: Byte): Int = {
    dimIdMappingArray(tag & 0xFF)
  }

  def needId(tag: Byte): Boolean = dimIdFieldIndex(tag) != -1

  private def getFixedFieldSizeArray(exprIndex: Map[Expression[_], Int]): Array[Int] = {
    val sizes = Array.fill[Int](numOfFields)(0)
    exprIndex.toSeq
      .sortBy(_._2)
      .foreach {
        case (expr, idx) =>
          sizes(idx) = getFieldSize(expr)
      }
    sizes
  }

  def withFieldName(i: Int, newFieldName: String) = {
    val newNameMapping = nameMapping.filterNot(_._2 == i) + (newFieldName -> i)
    new DatasetSchema(valueExprIndex, refExprIndex, newNameMapping, table)
  }

  private def getFieldsOffsets(fieldSizes: Array[Int]): Array[Int] = {
    fieldSizes.scanLeft(0) { (offset, size) =>
      offset + size
    }
  }

  private def getFixedAreaSize(exprIndex: Map[Expression[_], Int]): Int = {
    exprIndex.foldLeft(0) {
      case (s, (e, _)) =>
        s + getFieldSize(e)
    }
  }

  private def getFieldSize(expr: Expression[_]): Int = {
    expr.dataType.internalStorable.fixedSize match {
      case Some(fixSize)                                    => fixSize
      case None if expr.dataType.internalStorable.isRefType => 0
      case None                                             => BatchDataset.SIZE_OF_FIXED_PART_OF_VARIABLE_LENGHT_FIELDS
    }
  }

  private def createFixedFieldMappingArray(exprIndex: Map[Expression[_], Int]): Array[Boolean] = {

    val fixed = Array.fill[Boolean](numOfFields)(false)
    exprIndex.foreach {
      case (expr, index) =>
        fixed(index) = expr.dataType.internalStorable.fixedSize.isDefined
    }
    fixed
  }

  private def createTagByIndexMappingArray(exprIndex: Map[Expression[_], Int], table: Option[Table]): Array[Int] = {
    table match {
      case Some(t) =>
        val tagIndexes = Array.fill[Int](Table.MAX_TAGS)(-1)

        exprIndex.toSeq.foreach {
          case (expr, index) =>
            val tag = expr match {
              case MetricExpr(metric) =>
                Some(metric.tag)

              case DimensionExpr(dimension: Dimension) =>
                Some(t.dimensionTag(dimension))

              case _ =>
                None
            }
            tag.foreach { t =>
              tagIndexes(t & 0xFF) = index
            }
        }

        tagIndexes
      case None => Array.empty
    }
  }

  private def createDimIdMappingArray(exprIndex: Map[Expression[_], Int], table: Option[Table]): Array[Int] =
    table match {
      case Some(table) =>
        val tagIndex = Array.fill[Int](Table.MAX_TAGS)(-1)

        exprIndex.toSeq.foreach {
          case (DimensionIdExpr(dimension: Dimension), index) =>
            val t = table.dimensionTag(dimension)
            tagIndex(t & 0xFF) = index

          case _ =>
        }

        tagIndex

      case None =>
        Array.empty
    }

  private def createRefFieldsMappingArray(exprIndex: Map[Expression[_], Int]): Array[Int] = {
    val array = Array.fill[Int](numOfFields)(-1)
    exprIndex.zipWithIndex.foreach {
      case ((_, index), ordinal) =>
        array(index) = ordinal
    }
    array
  }

}

object DatasetSchema {
  def apply(table: Table) = {

    val dimExprs = table.dimensionSeq.map { dim =>
      dim.name -> DimensionExpr(dim.aux)
    }
    val metricExprs = table.metrics.map { metric =>
      metric.name -> MetricExpr(metric.aux)
    }

    val allExprs: Seq[(String, Expression[_])] = ("time" -> TimeExpr) +: (dimExprs ++ metricExprs)

    val (refExprIndex, valueExprIndex) =
      allExprs.map(_._2).zipWithIndex.toMap.partition(_._1.dataType.internalStorable.isRefType)

    val nameMapping = allExprs.map {
      case (name, expr) =>
        val index = refExprIndex.getOrElse(expr, valueExprIndex(expr))
        name -> index
    }.toMap

    new DatasetSchema(valueExprIndex, refExprIndex, nameMapping, Some(table))
  }
}

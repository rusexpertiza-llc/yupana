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

import com.typesafe.scalalogging.StrictLogging

import javax.sql.DataSource
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query.{ Expression, LinkExpr, Replace, TransformCondition }
import org.yupana.api.schema.{ Dimension, ExternalLink, Schema }
import org.yupana.api.types.{ BoxingTag, DataType }
import org.yupana.core.ExternalLinkService
import org.yupana.core.cache.{ Cache, CacheFactory }
import org.yupana.core.model.InternalRow
import org.yupana.core.utils.{ SparseTable, Table }
import org.yupana.externallinks.ExternalLinkUtils
import org.yupana.externallinks.universal.JsonCatalogs.SQLExternalLinkDescription
import org.yupana.schema.externallinks.ExternalLinks._

class SQLSourcedExternalLinkService[DimensionValue](
    override val schema: Schema,
    override val externalLink: ExternalLink.Aux[DimensionValue],
    config: SQLExternalLinkDescription,
    dataSource: DataSource
) extends ExternalLinkService[ExternalLink]
    with StrictLogging {

  import SQLSourcedExternalLinkService._
  import config._
  import org.yupana.api.query.syntax.All._

  private val mapping = config.fieldsMapping

  private val fieldValuesForDimValuesCache: Cache[DimensionValue, Map[FieldName, FieldValue]] =
    CacheFactory.initCache[DimensionValue, Map[FieldName, FieldValue]](externalLink.linkName + "_fields")(
      externalLink.dimension.dataType.boxingTag,
      BoxingTag[Map[FieldName, FieldValue]]
    )

  override def setLinkedValues(
      exprIndex: collection.Map[Expression[_], Int],
      rows: Seq[InternalRow],
      exprs: Set[LinkExpr[_]]
  ): Unit = {
    ExternalLinkUtils.setLinkedValues(
      externalLink.asInstanceOf[ExternalLink.Aux[externalLink.dimension.T]],
      exprIndex,
      rows,
      exprs,
      fieldValuesForDimValues
    )
  }

  override def transformCondition(condition: Condition): Seq[TransformCondition] = {
    ExternalLinkUtils.transformConditionT[String](
      expressionCalculator,
      externalLink.linkName,
      condition,
      includeTransform,
      excludeTransform
    )
  }

  private def includeTransform(values: Seq[(Condition, String, Set[String])]): TransformCondition = {
    val dimValues = dimValuesForFieldsValues(values, "AND").filter(x => x != null)
    if (externalLink.dimension.dataType == DataType[String]) {
      Replace(
        values.map(_._1).toSet,
        in(
          lower(dimension(externalLink.dimension.asInstanceOf[Dimension.Aux[String]])),
          dimValues.asInstanceOf[Set[String]]
        )
      )
    } else {
      Replace(
        values.map(_._1).toSet,
        in(dimension(externalLink.dimension.aux), dimValues)
      )
    }
  }

  private def excludeTransform(values: Seq[(Condition, String, Set[String])]): TransformCondition = {
    val dimValues = dimValuesForFieldsValues(values, "OR").filter(x => x != null)
    if (externalLink.dimension.dataType == DataType[String]) {
      Replace(
        values.map(_._1).toSet,
        notIn(
          lower(dimension(externalLink.dimension.asInstanceOf[Dimension.Aux[String]])),
          dimValues.asInstanceOf[Set[String]]
        )
      )
    } else {
      Replace(
        values.map(_._1).toSet,
        notIn(dimension(externalLink.dimension.aux), dimValues)
      )
    }
  }

  private def catalogFieldToSqlField(cf: FieldName): String = mapping.flatMap(_.get(cf)).getOrElse(camelToSnake(cf))

  private def catalogFieldToSqlFieldWithAlias(cf: FieldName): String =
    s"${catalogFieldToSqlField(cf)} AS ${SQLSourcedExternalLinkService.camelToSnake(cf)}"

  def projection(fields: Set[FieldName]): String = {
    (fields + dimensionName).map(catalogFieldToSqlFieldWithAlias).mkString(", ")
  }

  def relation: String = config.relation.getOrElse(camelToSnake(linkName))

  def fieldValuesForDimValues(
      fields: Set[FieldName],
      tagValues: Set[DimensionValue]
  ): Table[DimensionValue, FieldName, FieldValue] = {

    val tableRows = fieldValuesForDimValuesCache.getAll(tagValues)

    if (tagValues.forall(tableRows.contains)) {
      SparseTable(tableRows)
    } else {
      val missedKeys = tagValues diff tableRows.keys.toSet
      val q = fieldsByTagsQuery(externalLink.fields.map(_.name), missedKeys.size)
      val params = missedKeys.map(_.asInstanceOf[Object]).toSeq
      logger.debug(s"Query for fields for catalog $linkName: $q with params: $params")
      val sqlFields = (fields + dimensionName).map(camelToSnake)
      val dataFromDb = JdbcUtils
        .runQuery(dataSource, q, sqlFields, params)
        .map(vs => vs.map { case (k, v) => snakeToCamel(k) -> v })
        .groupBy(m => m(dimensionName))
        .map { case (k, v) => k.asInstanceOf[DimensionValue] -> (v.head - dimensionName).mapValues(_.toString) }
      fieldValuesForDimValuesCache.putAll(dataFromDb)
      SparseTable(tableRows ++ dataFromDb)
    }
  }

  private def fieldsByTagsQuery(fields: Set[FieldName], tagValuesCount: Int): String = {
    s"""SELECT ${projection(fields)}
       |FROM $relation
       |WHERE ${catalogFieldToSqlField(dimensionName)} ${tagValueInClause(tagValuesCount)}""".stripMargin
  }

  private def tagsByFieldsQuery(
      fieldValues: Seq[(Condition, FieldName, Set[FieldValue])],
      joiningOperator: String
  ): String = {
    s"""SELECT ${catalogFieldToSqlFieldWithAlias(dimensionName)}
       |FROM $relation
       |WHERE ${fieldValuesInClauses(fieldValues).mkString(s" $joiningOperator ")}""".stripMargin
  }

  private def tagValueInClause(tagValuesCount: Int): String = {
    s"""IN (${Seq.fill(tagValuesCount)("?").mkString(", ")})"""
  }

  private def fieldValuesInClauses(fieldValues: Seq[(Condition, FieldName, Set[FieldValue])]): Seq[String] = {
    fieldValues map {
      case (_, fieldName, possibleValues) =>
        s"""lower(${catalogFieldToSqlField(fieldName)}) IN (${Seq.fill(possibleValues.size)("?").mkString(", ")})"""
    }
  }

  private def dimValuesForFieldsValues(
      fieldsValues: Seq[(Condition, FieldName, Set[FieldValue])],
      joiningOperator: String
  ): Set[DimensionValue] = {
    val q = tagsByFieldsQuery(fieldsValues, joiningOperator)
    val params = fieldsValues.flatMap(_._3).map(_.asInstanceOf[Object])
    logger.debug(s"Query for dimensions for catalog $linkName: $q with params: $params")
    JdbcUtils
      .runQuery(dataSource, q, Set(camelToSnake(dimensionName)), params)
      .flatMap(_.values.map(_.asInstanceOf[DimensionValue]))
      .toSet
  }
}

object SQLSourcedExternalLinkService {

  private val snakeParts = "_([a-z\\d])".r

  def camelToSnake(s: String): String = {
    s.drop(1).foldLeft(s.headOption.map(_.toLower.toString) getOrElse "") {
      case (acc, c) if c.isUpper => acc + "_" + c.toLower
      case (acc, c)              => acc + c
    }
  }

  def snakeToCamel(s: String): String = {
    snakeParts.replaceAllIn(s, _.group(1).toUpperCase())
  }
}
